-module(darius).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([
         connection_start/1,
         channel_open/1,
         declare_exchange/2,
         declare_queue/2,
         bind_queue/4,
         queue_subscribe/2,
         exchange_publish/4,
         message_ack/2,
         message_nack/2,
         message_reject/2
        ]).

%% Make it a config from sys.config...
-define(BACKOFF_MAX_WAIT_TIME, 512).
-define(BACKOFF_MULTIPLIER, 2).
-define(BACKOFF_JITTER, 0.1).

%%-----------------------------------------------------------------------------
%% Interface
%%-----------------------------------------------------------------------------
%% @doc Starts a connection to an AMQP server.
-spec connection_start(Params :: #amqp_params_network{}) ->
    {ok, pid()} | {error, term()}.
connection_start(#amqp_params_network{host = Host,
                                      port = Port,
                                      virtual_host = VHost} = Params) ->
    lager:info("Connecting to RabbitMQ at ~s:~p~s", [Host, Port, VHost]),
    case connection_start(with_backoff, Params) of
        {ok, Pid} ->
            {ok, Pid};
        {error, econnrefused} ->
            {error, econnrefused};
        {error, _} = Error ->
            Error
    end.

connection_start(with_backoff, {Params, WaitTime}) ->
    lager:info("Connecting to RabbitMQ with backoff wait time ~s secs",
               [io_lib:format("~.4f",[WaitTime / 1000])]),
    timer:sleep(WaitTime),
    case amqp_connection:start(Params) of
        {ok, Pid} ->
            {ok, Pid};
        {error, econnrefused} ->
            if
                WaitTime < ?BACKOFF_MAX_WAIT_TIME * 1000 ->
                    connection_start(with_backoff, {Params, increase_wait_time(WaitTime)});
                WaitTime >= ?BACKOFF_MAX_WAIT_TIME * 1000 ->
                    lager:error("RabbitMQ backoff connection time limit reached, giving up."),
                    {error, econnrefused}
            end;
        {error, _} = Error ->
            Error
    end;

connection_start(with_backoff, Params) ->
    connection_start(with_backoff, {Params, 0}).

-spec increase_wait_time(Time :: non_neg_integer()) -> integer().
increase_wait_time(Time) ->
    case Time of
        0 ->
            offset_time(2000);
        _ ->
            offset_time(Time * ?BACKOFF_MULTIPLIER)
    end.

-spec offset_time(Time :: pos_integer()) -> integer().
%% @doc Compute time offset.
offset_time(Time) ->
    <<A:32, B:32, C:32>> = crypto:rand_bytes(12),
    random:seed({A,B,C}),
    Offset = Time * (random:uniform() * ?BACKOFF_JITTER),
    Rand = random:uniform(),
    if
        Rand > 0.5 ->
            round(Time + Offset);
        Rand =< 0.5 ->
            round(Time - Offset)
    end.

%% @doc Open a channel.
-spec channel_open(Connection :: pid()) -> {ok, pid()}.
channel_open(Connection) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Channel}.

%% @doc Declare an exchange.
-spec declare_exchange(Channel :: pid(),
                       Exchange :: binary() | #'exchange.declare'{}) -> ok.
declare_exchange(Channel, Exchange) when is_binary(Exchange) ->
    Declare = #'exchange.declare'{exchange = Exchange,
                                  type = <<"direct">>,
                                  durable = true},
    declare_exchange(Channel, Declare);
declare_exchange(Channel, #'exchange.declare'{exchange = Exchange,
                                              type = Type,
                                              durable = Durable} = Declare) ->
    lager:info("Declaring Exchange '~s' (type: ~s, durable: ~s)",
               [Exchange, Type, Durable]),
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Declare),
    ok.

%% @doc Declare a queue.
-spec declare_queue(Channel :: pid(),
                    Queue :: binary() | #'queue.declare'{}) -> ok.
declare_queue(Channel, Queue) when is_binary(Queue) ->
    Declare = #'queue.declare'{queue = Queue,
                               durable = true},
    declare_queue(Channel, Declare);
declare_queue(Channel, #'queue.declare'{queue = Queue,
                                        durable = Durable} = Declare) ->
    lager:info("Declaring Queue '~s' (durable: ~s)", [Queue, Durable]),
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Declare),
    ok.

%% @doc Bind a queue to an exchange with a routing key.
-spec bind_queue(Channel :: pid(),
                 Queue :: binary(),
                 Exchange :: binary(),
                 RoutingKey :: binary()) -> ok.
bind_queue(Channel, Queue, Exchange, RoutingKey) ->
    Binding = #'queue.bind'{queue = Queue,
                            exchange = Exchange,
                            routing_key = RoutingKey},
    lager:info("Binding Queue '~s' to exchange '~s' (routing key: '~s')",
               [Queue, Exchange, RoutingKey]),
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
    ok.

%% @doc Subscribe to a queue.
-spec queue_subscribe(Channel :: pid(),
                      Queue :: binary()) -> {ok, binary()}.
queue_subscribe(Channel, Queue) ->
    Sub = #'basic.consume'{queue = Queue,
                           no_ack = false},
    #'basic.consume_ok'{consumer_tag=Subscription} = amqp_channel:call(Channel,
                                                                       Sub),
    {ok, Subscription}.

%% @doc Publish to a queue.
-spec exchange_publish(Payload :: binary(),
                       Channel :: pid(),
                       Exchange :: binary(),
                       RoutingKey :: binary()) -> ok.
exchange_publish(Payload, Channel, Exchange, RoutingKey) ->
    Pub = #'basic.publish'{exchange = Exchange,
                           routing_key = RoutingKey},

    Params = #'P_basic'{delivery_mode = 2},
    Msg = #amqp_msg{props = Params, payload = Payload},
    ok = amqp_channel:call(Channel, Pub, Msg),
    ok.

%% @doc ACK a message.
-spec message_ack(Channel :: pid(),
                  Tag :: binary()) -> ok.
message_ack(Channel, Tag) ->
    ok = amqp_channel:call(Channel, #'basic.ack'{delivery_tag = Tag}),
    ok.

%% @doc NACK a message.
-spec message_nack(Channel :: pid(),
                   Tag :: binary()) -> ok.
message_nack(Channel, Tag) ->
    ok = amqp_channel:call(Channel, #'basic.nack'{delivery_tag = Tag,
                                                  multiple = false,
                                                  requeue = true}),
    ok.

%% @doc Reject a message.
-spec message_reject(Channel :: pid(),
                     Tag :: binary()) -> ok.
message_reject(Channel, Tag) ->
    ok = amqp_channel:call(Channel, #'basic.reject'{delivery_tag = Tag,
                                                    requeue = true}),
    ok.



