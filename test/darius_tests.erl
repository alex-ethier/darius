-module(darius_tests).

-include_lib("eunit/include/eunit.hrl").

darius_test_() ->
    {setup,
     fun() ->
             ok
     end,
     fun(_) ->
             ok
     end,
     [
      {"darius is alive",
       fun() ->
               %% format is always: expected, actual
               ?assertEqual(howdy, darius:hello())
       end}
      ]}.

