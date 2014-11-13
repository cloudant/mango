-module(mango_text_selector).


-export([
    normalize/1
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").


%% Normalize Text Selector
%% All text selectors will be normalized to:
%% {
%%    "$text": query-string
%%    "$options":
%%      {
%%       "$bookmark":  val1,
%%       "$counts":    val2,
%%       "$ranges": val3
%%      }
%% }
normalize({[]}) ->
    {[]};
normalize(Selector) ->
    Steps = [
        fun norm_ops/1
    ],
    {NProps} = lists:foldl(fun(Step, Sel) -> Step(Sel) end, Selector, Steps),
    % FieldNames = [Name || {Name, _} <- Props],
    % NProps = case lists:member(<<>>, FieldNames) of
    %     true ->
    %         %%use default as our field when no field has been specified
    %         [<<"default">>,{Props}];
    %     false ->
    %         Props
    % end,
    %twig:log(notice, "NProps~p",[NProps]),
    {NProps}.

%%text seach operators

norm_ops({[{<<"$text">>, Arg}]}) when is_binary(Arg); is_number(Arg); is_boolean(Arg) ->
    {[{<<"$text">>, Arg}]};
norm_ops({[{<<"$text">>, Arg}]}) ->
    ?MANGO_ERROR({bad_arg, '$text', Arg});

%%Options with $text
norm_ops({[{<<"$text">>, Arg}, Opts]}) when is_binary(Arg); is_number(Arg); is_boolean(Arg) ->
     {[{<<"$text">>, Arg},norm_ops(Opts)]};
norm_ops({[{<<"$text">>, Arg}, _]}) ->
      ?MANGO_ERROR({bad_arg, '$text', Arg});


norm_ops({<<"$options">>, {Args}}) ->
    Opts = lists:map(fun(Arg) -> norm_ops(Arg) end, Args),
    {<<"$options">>, {Opts}};

norm_ops({<<"$bookmark">>, Arg}) when is_binary(Arg) ->
    {<<"$bookmark">>, Arg};
norm_ops({[{<<"$bookmark">>, Arg}]}) ->
    ?MANGO_ERROR({bad_arg, '$bookmark', Arg});
norm_ops({<<"$counts">>, Arg}) when is_list(Arg) ->
    {<<"$counts">>, Arg};
norm_ops({[{<<"$counts">>, Arg}]}) ->
    ?MANGO_ERROR({bad_arg, '$counts', Arg});
norm_ops({<<"$ranges">>, {_}=Arg}) ->
    {<<"$ranges">>, Arg};
norm_ops({<<"$ranges">>, _}) ->
    ?MANGO_ERROR({bad_arg, '$ranges'});


% Known but unsupported text operators
norm_ops({<<"$group">>, _}) ->
    ?MANGO_ERROR({not_supported, '$group'});
% Unknown operator
norm_ops({<<"$", _/binary>>=Op, _}) ->
    ?MANGO_ERROR({invalid_operator, Op});
norm_ops({[{<<"$", _/binary>>=Op, _}]}) ->
    ?MANGO_ERROR({invalid_operator, Op});


norm_ops(Arg) ->
    ?MANGO_ERROR({invalid_selector, Arg}).