-module(mango_text_selector).


-export([
    parse_selector/1
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").

%% Builds the lucene query from the selector.
parse_selector(Value) when is_binary(Value) ->
    BinVal = get_value(Value),
    <<"\\:string:", BinVal/binary>>;
parse_selector(Value) when is_number(Value) ->
    BinVal = get_value(Value),
    <<"\\:number:", BinVal/binary>>;
parse_selector(Value) when is_boolean(Value) ->
    BinVal = get_value(Value),
    <<"\\:boolean:", BinVal/binary>>;
parse_selector(Values) when is_list(Values) ->
   lists:foldl(fun (Arg, Acc) ->
       case Arg of
           % array contains nested object
           {[{Key, Val}]} ->
                BinVal = parse_selector(Val),
                [<<".", Key/binary, BinVal/binary>> | Acc];
            % array contains a sub array
            SubArray when is_list(SubArray) ->
                BinVals = parse_selector(SubArray),
                lists:foldl(fun (SubArg, SubAcc) ->
                    [<<".\\[\\]", SubArg/binary>> | SubAcc]
                end, Acc, BinVals);
            SingleVal ->
                [parse_selector(SingleVal) | Acc]
        end
    end, [], Values);
parse_selector({[{<<"$lt">>, Value}]}) ->
    BinVal = get_value(Value),
    Type = mango_json:type(Value),
    <<"\\:", Type/binary, ":[-Infinity TO ", BinVal/binary, "}">>;
parse_selector({[{<<"$lte">>, Value}]}) ->
    BinVal = get_value(Value),
    Type = mango_json:type(Value),
    <<"\\:", Type/binary, ":[-Infinity TO ", BinVal/binary, "]">>;
parse_selector({[{<<"$gt">>, Value}]}) ->
    BinVal = get_value(Value),
    Type = mango_json:type(Value),
    <<"\\:", Type/binary, ":{", BinVal/binary, " TO Infinity]">>;
parse_selector({[{<<"$gte">>, Value}]}) ->
    BinVal = get_value(Value),
    Type = mango_json:type(Value),
    <<"\\:", Type/binary, ":[",  BinVal/binary, " TO Infinity]">>;
parse_selector({[{<<"$eq">>, Values}]}) when is_list(Values) ->
    Acc = parse_selector(Values),
    Len = list_to_binary(integer_to_list(length(Values))),
    [<<"\\:length:", Len/binary>> | Acc];
parse_selector({[{<<"$eq">>, Value}]}) ->
    parse_selector(Value);
parse_selector({[{<<"$ne">>, Value}]}) ->
    {negation, parse_selector(Value)};
parse_selector({[{<<"$not">>, Value}]}) ->
    {negation, parse_selector(Value)};
parse_selector({[{<<"$text">>, Value}]}) when is_binary(Value) ->
    parse_selector(Value);
% %% Escape the forward slashes for a regular expression
parse_selector({[{<<"$regex">>, Regex}]}) ->
    parse_selector(binary:replace(Regex, <<"/">>, <<"\/">>));
parse_selector({[{<<"$and">>, Args}]}) when is_list(Args) ->
    Values = lists:map(fun(Arg) ->
        binary_to_list(parse_selector(Arg)) end, Args),
    Values0 = list_to_binary(string:join([E || E <- Values, E /= []], " AND ")),
    <<"(", Values0/binary, ")">>;
parse_selector({[{<<"$or">>, Args}]}) when is_list(Args) ->
    Values = lists:map(fun(Arg) ->
        binary_to_list(parse_selector(Arg)) end, Args),
    Values0 = list_to_binary(string:join([E || E <- Values, E /= []], " OR ")),
    <<"(", Values0/binary, ")">>;
parse_selector({[{Field, {[{<<"$all">>, Args}]}}]}) when is_list(Args) ->
    Field0 = escape_lucene_chars(Field),
    Values = lists:foldl(fun (Arg, Acc) ->
       case Arg of
           % array contains nested object
           {[{Key, Val}]} ->
                BinVal = parse_selector(Val),
                Separator = get_separator(mango_json:type(Val)),
                [binary_to_list(<<Field0/binary, ".", Key/binary,
                    Separator/binary, BinVal/binary>>) | Acc];
            SingleVal ->
                BinVal = parse_selector(SingleVal),
                [binary_to_list(<<Field0/binary, BinVal/binary>>) | Acc]
        end
    end, [], Args),
    Len = list_to_binary(integer_to_list(length(Args))),
    Values0 =[binary_to_list(<<Field0/binary, "\\:length:",
        Len/binary>>) | Values],
    Values1 = list_to_binary(string:join([E || E <- Values0, E /= []],
        " AND ")),
    <<"(", Values1/binary, ")">>;
%% The same as $all, but uses OR and does not have length attached.
parse_selector({[{Field, {[{<<"$in">>, Args}]}}]}) when is_list(Args) ->
    Field0 = escape_lucene_chars(Field),
    Values = lists:foldl(fun (Arg, Acc) ->
       case Arg of
           % array contains nested object
           {[{Key, Val}]} ->
                BinVal = parse_selector(Val),
                Separator = get_separator(mango_json:type(Val)),
                [binary_to_list(<<Field0/binary, ".", Key/binary,
                    Separator/binary, BinVal/binary>>) | Acc];
            SingleVal ->
                BinVal = parse_selector(SingleVal),
                [binary_to_list(<<Field0/binary, BinVal/binary>>) | Acc]
        end
    end, [], Args),
    Values0 = list_to_binary(string:join([E || E <- Values, E /= []], " OR ")),
    <<"(", Values0/binary, ")">>;
parse_selector({[{Field, {[{<<"$nin">>, Args}]}}]}) when is_list(Args) ->
    Results = parse_selector({[{Field, {[{<<"$in">>, Args}]}}]}),
    <<"(NOT ", Results/binary, ")">>;
parse_selector({[{Field, {[{<<"$elemMatch">>, {[{<<"$and">>,
        Queries}]}}]}}]}) ->
    Values = lists:map(fun({[{SubField, Cond}]}) ->
        SubField0 = case SubField of
            <<>> -> Field;
            Else -> <<Field/binary, ".", Else/binary>>
        end,
        binary_to_list(parse_selector({[{SubField0, Cond}]})) end, Queries),
    Values0 = list_to_binary(string:join([E || E <- Values, E /= []], " AND ")),
    <<"(", Values0/binary, ")">>;
parse_selector({[{Field, {[{<<"$elemMatch">>, Query}]}}]}) ->
    {[{SubField, Cond}]} = Query,
    SubField0 = case SubField of
        <<>> -> Field;
        Else -> <<Field/binary, ".", Else/binary>>
    end,
    Value = parse_selector({[{SubField0, Cond}]}),
    <<"(", Value/binary, ")">>;
parse_selector({[{Field, {[{<<"$size">>, Arg}]}}]}) ->
    Field0 = escape_lucene_chars(Field),
    Length = get_value(Arg),
    <<Field0/binary, "\\:length:", Length/binary>>;
parse_selector({[{Field, {[{<<"$exists">>, true}]}}]}) ->
    Field0 = escape_lucene_chars(Field),
    String = <<Field0/binary, "\\:string:\/.*\/">>,
    Number = <<Field0/binary, "\\:number:[-Infinity TO Infinity]">>,
    BoolTrue = <<Field0/binary, "\\:boolean: true">>,
    BoolFalse = <<Field0/binary, "\\:boolean: false">>,
    <<"(", String/binary, " OR ", Number/binary, " OR ", BoolTrue/binary,
        " OR ", BoolFalse/binary, ")">>;
%% Placeholder, not sure if $exists:false can be translated to a lucene query
parse_selector({[{Field, {[{<<"$exists">>, false}]}}]}) ->
    parse_selector({[{Field, {[{<<"$exists">>, true}]}}]});
parse_selector({[{<<"default">>, Cond}]}) ->
    {[{<<"$text">>, Val}]} = Cond,
    Val0 = get_value(Val),
    case parse_selector(Cond) of
        {negation, _} ->
            <<"NOT default:", Val0/binary>>;
        _ ->
            <<"default:", Val0/binary>>
    end;
%% Object
parse_selector({[{Field, Cond}]}) ->
    Field0 = escape_lucene_chars(Field),
    Separator = get_separator(Cond),
    case parse_selector(Cond) of
        {negation, Values} when is_list(Values) ->
            Values0 = lists:map(fun(Arg) ->
                binary_to_list(<<Field0/binary, Arg/binary>>)
            end, Values),
            RetVal = list_to_binary(string:join([E || E <- Values0, E /= []],
                " AND ")),
            <<"NOT (", RetVal/binary, ")">>;
        Values when is_list(Values) ->
            Values0 = lists:map(fun(Arg) ->
                binary_to_list(<<Field0/binary, Arg/binary>>)
            end, Values),
            RetVal = list_to_binary(string:join([E || E <- Values0, E /= []],
                " AND ")),
            <<"(", RetVal/binary, ")">>;
        {negation, Value} ->
            <<"NOT (", Field0/binary, Value/binary, ")">>;
        Val ->
            <<Field0/binary, Separator/binary, Val/binary>>
    end.


%% Only returns the value and not with the prepended \\:type
get_value(Value) when is_binary(Value) ->
    Value;
get_value(Value) when is_integer(Value) ->
    list_to_binary(integer_to_list(Value));
get_value(Value) when is_float(Value) ->
    list_to_binary(float_to_list(Value));
get_value(Value) when is_boolean(Value) ->
    case Value of
        true -> <<"true">>;
        false -> <<"false">>
    end.

%% This method is for recursively appending the:
%% . separator for the String Builder
get_separator(Cond) ->
   case Cond of
        {[{<<"$gt">>, _}]} -> <<>>;
        {[{<<"$gte">>, _}]} -> <<>>;
        {[{<<"$lt">>, _}]} -> <<>>;
        {[{<<"$lte">>, _}]} -> <<>>;
        {[{<<"$eq">>, _}]} -> <<>>;
        {[{<<"$text">>, _}]} -> <<>>;
        <<"object">> -> <<".">>;
        {_} -> <<".">>;
        _ -> <<>>
    end.


%% + - && || ! ( ) { } [ ] ^ " ~ * ? : \
escape_lucene_chars(Field) when is_binary(Field) ->
    LuceneChars = [<<"+">>, <<"-">>, <<"&&">>, <<"||">>,
    <<"!">>, <<"(">>, <<")">>, <<"{">>, <<"}">>, <<"\"">>,
    <<"[">>, <<"]">>, <<"^">>, <<"\~">>, <<"*">>, <<"?">>,
    <<":">>],
    lists:foldl(fun(Char, Acc) ->
        binary:replace(Acc, Char, <<"\\", Char/binary>>)
    end, Field, LuceneChars).
