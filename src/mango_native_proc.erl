-module(mango_native_proc).
-behavior(gen_server).


-export([
    start_link/0,
    set_timeout/2,
    prompt/2
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-record(st, {
    indexes = [],
    timeout = 5000
}).


start_link() ->
    gen_server:start_link(?MODULE, [], []).


set_timeout(Pid, TimeOut) when is_integer(TimeOut), TimeOut > 0 ->
    gen_server:call(Pid, {set_timeout, TimeOut}).


prompt(Pid, Data) ->
    gen_server:call(Pid, {prompt, Data}).


init(_) ->
    {ok, #st{}}.


terminate(_Reason, _St) ->
    ok.


handle_call({set_timeout, TimeOut}, _From, St) ->
    {reply, ok, St#st{timeout=TimeOut}};

handle_call({prompt, [<<"reset">>]}, _From, St) ->
    {reply, true, St#st{indexes=[]}};

handle_call({prompt, [<<"reset">>, _QueryConfig]}, _From, St) ->
    {reply, true, St#st{indexes=[]}};

handle_call({prompt, [<<"add_fun">>, IndexInfo]}, _From, St) ->
    Indexes = St#st.indexes ++ [IndexInfo],
    NewSt = St#st{indexes = Indexes},
    {reply, true, NewSt};

handle_call({prompt, [<<"map_doc">>, Doc]}, _From, St) ->
    {reply, map_doc(St, mango_json:to_binary(Doc)), St};

handle_call({prompt, [<<"reduce">>, _, _]}, _From, St) ->
    {reply, null, St};

handle_call({prompt, [<<"rereduce">>, _, _]}, _From, St) ->
    {reply, null, St};

handle_call({prompt, [<<"index_doc">>, Doc]}, _From, St) ->
    {reply, index_doc(St, mango_json:to_binary(Doc)), St};

handle_call(Msg, _From, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.


handle_cast(garbage_collect, St) ->
    erlang:garbage_collect(),
    {noreply, St};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.


handle_info(Msg, St) ->
    {stop, {invalid_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


map_doc(#st{indexes=Indexes}, Doc) ->
    lists:map(fun(Idx) -> get_index_entries(Idx, Doc) end, Indexes).


index_doc(#st{indexes=Indexes}, Doc) ->
    lists:map(fun(Idx) -> get_text_entries(Idx, Doc) end, Indexes).


get_index_entries({IdxProps}, Doc) ->
    {Fields} = couch_util:get_value(<<"fields">>, IdxProps),
    Values = lists:map(fun({Field, _Dir}) ->
        case mango_doc:get_field(Doc, Field) of
            not_found -> not_found;
            bad_path -> not_found;
            Else -> Else
        end
    end, Fields),
    case lists:member(not_found, Values) of
        true ->
            [];
        false ->
            [[Values, null]]
    end.


%% Retrieves values for each field and returns it to dreyfus for indexing
get_text_entries({IdxProps}, Doc0) ->
    Selector = case couch_util:get_value(<<"selector">>, IdxProps) of
        [] -> {[]};
        Else -> Else
    end,
    Doc = filter_doc(Selector, Doc0),
    Fields = couch_util:get_value(<<"fields">>, IdxProps),
    DefaultField = couch_util:get_value(<<"default_field">>, IdxProps),
    Results0 = case Fields of
        <<"all_fields">> ->
           get_all_textfield_values(Doc, []);
        _ ->
            lists:foldl(fun({Field}, Acc) ->
                FieldName= couch_util:get_value(<<"field">>, Field),
                FieldType= couch_util:get_value(<<"type">>, Field, <<"string">>),
                get_textfield_values(Doc, {FieldName, FieldType}, Acc)
            end, [], Fields)
    end,
    Results = case DefaultField of
        {[{<<"enabled">>, false}, _]} ->
            Results0;
        {[{<<"enabled">>, false}]} ->
            Results0;
        _ ->
            get_default_values(Doc,Results0)
        end,
    case lists:member(not_found, Results) of
        true ->
            [];
        false ->
            Results
    end.

%% This is used by the selector for the lucene field name
%% to filter out documents prior to the text search
%% Also, do not index design documents
filter_doc(Selector0, Doc) ->
    Selector = mango_selector:normalize(Selector0),
    Match = mango_selector:match(Selector, Doc),
    DDocId = mango_doc:get_field(Doc, <<"_id">>),
    case {Match, DDocId} of
        {_, <<"_design/", _/binary>>} -> [];
        {false, _} -> [];
        {true, _} -> Doc
    end.


get_textfield_values(Doc, {FieldName, FieldType}, Acc) ->
    Path = re:split(FieldName, <<"\\.">>),
    get_textfield_values(Doc, {FieldName, FieldType}, Path, Acc).

get_textfield_values(Values, {FieldName, FieldType}, [SubName | Rest], Acc) when is_list(Values)->
    case {SubName, Rest} of
        {<<"[]">>, []} ->
            lists:foldl(fun (Value, SubAcc) ->
                case match_text_type(FieldType, Value) of
                    true ->
                        [[<<FieldName/binary, ":", FieldType/binary>>, Value, []] | SubAcc];
                    false ->
                        SubAcc
                 end
            end, Acc, Values);
        {<<"[]">>, _} ->
            lists:foldl(fun (Value, SubAcc) ->
                get_textfield_values(Value, {FieldName, FieldType}, Rest, SubAcc)    
            end, Acc, Values);
        {_, _} ->
            Acc
    end;
get_textfield_values({Values}, {FieldName, FieldType}, [SubName | Rest], Acc) ->
    Values0 = mango_doc:get_field({Values}, SubName),
    case {SubName, Rest, Values0} of
        {<<"[]">>, _, _} ->
            Acc;
        {_, _, not_found} ->
            Acc;
        {_, _, bad_path} ->
            Acc;
        {_, [], Value} ->
            case match_text_type(FieldType, Value) of
                true ->
                    [[<<FieldName/binary, ":", FieldType/binary>>, Value, []] | Acc];
                false ->
                    Acc
             end;
        {_, _, SubDoc} ->
            get_textfield_values(SubDoc, {FieldName, FieldType}, Rest, Acc)
    end;
get_textfield_values(_, _ , _ , Acc) ->
    Acc.


get_all_textfield_values([], Acc) ->
    Acc;
get_all_textfield_values({Doc}, Acc) when is_list(Doc) ->
    lists:foldl(fun(SubDoc, SubAcc) ->
        get_all_textfield_values(SubDoc, SubAcc)
    end, Acc, Doc);
get_all_textfield_values({<<Field/binary>>, <<Value/binary>>}, Acc) ->
    [[<<Field/binary, ":string">>, Value, []] | Acc];
get_all_textfield_values({<<Field/binary>>, Value}, Acc) when is_number(Value) ->
    [[<<Field/binary, ":number">>, Value, []] | Acc];
get_all_textfield_values({<<Field/binary>>, Value}, Acc) when is_boolean(Value) ->
    [[<<Field/binary, ":boolean">>, Value, []] | Acc];
%% field : array
get_all_textfield_values({<<Field/binary>>, Values}, Acc) when is_list(Values) ->
    Acc0 = [[<<Field/binary,".[]:length">>, length(Values), []] | Acc],
    lists:foldl(fun(ListVal, SubAcc) ->
        get_all_textfield_values({<<Field/binary, ".[]">>, ListVal}, SubAcc)
    end, Acc0, Values);
%% field : object
get_all_textfield_values({<<Field/binary>>, {Values}}, Acc) when is_list(Values) ->
    lists:foldl(fun(ListVal, SubAcc) ->
        get_all_textfield_values({<<Field/binary>>, ListVal}, SubAcc)
    end, Acc, Values);
get_all_textfield_values({<<Field/binary>>, {<<SubField/binary>>, <<Value/binary>>}}, Acc) ->
    [[<<Field/binary, ".", SubField/binary, ":string">>, Value, []] | Acc];
get_all_textfield_values({<<Field/binary>>, {<<SubField/binary>>, Value}}, Acc) when is_number(Value) ->
    [[<<Field/binary, ".", SubField/binary, ":number">>, Value, []] | Acc];
get_all_textfield_values({<<Field/binary>>, {<<SubField/binary>>, Value}}, Acc) when is_boolean(Value) ->
    [[<<Field/binary, ".", SubField/binary, ":boolean">>, Value, []] | Acc];
get_all_textfield_values({<<Field/binary>>, {<<SubField/binary>>, Values}}, Acc) when is_list(Values) ->
    Acc0 = [[<<Field/binary,".[]:length">>, length(Values), []] | Acc],
    lists:foldl(fun(ListVal, SubAcc) ->
        get_all_textfield_values({<<Field/binary, ".[].", SubField/binary>>, ListVal}, SubAcc)
    end, Acc0, Values);
get_all_textfield_values({<<Field/binary>>, {<<SubField/binary>>, {Values}}}, Acc) when is_list(Values) ->
    lists:foldl(fun(ListVal, SubAcc) ->
        get_all_textfield_values({<<Field/binary, ".", SubField/binary>>, ListVal}, SubAcc)
    end, Acc, Values).


get_default_values([], Acc) ->
    Acc;
get_default_values({Doc}, Acc) when is_list(Doc) ->
    lists:foldl(fun(SubDoc, SubAcc) ->
        get_default_values(SubDoc, SubAcc)
    end, Acc, Doc);
get_default_values({_, <<Value/binary>>}, Acc) ->
    [[<<"default">>, Value, []] | Acc];
%% field : array
get_default_values({<<Field/binary>>, Values}, Acc) when is_list(Values) ->
    lists:foldl(fun(ListVal, SubAcc) ->
        get_default_values({<<Field/binary>>, ListVal}, SubAcc)
    end, Acc, Values);
%% field : object
get_default_values({<<_/binary>>, {Values}}, Acc) when is_list(Values) ->
    lists:foldl(fun(ListVal, SubAcc) ->
        get_default_values(ListVal, SubAcc)
    end, Acc, Values);
get_default_values(_, Acc) ->
    Acc.


match_text_type(<<"string">>, Value) when is_binary(Value) ->
    true;
match_text_type(<<"number">>, Value) when is_number(Value) ->
    true;
match_text_type(<<"boolean">>, Value) when is_boolean(Value) ->
    true;
match_text_type(_, _)  ->
    false.

