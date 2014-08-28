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


get_text_entries({IdxProps}, Doc0) ->
    Selector = case couch_util:get_value(<<"selector">>,IdxProps) of
        [] -> {[]};
        Else -> Else
    end,
    Doc = filter_doc(Selector,Doc0),
    Fields = couch_util:get_value(<<"fields">>, IdxProps),
    Results = lists:map(fun(Field) -> 
    FieldName = get_textfield_name(Field),
    Values = get_textfield_values(Doc,Field),
    Store = get_textfield_opts(Field,"<<store>>"),
    Index = get_textfield_opts(Field,"<<index>>"),
    Facet = get_textfield_opts(Field,"<<facet>>"),
        case Values of 
            not_found -> not_found;
            [] -> not_found;
            _ -> [FieldName, list_to_binary(io_lib:format("~p", [Values])), [{<<"store">>,Store}, {<<"index">>,Index},{"<<facet>>",Facet}]]
        end
    end, Fields),
    case lists:member(not_found, Results) of
        true ->
            [];
        false ->
            Results
    end.

filter_doc(Selector0,Doc) ->
    Selector = mango_selector:normalize(Selector0),
    case mango_selector:match(Selector,Doc) of
        true -> Doc;
        false -> []
    end.



get_textfield_name({[{FieldName,_}]}) ->
    FieldName;
get_textfield_name(Field) ->
    Field.


get_textfield_values(Doc,{[{FieldName,{FieldOpts}}]}) ->
    DocFields = case couch_util:get_value(<<"doc_fields">>, FieldOpts) of
        [] -> [FieldName];
        undefined -> [FieldName];
        Else -> Else 
    end,
    Values = lists:map(fun(SubField) ->
        get_textfield_values(Doc,SubField)
    end,DocFields),
    case lists:member(not_found,Values) of 
            true-> [];
            false -> Values
        end;
get_textfield_values(Doc,Field) ->
    case mango_doc:get_field(Doc, Field) of
        not_found -> not_found;
        bad_path -> not_found;
        Else -> Else  
    end.


get_textfield_opts({[{_,{FieldOpts}}]},Option) ->
     case Option of
        <<"store">> -> couch_util:get_value(<<"store">>,FieldOpts,<<"false">>);
        <<"index">> -> couch_util:get_value(<<"index">>,FieldOpts,<<"true">>);
        <<"facet">> -> couch_util:get_value(<<"facet">>,FieldOpts,<<"false">>);
        _->undefined_textfield_option
    end;
get_textfield_opts(_,Option) ->
    %return defaults when no options are provided
    case Option of
        <<"store">> -> <<"false">>;
        <<"index">> -> <<"true">>;
        <<"face">> -> <<"false">>;
        _->undefined_textfield_option
    end.


    

 

