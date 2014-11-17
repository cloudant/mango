-module(mango_cursor_text).

-export([
    create/3,
    execute/3
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("dreyfus/include/dreyfus.hrl").
-include("mango_cursor.hrl").
-include("mango.hrl").


create(Db, Selector0, Opts) ->
    Selector = mango_text_selector:normalize(Selector0),
    Fields = case couch_util:get_value(fields, Opts, all_fields) of
        all_fields -> [];
        Else -> Else
    end,
    IndexFields = [<<"default">> | Fields],
    ExistingIndexes = mango_idx:filter_list(mango_idx:list(Db),[<<"text">>]),
    UsableIndexes = find_usable_indexes(IndexFields,ExistingIndexes),
    SortIndexes = mango_cursor:get_sort_indexes(ExistingIndexes, UsableIndexes, Opts),
    Index = choose_best_index(SortIndexes,IndexFields),
    Limit = couch_util:get_value(limit, Opts, 50),
    %% The default passed in from Opts is 10000000000, which dreyfus will not accept
    %% For now, we cap it at 50
    CapLimit = case Limit > 50 of 
        true -> 50;  %% Should we throw an error instead?
        false -> Limit
    end,
    Skip = couch_util:get_value(skip, Opts, 0),

    {ok, #cursor{
        db = Db,
        index = Index,
        ranges = null,
        selector = Selector,
        opts = Opts,
        limit = CapLimit,
        skip = Skip,
        fields = IndexFields
    }}.


execute(#cursor{db = Db, index = Idx,limit=Limit,opts=Opts} = Cursor0, UserFun, UserAcc) ->
    DbName = Db#db.name,
    DDoc = ddocid(Idx),
    IndexName = mango_idx:name(Idx),
    % Get the Query arguments and Options
    QueryArgs0 = parse_selector(Cursor0#cursor.selector),
    SortQuery = sort_query(Opts),
    % Set limit and sort
    QueryArgs = QueryArgs0#index_query_args{
        include_docs = true,
        limit = Limit,
        sort=SortQuery
    },
    case dreyfus_fabric_search:go(DbName, DDoc, IndexName, QueryArgs) of
        {ok, Bookmark0, _, Hits0, Counts0, Ranges0} ->
            Hits = hits_to_json(DbName, true, Hits0),
            Bookmark = dreyfus_fabric_search:pack_bookmark(Bookmark0),
            Counts = case Counts0 of
                undefined ->
                    [];
                _ ->
                    [{counts, facets_to_json(Counts0)}]
            end,
            Ranges = case Ranges0 of
                undefined ->
                    [];
                _ ->
                    [{ranges, facets_to_json(Ranges0)}]
            end,
            {ok, UserAcc0} = UserFun({row, {Counts}}, UserAcc),
            {ok, UserAcc1} = UserFun({row, {Ranges}}, UserAcc0),
            {ok, UserAcc2} = UserFun({row, {[{bookmark,Bookmark}]}}, UserAcc1),
            {ok,lists:foldl(fun(Hit,Acc) -> 
                {ok, {Resp1, ",\r\n"}} = UserFun({row, Hit}, Acc),
                {Resp1, ",\r\n"}
               end, UserAcc2,Hits)};
        {error, Reason} ->
            ?MANGO_ERROR({text_search_error, {error,Reason}})
    end.

%% Query For Dreyfus Sort, Covert <<"Field">>,<<"desc">> to <<"-Field">>
%% and append to the dreyfus query
sort_query(Opts) ->
    {sort, {Sort}} = lists:keyfind(sort, 1, Opts),
    SortList = lists:map(fun(SortField) -> 
        {Field,Dir} = SortField,
        case Dir of
            <<"asc">> -> Field;
            <<"desc">> -> <<"-",Field/binary>>;
            _ -> Field
        end
    end,Sort),
    case SortList of
        [] -> relevance;
        _ -> SortList
    end.


ddocid(Idx) ->
    case mango_idx:ddoc(Idx) of
        <<"_design/", Rest/binary>> ->
            Rest;
        Else ->
            Else
    end.

%% Parse Selector when no options provided
parse_selector({[{<<"$text">>,Value}]}) when is_binary(Value) ->
    #index_query_args{q = Value};
parse_selector({[{<<"$text">>,Value}]}) when is_integer(Value) ->
    BinVal = list_to_binary(integer_to_list(Value)),
    #index_query_args{q = BinVal};
parse_selector({[{<<"$text">>,Value}]}) when is_float(Value) ->
    BinVal = list_to_binary(float_to_list(Value)),
    #index_query_args{q = BinVal};
parse_selector({[{<<"$text">>,Value}]}) when is_boolean(Value) ->
    Query = case Value of
        true -> <<"true">>;
        false -> <<"false">>
    end,
    #index_query_args{q=Query};

% Parse Selector With Options
parse_selector({[{<<"$text">>,Value},Opts]}) when is_binary(Value) ->
    IndexQueryArgs = parse_options(Opts),
    IndexQueryArgs#index_query_args{q = Value};
parse_selector({[{<<"$text">>,Value},Opts]}) when is_integer(Value) ->
    BinVal = list_to_binary(integer_to_list(Value)),
    IndexQueryArgs = parse_options(Opts),
   IndexQueryArgs#index_query_args{q = BinVal};
parse_selector({[{<<"$text">>,Value},Opts]}) when is_float(Value) ->
    BinVal = list_to_binary(float_to_list(Value)),
    IndexQueryArgs = parse_options(Opts),
    IndexQueryArgs#index_query_args{q = BinVal};
parse_selector({[{<<"$text">>,Value},Opts]}) when is_boolean(Value) ->
    Query = case Value of
        true -> <<"true">>;
        false -> <<"false">>
    end,
    IndexQueryArgs = parse_options(Opts),
    IndexQueryArgs#index_query_args{q = Query}.


parse_options(SearchOptions) ->
    {<<"$options">>,{Options}} = SearchOptions,
    lists:foldl (fun (Option,QueryArgsAcc) ->
        parse_option(Option,QueryArgsAcc)
    end, #index_query_args{}, Options).

parse_option({<<"$bookmark">>,Val},IndexQueryArgs) ->
    IndexQueryArgs#index_query_args{bookmark=Val};
parse_option({<<"$counts">>,Val},IndexQueryArgs) ->
    IndexQueryArgs#index_query_args{counts=Val};
parse_option({<<"$ranges">>,Val},IndexQueryArgs) ->
    IndexQueryArgs#index_query_args{ranges=Val};


%% Options below are currently not supported and won't pass the validator
%% but we leave the code here to use in future
parse_option({<<"$group">>,Val},IndexQueryArgs) ->
    IndexQueryArgs#index_query_args{grouping=Val};
parse_option({<<"$drilldown">>,Val},IndexQueryArgs) ->
    IndexQueryArgs#index_query_args{drilldown=Val};

parse_option({Option,_},_) ->
    ?MANGO_ERROR({unknown_option, {option, Option}}).


find_usable_indexes([], _) ->
    ?MANGO_ERROR({no_usable_index, query_unsupported});
find_usable_indexes(Possible, []) ->
    ?MANGO_ERROR({no_usable_index, {fields, Possible}});
find_usable_indexes(Possible, Existing) ->
    Usable = lists:foldl(fun(Idx, Acc) ->
        Columns = mango_idx:columns(Idx),
        %% Check to see if any of the Columns exist in our Possible Fields
        case sets:is_subset(sets:from_list(Possible),sets:from_list(Columns)) of
            true ->
                [Idx | Acc];
            false ->
                Acc
        end
    end, [], Existing),
    if length(Usable) > 0 -> ok; true ->
        ?MANGO_ERROR({no_usable_index, {fields, Possible}})
    end,
    Usable.

%% If no field list exists, we choose the default field with the most
%% sub fields indexed, otherwise just choose the last one created
choose_best_index(Indexes,IndexFields) -> 
    case IndexFields of
        [<<"default">>] ->
            lists:foldl(fun(Idx, Acc) ->
                PrevLen = length(mango_idx:columns(Acc)),
                case length(mango_idx:columns(Idx)) of
                    Len when Len >= PrevLen ->
                        Idx;
                    _ ->
                        Acc
                end
            end, hd(Indexes), Indexes);
        _ ->
            hd(Indexes)
    end.

%% Copied Over From Dreyfus. Refactor later when dreyfus limit=all branch
%% is merged.
facets_to_json(Facets) ->
    {[facet_to_json(F) || F <- Facets]}.

facet_to_json({K, V, []}) ->
    {hd(K), V};
facet_to_json({K0, _V0, C0}) ->
    C2 = [{tl(K1), V1, C1} || {K1, V1, C1} <- C0],
    {hd(K0), facets_to_json(C2)}.

hits_to_json(DbName, IncludeDocs, Hits) ->
    {Ids, HitData} = lists:unzip(lists:map(fun get_hit_data/1, Hits)),
    if IncludeDocs ->
        {ok, JsonDocs} = dreyfus_fabric:get_json_docs(DbName, Ids),
        lists:zipwith(fun({Id, Order, Fields}, {Id, Doc}) ->
                {[{id, Id}, {order, Order}, {fields, {Fields}}, Doc]}
            end, HitData, JsonDocs);
    true ->
        lists:map(fun({Id, Order, Fields}) ->
            {[{id, Id}, {order, Order}, {fields, {Fields}}]}
        end, HitData)
    end.

get_hit_data(Hit) ->
    Id = couch_util:get_value(<<"_id">>, Hit#hit.fields),
    Fields = lists:keydelete(<<"_id">>, 1, Hit#hit.fields),
    {Id, {Id, Hit#hit.order, Fields}}.
