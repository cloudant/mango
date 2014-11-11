-module(mango_cursor_text).

-export([
    create/3,
    execute/3
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("dreyfus/include/dreyfus.hrl").
-include("mango_cursor.hrl").
-include("mango.hrl").


create(Db, Selector, Opts) ->
    IndexFields = mango_selector:index_fields(Selector),  

    if IndexFields /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, operator_unsupported})
    end,

    ExistingIndexes = mango_idx:filter_list(mango_idx:list(Db),[<<"text">>]),
    UsableIndexes = find_usable_indexes(IndexFields,ExistingIndexes),
    SortIndexes = mango_cursor:get_sort_indexes(ExistingIndexes, UsableIndexes, Opts),
    Index = hd(SortIndexes),
    Limit = couch_util:get_value(limit, Opts, 50),
    %%The default passed in from Opts is 10000000000, which dreyfus will not accept
    %%For now, we cap it at 50
    CapLimit = case Limit > 50 of 
        true -> 50;  %% Should we throw an error instead?
        false -> Limit
    end,
    Skip = couch_util:get_value(skip, Opts, 0),
    Fields = couch_util:get_value(fields, Opts, all_fields),

    {ok, #cursor{
        db = Db,
        index = Index,
        ranges = null,
        selector = Selector,
        opts = Opts,
        limit = CapLimit,
        skip = Skip,
        fields = Fields
    }}.

execute(#cursor{db = Db, index = Idx,limit=Limit,opts=Opts} = Cursor0, UserFun, UserAcc) ->
    DbName = Db#db.name,
    DDoc = ddocid(Idx),
    IndexName = mango_idx:name(Idx),
    SearchQuery = parse_selector(Cursor0#cursor.selector),
    SortQuery = sort_query(Opts),
    QueryArgs = #index_query_args{
        q = SearchQuery,
        include_docs = true,
        limit = Limit,
        sort=SortQuery
    },
    case dreyfus_fabric_search:go(DbName, DDoc, IndexName, QueryArgs) of
        {ok, Bookmark0, TotalHits, Hits0} -> % legacy clause
            Hits = dreyfus_util:hits_to_json(DbName, true, Hits0),
            Bookmark = dreyfus_fabric_search:pack_bookmark(Bookmark0),
            {ok,lists:foldl(fun(Hit,Acc) -> 
                {ok, {Resp1, ",\r\n"}} = UserFun({row, Hit}, Acc),
                {Resp1, ",\r\n"}
               end, UserAcc,Hits)};
        {ok, Bookmark0, TotalHits, Hits0, Counts0, Ranges0} ->
            Hits = dreyfus_util:hits_to_json(DbName, true, Hits0),
            Bookmark = dreyfus_fabric_search:pack_bookmark(Bookmark0),
            {ok,lists:foldl(fun(Hit,Acc) -> 
                {ok, {Resp1, ",\r\n"}} = UserFun({row, Hit}, Acc),
                {Resp1, ",\r\n"}
               end, UserAcc,Hits)}
    end.

%% Query For Dreyfus Sort, Covert <<"Field">>,<<"desc">> to <<"-Field">>
sort_query(Opts) ->
   {sort, {Sort}} = lists:keyfind(sort, 1, Opts),
   lists:map(fun(SortField) -> 
        {Field,Dir} = SortField,
        case Dir of
            <<"asc">> -> Field;
            <<"desc">> -> <<"-",Field/binary>>;
            _ -> Field
        end
   end,Sort).


ddocid(Idx) ->
    case mango_idx:ddoc(Idx) of
        <<"_design/", Rest/binary>> ->
            Rest;
        Else ->
            Else
    end.

%our query is basically a concatentation of the fieldname and the text search string
%what happens when they don't pass in a field name, what is the default?
parse_selector({[{FieldName,{[{<<"$text">>,Value}]}}]}) when is_binary(Value) ->
    <<FieldName/binary,":",Value/binary>>;
parse_selector({[{FieldName,{[{<<"$text">>,Value}]}}]}) when is_integer(Value) ->
    BinVal = list_to_binary(integer_to_list(Value)),
    <<FieldName/binary,":",BinVal/binary>>;
parse_selector({[{FieldName,{[{<<"$text">>,Value}]}}]}) when is_float(Value) ->
    BinVal = list_to_binary(float_to_list(Value)),
    <<FieldName/binary,":",BinVal/binary>>;
parse_selector({[{FieldName,{[{<<"$text">>,Value}]}}]}) when is_boolean(Value) ->
    case Value of
        true -> <<FieldName/binary,":true">>;
        false -> <<FieldName/binary,":false">>
    end.

find_usable_indexes([], _) ->
    ?MANGO_ERROR({no_usable_index, query_unsupported});
find_usable_indexes(Possible, []) ->
    ?MANGO_ERROR({no_usable_index, {fields, Possible}});
find_usable_indexes(Possible, Existing) ->
    Usable = lists:foldl(fun(Idx, Acc) ->
        Columns = mango_idx:columns(Idx),
        %% Check to see if any of the Columns exist in our Possible Fields
        case sets:is_disjoint(sets:from_list(Columns),sets:from_list(Possible)) of
            true ->
                Acc;
            false ->
                [Idx | Acc]
        end
    end, [], Existing),
    if length(Usable) > 0 -> ok; true ->
        ?MANGO_ERROR({no_usable_index, {fields, Possible}})
    end,
    Usable.

