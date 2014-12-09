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
    Selector = mango_selector:normalize(Selector0),
    Fields = case couch_util:get_value(fields, Opts, all_fields) of
        all_fields -> [];
        <<"all_fields">> -> [];
        Else -> Else
    end,
    IndexFields = [<<"default">> | Fields],
    % twig:log(notice, "Index Fields ~p", [IndexFields]),
    ExistingIndexes = mango_idx:filter_list(mango_idx:list(Db), [<<"text">>]),
    UsableIndexes = find_usable_indexes(IndexFields, ExistingIndexes),
    if UsableIndexes /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, operator_unsupported})
    end,
    SortIndexes = mango_cursor:get_sort_indexes(ExistingIndexes, UsableIndexes, Opts),
    Index = choose_best_index(SortIndexes, IndexFields),
    Limit = couch_util:get_value(limit, Opts, 50),
    %% Currently set the Limit at 50. We want to set this
    %% in mango_opts to be consistent with view queries.
    CapLimit = case Limit > 50 of 
        true -> 50;
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


execute(#cursor{db = Db, index = Idx, limit=Limit, opts=Opts} = Cursor0, UserFun, UserAcc) ->
    DbName = Db#db.name,
    DDoc = ddocid(Idx),
    IndexName = mango_idx:name(Idx),
    % twig:log(notice, "Cursor Selector ~p", [Cursor0#cursor.selector]),
    Query = mango_text_selector:parse_selector(Cursor0#cursor.selector),
    % SortQuery = sort_query(Opts),
    QueryArgs = #index_query_args{
        q = Query,
        include_docs = true,
        limit = Limit
        % sort=SortQuery
    },
    case dreyfus_fabric_search:go(DbName, DDoc, IndexName, QueryArgs) of
        {ok, Bookmark0, _, Hits0, _, _} ->
            Hits = hits_to_json(DbName, true, Hits0, Cursor0#cursor.selector),
            % twig:log(notice, "Hits ~p", [Hits]),
            Bookmark = dreyfus_fabric_search:pack_bookmark(Bookmark0),
            UserAcc1 = try UserFun({row, {[{bookmark, Bookmark}]}}, UserAcc) of
                {ok, NewAcc} -> NewAcc;
                {stop, Acc} -> Acc
            catch
                error:{error, Error}  -> ?MANGO_ERROR({text_search_error, {error, Error}})
            end,
            {ok, lists:foldl(fun(Hit, HAcc) ->
                try UserFun({row, Hit}, HAcc) of
                    {ok, HNewAcc} -> HNewAcc;
                    {stop, HAcc} -> HAcc
                catch
                    error:{error, HError}  -> ?MANGO_ERROR({text_search_error, {error, HError}})
                end
            end, UserAcc1, Hits)};
        {error, Reason} ->
            ?MANGO_ERROR({text_search_error, {error, Reason}})
    end.

%% Convert Query to Dreyfus sort specifications 
%% Covert <<"Field">>, <<"desc">> to <<"-Field">>
%% and append to the dreyfus query
sort_query(Opts) ->
    {sort, {Sort}} = lists:keyfind(sort, 1, Opts),
    SortList = lists:map(fun(SortField) -> 
        case SortField of
            {Field, <<"asc">>} -> Field;
            {Field, <<"desc">>} -> <<"-", Field/binary>>;
            Field when is_binary(Field) -> Field
        end
    end, Sort),
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


find_usable_indexes(Possible, []) ->
    ?MANGO_ERROR({no_usable_index, {fields, Possible}});
%% If the user did not specify any fields, then return all existing text indexes
find_usable_indexes([<<"default">>], Existing) ->
    Existing;
find_usable_indexes(Possible, Existing) ->
    % twig:log(notice,"Possible ~p", [Possible]),
    Usable = lists:foldl(fun(Idx, Acc) ->
        Columns = mango_idx:columns(Idx),
        % twig:log(notice,"Columns ~p", [Columns]),
        %% Check to see if any of the Columns exist in our Possible Fields
        case sets:is_subset(sets:from_list(Possible), sets:from_list(Columns)) of
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

%% If no field list exists, we choose the an index that has <<"all_fields">>
%% or any of the text indexes
choose_best_index(Indexes, IndexFields) ->
    case IndexFields of
        [<<"default">>] ->
            lists:foldl(fun(Idx, Acc) ->
                case mango_idx:columns(Acc) of
                    all_fields -> Idx;
                    _ -> Acc
                end
            end, hd(Indexes), Indexes);
        _ ->
            hd(Indexes)
    end.

hits_to_json(DbName, IncludeDocs, Hits, Selector) ->
    {Ids, HitData} = lists:unzip(lists:map(fun get_hit_data/1, Hits)),
    if IncludeDocs ->
        {ok, JsonDocs} = dreyfus_fabric:get_json_docs(DbName, Ids),
        % JsonDocs0 = case mango_selector:contains_op(Selector, [<<"$size">>]) of
        %     true ->
        %         filter_results(Selector, JsonDocs);
        %     false ->
        %         JsonDocs
        % end,
        % case JsonDocs0 of
        %     [] -> 
        %         [];
        %     _ ->
        lists:zipwith(fun({Id, Order, Fields}, {Id, Doc}) ->
            {[{id, Id}, {order, Order}, {fields, {Fields}}, Doc]}
        end, HitData, JsonDocs);
        % end;
    true ->
        lists:map(fun({Id, Order, Fields}) ->
            {[{id, Id}, {order, Order}, {fields, {Fields}}]}
        end, HitData)
    end.

get_hit_data(Hit) ->
    Id = couch_util:get_value(<<"_id">>, Hit#hit.fields),
    Fields = lists:keydelete(<<"_id">>, 1, Hit#hit.fields),
    {Id, {Id, Hit#hit.order, Fields}}.

filter_results(Selector, JsonDocs) ->
    twig:log(notice, "Selector in Filter ~p", [Selector]),
    twig:log(notice, "JsonDocs ~p", [JsonDocs]),
    lists:filter(fun ({DocId, {doc, Doc}}) -> 
        twig:log(notice, "Doc in Filter ~p", [Doc]),
        mango_selector:match(Selector, Doc)
    end, JsonDocs).
