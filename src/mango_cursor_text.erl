-module(mango_cursor_text).

-export([
    create/4,
    explain/1,
    execute/3
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("dreyfus/include/dreyfus.hrl").
-include("mango_cursor.hrl").
-include("mango.hrl").


-record(cacc, {
    selector,
    dbname,
    ddocid,
    idx_name,
    query_args,
    bookmark,
    limit,
    skip,
    user_fun,
    user_acc
}).


create(Db, Indexes, Selector, Opts0) ->
    Index = case Indexes of
        [Index0] ->
            Index0;
        _ ->
            ?MANGO_ERROR(multiple_text_indexes)
    end,

    Opts = unpack_bookmark(Db#db.name, Opts0),

    % Limit the result set size to 50 for Clouseau's
    % sake. We may want to revisit this.
    Limit0 = couch_util:get_value(limit, Opts, 50),
    Limit = if Limit0 < 50 -> Limit0; true -> 50 end,
    Skip = couch_util:get_value(skip, Opts, 0),
    Fields = couch_util:get_value(fields, Opts, all_fields),

    {ok, #cursor{
        db = Db,
        index = Index,
        ranges = null,
        selector = Selector,
        opts = Opts,
        limit = Limit,
        skip = Skip,
        fields = Fields
    }}.


explain(Cursor) ->
    #cursor{
        selector = Selector,
        opts = Opts
    } = Cursor,
    [
        {'query', mango_selector_text:convert(Selector)},
        {sort, sort_query(Opts, Selector)}
    ].


execute(Cursor, UserFun, UserAcc) ->
    #cursor{
        db = Db,
        index = Idx,
        limit = Limit,
        skip = Skip,
        selector = Selector,
        opts = Opts
    } = Cursor,
    QueryArgs = #index_query_args{
        q = mango_selector_text:convert(Selector),
        sort = sort_query(Opts, Selector),
        raw_bookmark = true
    },
    CAcc = #cacc{
        selector = Selector,
        dbname = Db#db.name,
        ddocid = ddocid(Idx),
        idx_name = mango_idx:name(Idx),
        bookmark = get_bookmark(Opts),
        limit = Limit,
        skip = Skip,
        query_args = QueryArgs,
        user_fun = UserFun,
        user_acc = UserAcc
    },
    try
        execute(CAcc)
    catch
        throw:{stop, FinalCAcc} ->
            #cacc{
                bookmark = FinalBM,
                user_fun = UserFun,
                user_acc = LastUserAcc
            } = FinalCAcc,
            JsonBM = dreyfus_bookmark:pack(FinalBM),
            Arg = {add_key, bookmark, JsonBM},
            {_Go, FinalUserAcc} = UserFun(Arg, LastUserAcc),
            {ok, FinalUserAcc}
    end.


execute(CAcc) ->
    case search_docs(CAcc) of
        {ok, Bookmark, []} ->
            % If we don't have any results from the
            % query it means the request has paged through
            % all possible results and the request is over.
            NewCAcc = CAcc#cacc{bookmark = Bookmark},
            throw({stop, NewCAcc});
        {ok, Bookmark, Hits} ->
            NewCAcc = CAcc#cacc{bookmark = Bookmark},
            HitDocs = get_json_docs(CAcc#cacc.dbname, Hits),
            {ok, FinalCAcc} = handle_hits(NewCAcc, HitDocs),
            execute(FinalCAcc)
    end.


search_docs(CAcc) ->
    #cacc{
        dbname = DbName,
        ddocid = DDocId,
        idx_name = IdxName
    } = CAcc,
    QueryArgs = update_query_args(CAcc),
    case dreyfus_fabric_search:go(DbName, DDocId, IdxName, QueryArgs) of
        {ok, Bookmark, _, Hits, _, _} ->
            {ok, Bookmark, Hits};
        {error, Reason} ->
            ?MANGO_ERROR({text_search_error, {error, Reason}})
    end.


handle_hits(CAcc, []) ->
    {ok, CAcc};

handle_hits(CAcc0, [{Sort, Doc} | Rest]) ->
    CAcc1 = handle_hit(CAcc0, Sort, Doc),
    handle_hits(CAcc1, Rest).


handle_hit(CAcc0, Sort, Doc) ->
    #cacc{
        limit = Limit,
        skip = Skip
    } = CAcc0,
    CAcc1 = update_bookmark(CAcc0, Sort),
    case mango_selector:match(CAcc1#cacc.selector, Doc) of
        true when Skip > 0 ->
            CAcc1#cacc{skip = Skip - 1};
        true when Limit == 0 ->
            % We hit this case if the user spcified with a
            % zero limit. Notice that in this case we need
            % to return the bookmark from before this match
            throw({stop, CAcc0});
        true when Limit == 1 ->
            NewCAcc = apply_user_fun(CAcc1, Doc),
            throw({stop, NewCAcc});
        true when Limit > 1 ->
            NewCAcc = apply_user_fun(CAcc1, Doc),
            NewCAcc#cacc{limit = Limit - 1};
        false ->
            CAcc1
    end.


apply_user_fun(CAcc, Doc) ->
    #cacc{
        user_fun = UserFun,
        user_acc = UserAcc
    } = CAcc,
    case UserFun({row, Doc}, UserAcc) of
        {ok, NewUserAcc} ->
            CAcc#cacc{user_acc = NewUserAcc};
        {stop, NewUserAcc} ->
            throw({stop, CAcc#cacc{user_acc = NewUserAcc}})
    end.


%% Convert Query to Dreyfus sort specifications
%% Covert <<"Field">>, <<"desc">> to <<"-Field">>
%% and append to the dreyfus query
sort_query(Opts, Selector) ->
    {sort, {Sort}} = lists:keyfind(sort, 1, Opts),
    SortList = lists:map(fun(SortField) ->
        RawSortField = case SortField of
            {Field, <<"asc">>} -> Field;
            {Field, <<"desc">>} -> <<"-", Field/binary>>;
            Field when is_binary(Field) -> Field
        end,
        mango_selector_text:append_sort_type(RawSortField, Selector)
    end, Sort),
    case SortList of
        [] -> relevance;
        _ -> SortList
    end.


get_bookmark(Opts) ->
    case lists:keyfind(bookmark, 1, Opts) of
        {_, BM} when is_list(BM), BM /= [] ->
            BM;
        _ ->
            nil
    end.


update_bookmark(CAcc, Sortable) ->
    BM = CAcc#cacc.bookmark,
    QueryArgs = CAcc#cacc.query_args,
    Sort = QueryArgs#index_query_args.sort,
    NewBM = dreyfus_bookmark:update(Sort, BM, [Sortable]),
    CAcc#cacc{bookmark = NewBM}.


pack_bookmark(Bookmark) ->
    case dreyfus_bookmark:pack(Bookmark) of
        null -> nil;
        Enc -> Enc
    end.


unpack_bookmark(DbName, Opts) ->
    NewBM = case lists:keyfind(bookmark, 1, Opts) of
        {_, nil} ->
            [];
        {_, Bin} ->
            try
                dreyfus_bookmark:unpack(DbName, Bin)
            catch _:_ ->
                ?MANGO_ERROR({invalid_bookmark, Bin})
            end
    end,
    lists:keystore(bookmark, 1, Opts, {bookmark, NewBM}).


ddocid(Idx) ->
    case mango_idx:ddoc(Idx) of
        <<"_design/", Rest/binary>> ->
            Rest;
        Else ->
            Else
    end.


update_query_args(CAcc) ->
    #cacc{
        bookmark = Bookmark,
        query_args = QueryArgs
    } = CAcc,
    QueryArgs#index_query_args{
        bookmark = pack_bookmark(Bookmark),
        limit = get_limit(CAcc)
    }.


get_limit(CAcc) ->
    Total = CAcc#cacc.limit + CAcc#cacc.skip,
    if
        Total < 25 -> 25;
        Total > 100 -> 100;
        true -> Total
    end.


get_json_docs(DbName, Hits) ->
    Ids = lists:map(fun(#sortable{item = Item}) ->
        couch_util:get_value(<<"_id">>, Item#hit.fields)
    end, Hits),
    {ok, IdDocs} = dreyfus_fabric:get_json_docs(DbName, Ids),
    lists:map(fun(#sortable{item = Item} = Sort) ->
        Id = couch_util:get_value(<<"_id">>, Item#hit.fields),
        case lists:keyfind(Id, 1, IdDocs) of
            {Id, {doc, Doc}} ->
                {Sort, Doc};
            false ->
                {Sort, not_found}
        end
    end, Hits).

