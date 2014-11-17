-module(mango_cursor).


-export([
    create/3,
    execute/3,
    get_sort_indexes/3
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").
-include("mango_cursor.hrl").


-define(SUPERVISOR, mango_cursor_sup).

create(Db, Selector0, Opts) ->
    Mod = mango_opts:index_cursor_type(Selector0),
    Mod:create(Db,Selector0,Opts).


execute(#cursor{index=Idx}=Cursor, UserFun, UserAcc) ->
    Mod = mango_idx:cursor_mod(Idx),
    Mod:execute(Cursor, UserFun, UserAcc).


get_sort_indexes(ExistingIndexes, UsableIndexes, Opts) ->
    % If a sort was specified we have to find an index that
    % can satisfy the request.
    case lists:keyfind(sort, 1, Opts) of
        {sort, {[_ | _]} = Sort} ->
            limit_to_sort(ExistingIndexes, UsableIndexes, Sort);
        _ ->
            UsableIndexes
    end.


limit_to_sort(ExistingIndexes, UsableIndexes, Sort) ->
    Fields = mango_sort:fields(Sort),
    % First make sure that we have an index that could
    % answer this sort. We split like this so that the
    % user error is more obvious.
    SortFilt = fun(Idx) ->
        Cols = mango_idx:columns(Idx),
        case mango_idx:type(Idx) of
            <<"text">> ->
                sets:is_subset(sets:from_list(Fields),sets:from_list(Cols));
            _ ->
                lists:prefix(Fields, Cols)
        end
    end,
    SortIndexes = lists:filter(SortFilt, ExistingIndexes),
    if SortIndexes /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, {sort, Fields}})
    end,
    % And then check if one or more of our SortIndexes
    % is usable.
    UsableFilt = fun(Idx) -> lists:member(Idx, UsableIndexes) end,
    FinalIndexes = lists:filter(UsableFilt, SortIndexes),
    if FinalIndexes /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, sort_field})
    end,
    FinalIndexes.
