-module(mango_cursor).


-export([
    create/3,
    execute/3
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").
-include("mango_cursor.hrl").


-define(SUPERVISOR, mango_cursor_sup).


create(Db, Selector0, Opts) ->
    Selector = mango_selector:normalize(Selector0),

    ExistingIndexes = mango_idx:list(Db),
    if ExistingIndexes /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, no_indexes_defined})
    end,

    SortIndexes = mango_idx:for_sort(ExistingIndexes, Opts),
    if SortIndexes /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, missing_sort_index})
    end,

    UsableFilter = fun(I) -> mango_idx:is_usable(I, Selector) end,
    UsableIndexes = lists:filter(UsableFilter, SortIndexes),
    if UsableIndexes /= [] -> ok; true ->
        ?MANGO_ERROR({no_usable_index, selector_unsupported})
    end,

    create_cursor(Db, UsableIndexes, Selector, Opts).


execute(#cursor{index=Idx}=Cursor, UserFun, UserAcc) ->
    Mod = mango_idx:cursor_mod(Idx),
    Mod:execute(Cursor, UserFun, UserAcc).


create_cursor(Db, Indexes, Selector, Opts) ->
    [{CursorMod, CursorModIndexes} | _] = group_indexes_by_type(Indexes),
    CursorMod:create(Db, CursorModIndexes, Selector, Opts).


group_indexes_by_type(Indexes) ->
    IdxDict = lists:foldl(fun(I, D) ->
        dict:append(mango_idx:cursor_mod(I), I, D)
    end, dict:new(), Indexes),
    CursorModules = [
        mango_cursor_view
    ],
    lists:flatmap(fun(CMod) ->
        case dict:find(CMod, IdxDict) of
            {ok, CModIndexes} ->
                [{CMod, CModIndexes}];
            error ->
                []
        end
    end, CursorModules).
