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
    Mod = mango_selector:index_cursor_type(Selector),
    Mod:create(Db,Selector0,Opts).


execute(#cursor{index=Idx}=Cursor, UserFun, UserAcc) ->
    Mod = mango_idx:cursor_mod(Idx),
    Mod:execute(Cursor, UserFun, UserAcc).

