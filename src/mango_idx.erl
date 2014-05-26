% This module is for the "index object" as in, the data structure
% representing an index. Not to be confused with mango_index which
% contains APIs for managing indexes.

-module(mango_idx).


-export([
    new/3,
    validate/1,
    add/2,
    from_ddoc/2,
    special/1,

    dbname/1,
    ddoc/1,
    name/1,
    type/1,
    def/1,
    opts/1,
    columns/1,
    start_key/2,
    end_key/2,
    cursor_mod/1,
    idx_mod/1
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").
-include("mango_idx.hrl").


validate(Idx) ->
    Mod = idx_mod(Idx),
    Mod:validate(Idx).


new(Db, Def, Opts) ->
    Type = get_idx_type(Opts),
    IdxName = get_idx_name(Def, Opts),
    DDoc = get_idx_ddoc(Def, Opts),
    {ok, #idx{
        dbname = db_to_name(Db),
        ddoc = DDoc,
        name = IdxName,
        type = Type,
        def = Def,
        opts = filter_opts(Opts)
    }}.


add(DDoc, Idx) ->
    Mod = idx_mod(Idx),
    Mod:add(DDoc, Idx).


from_ddoc(Db, {Props}) ->
    DbName = db_to_name(Db),
    DDoc = proplists:get_value(<<"_id">>, Props),

    case proplists:get_value(<<"language">>, Props) of
        <<"query">> -> ok;
        _ ->
            ?MANGO_ERROR(invalid_query_ddoc_language)
    end,

    IdxMods = [mango_idx_view],
    Idxs = lists:flatmap(fun(Mod) -> Mod:from_ddoc({Props}) end, IdxMods),
    lists:map(fun(Idx) ->
        Idx#idx{
            dbname = DbName,
            ddoc = DDoc
        }
    end, Idxs).


special(Db) ->
    AllDocs = #idx{
        dbname = db_to_name(Db),
        name = <<"_all_docs">>,
        type = special,
        def = all_docs,
        opts = []
    },
    % Add one for _update_seq
    [AllDocs].


dbname(#idx{dbname=DbName}) ->
    DbName.


ddoc(#idx{ddoc=DDoc}) ->
    DDoc.


name(#idx{name=Name}) ->
    Name.


type(#idx{type=Type}) ->
    Type.


def(#idx{def=Def}) ->
    Def.


opts(#idx{opts=Opts}) ->
    Opts.


columns(#idx{}=Idx) ->
    Mod = idx_mod(Idx),
    Mod:columns(Idx).


start_key(#idx{}=Idx, Ranges) ->
    Mod = idx_mod(Idx),
    Mod:start_key(Ranges).


end_key(#idx{}=Idx, Ranges) ->
    Mod = idx_mod(Idx),
    Mod:end_key(Ranges).


cursor_mod(#idx{type=view}) ->
    mango_cursor_view;
cursor_mod(#idx{def=all_docs, type=special}) ->
    mango_cursor_view.


idx_mod(#idx{type=view}) ->
    mango_idx_view;
idx_mod(#idx{type=special}) ->
    mango_idx_special.


db_to_name(#db{name=Name}) ->
    Name;
db_to_name(Name) when is_binary(Name) ->
    Name;
db_to_name(Name) when is_list(Name) ->
    iolist_to_binary(Name).


get_idx_type(Opts) ->
    case proplists:get_value(type, Opts) of
        <<"json">> -> view;
        <<"text">> -> lucene;
        <<"geo">> -> geo;
        undefined -> view;
        _ ->
            ?MANGO_ERROR(invalid_index_type)
    end.


get_idx_ddoc(Idx, Opts) ->
    case proplists:get_value(ddoc, Opts) of
        <<"_design/", _Rest>> = Name ->
            Name;
        Name when is_binary(Name) ->
            <<"_design/", Name/binary>>;
        _ ->
            Bin = gen_name(Idx, Opts),
            <<"_design/", Bin/binary>>
    end.


get_idx_name(Idx, Opts) ->
    case proplists:get_value(name, Opts) of
        Name when is_binary(Name) ->
            Name;
        _ ->
            gen_name(Idx, Opts)
    end.


gen_name(Idx, Opts0) ->
    Opts = lists:usort(Opts0),
    TermBin = term_to_binary({Idx, Opts}),
    Sha = crypto:sha(TermBin),
    mango_util:enc_hex(Sha).


filter_opts([]) ->
    [];
filter_opts([{user_ct, _} | Rest]) ->
    filter_opts(Rest);
filter_opts([{ddoc, _} | Rest]) ->
    filter_opts(Rest);
filter_opts([{name, _} | Rest]) ->
    filter_opts(Rest);
filter_opts([{type, _} | Rest]) ->
    filter_opts(Rest);
filter_opts([Opt | Rest]) ->
    [Opt | filter_opts(Rest)].


