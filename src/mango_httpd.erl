% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
% http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(mango_httpd).


-export([
    handle_req/2
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").


handle_req(#httpd{} = Req, Db0) ->
    try
        Db = set_user_ctx(Req, Db0),
        handle_req_int(Req, Db)
    catch
        throw:{mango_error, Module, Reason} ->
            %Stack = erlang:get_stacktrace(),
            %twig:log(err, "Error: ~s :: ~w~n~p", [Module, Reason, Stack]),
            {Code, ErrorStr, ReasonStr} = mango_error:info(Module, Reason),
            Resp = {[
                {<<"error">>, ErrorStr},
                {<<"reason">>, ReasonStr}
            ]},
            chttpd:send_json(Req, Code, Resp)
    end.


handle_req_int(#httpd{path_parts=[_, <<"_index">> | _]} = Req, Db) ->
    handle_index_req(Req, Db);
handle_req_int(#httpd{path_parts=[_, <<"_explain">> | _]} = Req, Db) ->
    handle_explain_req(Req, Db);
handle_req_int(#httpd{path_parts=[_, <<"_find">> | _]} = Req, Db) ->
    handle_find_req(Req, Db);
handle_req_int(_, _) ->
    throw({not_found, missing}).


handle_index_req(#httpd{method='GET', path_parts=[_, _]}=Req, Db) ->
    Idxs = lists:sort(mango_idx:list(Db)),
    JsonIdxs = lists:map(fun mango_idx:to_json/1, Idxs),
	chttpd:send_json(Req, {[{indexes, JsonIdxs}]});

handle_index_req(#httpd{method='POST', path_parts=[_, _]}=Req, Db) ->
    {ok, Opts} = mango_opts:validate_idx_create(chttpd:json_body_obj(Req)),
    {ok, Idx0} = mango_idx:new(Db, Opts),
    {ok, Idx} = mango_idx:validate(Idx0),
    Id = mango_idx:ddoc(Idx),
    Name = mango_idx:name(Idx),
    {ok, DDoc} = mango_util:load_ddoc(Db, mango_idx:ddoc(Idx)),
    Status = case mango_idx:add(DDoc, Idx) of
        {ok, DDoc} ->
            <<"exists">>;
        {ok, NewDDoc} ->
            CreateOpts = get_idx_w_opts(Opts),
            case mango_crud:insert(Db, NewDDoc, CreateOpts) of
                {ok, [{RespProps}]} ->
                    case lists:keyfind(error, 1, RespProps) of
                        {error, Reason} ->
                            ?MANGO_ERROR({error_saving_ddoc, Reason});
                        _ ->
                            <<"created">>
                    end;
                _ ->
                    ?MANGO_ERROR(error_saving_ddoc)
            end
    end,
	chttpd:send_json(Req, {[{result, Status}, {id, Id}, {name, Name}]});

%% Essentially we just iterate through the list of ddoc ids passed in and
%% delete one by one. If an error occurs, all previous documents will be
%% deleted, but an error will be thrown for the current ddoc id.
handle_index_req(#httpd{method='POST', path_parts=[_, <<"_index">>,
        <<"_bulk_delete">>]}=Req, Db) ->
    {ok, Opts} = mango_opts:validate_bulk_delete(chttpd:json_body_obj(Req)),
    Idxs = mango_idx:list(Db),
    DDocs = get_bulk_delete_ddocs(Opts),
    DelOpts = get_idx_w_opts(Opts),
    {Success, Fail} = lists:foldl(fun(DDocId0, {Success0, Fail0}) ->
        DDocId = convert_to_design_id(DDocId0),
        Filt = fun(Idx) -> mango_idx:ddoc(Idx) == DDocId end,
        Id = {<<"id">>, DDocId},
        case mango_idx:delete(Filt, Db, Idxs, DelOpts) of
            {ok, true} ->
                {[{[Id, {<<"ok">>, true}]} | Success0], Fail0};
            {error, Error} ->
                {Success0, [{[Id, {<<"error">>, Error}]} | Fail0]}
        end
    end, {[], []}, DDocs),
    chttpd:send_json(Req, {[{<<"success">>, Success}, {<<"fail">>, Fail}]});

handle_index_req(#httpd{method='DELETE',
        path_parts=[A, B, <<"_design">>, DDocId0, Type, Name]}=Req, Db) ->
    PathParts = [A, B, <<"_design/", DDocId0/binary>>, Type, Name],
    handle_index_req(Req#httpd{path_parts=PathParts}, Db);

handle_index_req(#httpd{method='DELETE',
        path_parts=[_, _, DDocId0, Type, Name]}=Req, Db) ->
    Idxs = mango_idx:list(Db),
    DDocId = convert_to_design_id(DDocId0),
    DelOpts = get_idx_del_opts(Req),
    Filt = fun(Idx) ->
        IsDDoc = mango_idx:ddoc(Idx) == DDocId,
        IsType = mango_idx:type(Idx) == Type,
        IsName = mango_idx:name(Idx) == Name,
        IsDDoc andalso IsType andalso IsName
    end,
    case mango_idx:delete(Filt, Db, Idxs, DelOpts) of
        {ok, true} ->
            chttpd:send_json(Req, {[{ok, true}]});
        {error, not_found} ->
            throw({not_found, missing});
        {error, Error} ->
            ?MANGO_ERROR({error_saving_ddoc, Error})
    end;

handle_index_req(Req, _Db) ->
    chttpd:send_method_not_allowed(Req, "GET,POST,DELETE").


handle_explain_req(#httpd{method='POST'}=Req, Db) ->
    {ok, Opts0} = mango_opts:validate_find(chttpd:json_body_obj(Req)),
    {value, {selector, Sel}, Opts} = lists:keytake(selector, 1, Opts0),
    Resp = mango_crud:explain(Db, Sel, Opts),
    chttpd:send_json(Req, Resp);

handle_explain_req(Req, _Db) ->
    chttpd:send_method_not_allowed(Req, "POST").


handle_find_req(#httpd{method='POST'}=Req, Db) ->
    {ok, Opts0} = mango_opts:validate_find(chttpd:json_body_obj(Req)),
    {value, {selector, Sel}, Opts} = lists:keytake(selector, 1, Opts0),
    {ok, Resp0} = start_find_resp(Req),
    {ok, {Resp1, _, KVs}} = run_find(Resp0, Db, Sel, Opts),
    end_find_resp(Resp1, KVs);

handle_find_req(Req, _Db) ->
    chttpd:send_method_not_allowed(Req, "POST").


set_user_ctx(#httpd{user_ctx=Ctx}, Db) ->
    Db#db{user_ctx=Ctx}.


get_idx_w_opts(Opts) ->
    case lists:keyfind(w, 1, Opts) of
        {w, N} when is_integer(N), N > 0 ->
            [{w, integer_to_list(N)}];
        _ ->
            [{w, "2"}]
    end.


get_bulk_delete_ddocs(Opts) ->
    case lists:keyfind(docids, 1, Opts) of
        {docids, DDocs} when is_list(DDocs) ->
            DDocs;
        _ ->
            []
    end.


get_idx_del_opts(Req) ->
    try
        WStr = chttpd:qs_value(Req, "w", "2"),
        _ = list_to_integer(WStr),
        [{w, WStr}]
    catch _:_ ->
        [{w, "2"}]
    end.


convert_to_design_id(DDocId) ->
    case DDocId of
        <<"_design/", _/binary>> -> DDocId;
        _ -> <<"_design/", DDocId/binary>>
    end.


start_find_resp(Req) ->
    chttpd:start_delayed_json_response(Req, 200, [], "{\"docs\":[").


end_find_resp(Resp0, KVs) ->
    FinalAcc = lists:foldl(fun({K, V}, Acc) ->
        JK = ?JSON_ENCODE(K),
        JV = ?JSON_ENCODE(V),
        [JV, ": ", JK, ",\r\n" | Acc]
    end, ["\r\n]"], KVs),
    Chunk = lists:reverse(FinalAcc, ["}\r\n"]),
    {ok, Resp1} = chttpd:send_delayed_chunk(Resp0, Chunk),
    chttpd:end_delayed_json_response(Resp1).


run_find(Resp, Db, Sel, Opts) ->
    mango_crud:find(Db, Sel, fun handle_doc/2, {Resp, "\r\n", []}, Opts).


handle_doc({add_key, Key, Value}, {Resp, Prepend, KVs}) ->
    NewKVs = lists:keystore(Key, 1, KVs, {Key, Value}),
    {ok, {Resp, Prepend, NewKVs}};
handle_doc({row, Doc}, {Resp0, Prepend, KVs}) ->
    Chunk = [Prepend, ?JSON_ENCODE(Doc)],
    {ok, Resp1} = chttpd:send_delayed_chunk(Resp0, Chunk),
    {ok, {Resp1, ",\r\n", KVs}}.
