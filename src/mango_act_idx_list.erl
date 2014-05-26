-module(mango_act_idx_list).

-export([
    init/2,
    run/3,
    
    format_error/1
]).


init(Db, {Props}) ->
    {ok, Opts} = mango_opts:validate(Props, opts()),
    [<<"list_indexes">>] =Opts.

run(Resp, Db, St) ->
    Result = format_results(Db),
    {ok, {[
        {ok, true},
        {result, {Result}}
    ]}}.

format_results(Db) ->
    Indexes = mango_index:list(Db),    
    Result= lists:flatmap(fun(Idx) ->
    IndexSize=mango_index:get_index_size(element(2,Idx),element(3,Idx)), 
    case element(6,Idx) of
        all_docs ->
        IdxInfo = {[{<<"fields">>,<<"all_docs">>},{<<"ddocid">>,element(3,Idx)}, {<<"disk_size">>,<<"N/A">>}]};
        _ ->    
        {Fields} = element(6,Idx),
        IdxInfo = {Fields ++ [{<<"ddocid">>,element(3,Idx)}] ++ [IndexSize]}  
    end,
    [{element(4,Idx),IdxInfo
    }] end, Indexes),
    Result.
   
format_error(Else) ->
    mango_util:fmt("Unknown error: ~p", [Else]).

opts() ->
    [
        {<<"action">>, [
            {assert, <<"list_indexes">>}
        ]}
    ].


