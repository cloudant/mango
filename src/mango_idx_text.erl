-module(mango_idx_text).


-export([
    validate/1,
    add/2,
    remove/2,
    from_ddoc/1,
    to_json/1,
    columns/1
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").
-include("mango_idx.hrl").

validate(#idx{}=Idx) ->
    {ok, Def} = do_validate(Idx#idx.def),
    %TODO: Validate the analyzer json? 
    {ok, Idx#idx{def=Def}}.


add(#doc{body={Props0}}=DDoc, Idx) ->
    Texts1 = case proplists:get_value(<<"indexes">>, Props0) of
        {Texts0} -> Texts0;
        _ -> []
    end,
    NewText = make_text(Idx),
    Texts2 = lists:keystore(element(1, NewText), 1, Texts1, NewText),
    Props1 = lists:keystore(<<"indexes">>, 1, Props0, {<<"indexes">>, {Texts2}}),
    {ok, DDoc#doc{body={Props1}}}.


remove(#doc{body={Props0}}=DDoc, Idx) ->
    Texts1 = case proplists:get_value(<<"indexes">>, Props0) of
        {Texts0} ->
            Texts0;
        _ ->
            ?MANGO_ERROR({index_not_found, Idx#idx.name})
    end,
    Texts2 = lists:keydelete(Idx#idx.name, 1, Texts1),
    if Texts2 /= Texts1 -> ok; true ->
        ?MANGO_ERROR({index_not_found, Idx#idx.name})
    end,
    Props1 = case Texts2 of
        [] ->
            lists:keydelete(<<"indexes">>, 1, Props0);
        _ ->
            lists:keystore(<<"indexes">>, 1, Props0, {<<"indexes">>, {Texts2}})
    end,
    {ok, DDoc#doc{body={Props1}}}.


from_ddoc({Props}) ->
    case lists:keyfind(<<"indexes">>, 1, Props) of
        {<<"indexes">>, {Texts}} when is_list(Texts) ->
            lists:flatmap(fun({Name, {VProps}}) ->
                Def = proplists:get_value(<<"index">>, VProps),
                % {Opts0} = proplists:get_value(<<"options">>, VProps),
                %Opts = lists:keydelete(<<"sort">>, 1, Opts0),
                I = #idx{
                    type = <<"text">>,
                    name = Name,
                    def = Def
                    % opts = Opts
                },
                % TODO: Validate the index definition
                [I]
            end, Texts);
        _ ->
            []
    end.


to_json(Idx) ->
    {[
        {ddoc, Idx#idx.ddoc},
        {name, Idx#idx.name},
        {type, Idx#idx.type},
        {def, {def_to_json(Idx#idx.def)}}
    ]}.


columns(Idx) ->
    {Props} = Idx#idx.def,
    {<<"fields">>, Fields} = lists:keyfind(<<"fields">>, 1, Props),
    case Fields of
        <<"all_fields">> ->
            all_fields;
        _ ->
            [{FieldName,FieldType} || {[{_, FieldName}, {_, FieldType}, _]} <- Fields]
    end.

    
do_validate({Props}) ->
    {ok, Opts} = mango_opts:validate(Props, opts()),
    {ok, {Opts}};
do_validate(Else) ->
    ?MANGO_ERROR({invalid_index_text, Else}).


def_to_json({Props}) ->
    def_to_json(Props);
def_to_json([]) ->
    [];
def_to_json([{<<"fields">>, <<"all_fields">>} | Rest]) ->
    [{<<"fields">>, []} | def_to_json(Rest)];
def_to_json([{fields, Fields} | Rest]) ->
    [{<<"fields">>, mango_sort:to_json(Fields)} | def_to_json(Rest)];
def_to_json([{<<"fields">>, Fields} | Rest]) ->
    [{<<"fields">>, mango_sort:to_json(Fields)} | def_to_json(Rest)];
def_to_json([{Key, Value} | Rest]) ->
    [{Key, Value} | def_to_json(Rest)].


opts() ->
    [   
        {<<"default_analyzer">>, [
            {tag, default_analyzer},
            {optional, true},
            {default, <<"keyword">>}
        ]},
        {<<"default_field">>, [
            {tag, default_field},
            {optional, true},
            {default, {[]}}
        ]},
         {<<"selector">>, [
            {tag, selector},
            {optional, true},
            {default, {[]}},
            {validator, fun mango_opts:validate_selector/1}
        ]},
        {<<"fields">>, [
            {tag, fields},
            {optional, true},
            {default, []},
            {validator, fun mango_opts:validate_fields/1}
        ]}
    ].


make_text(Idx) ->
    Text= {[
        {<<"index">>, Idx#idx.def},
        {<<"analyzer">>, construct_analyzer(Idx#idx.def)}
    ]},
    {Idx#idx.name, Text}.


construct_analyzer({Props}) ->
    % twig:log(notice, "Props: ~p", [Props]),
    DefaultAnalyzer = couch_util:get_value(default_analyzer, Props, "keyword"),
    {DefaultField, DefaultFieldAnalyzer} = case lists:keyfind(default_field, 1, Props) of
        {[{<<"enabled">>, true}, {<<"analyzer">>, Analyzer}]} ->
            {true, Analyzer};
        {[{<<"enabled">>, false}, _]} ->
            {false, <<"standard">>};
        _ ->
            {true, <<"standard">>}
        end,
    Fields = couch_util:get_value(fields, Props, all_fields),
    PerFieldAnalyzerList = case Fields of
        all_fields ->
            [];
        _ ->
            lists:foldl(fun ({Field}, Acc) ->
            {<<"field">>, FieldName} = lists:keyfind(<<"field">>, 1, Field),
            {<<"type">>, FieldType} = lists:keyfind(<<"type">>, 1, Field),
            case lists:keyfind(<<"analyzer">>, 1, Field) of
                false ->
                    Acc;
                {<<"analyzer">>, PerFieldAnalyzer} ->
                    [{<<FieldName/binary, ":", FieldType/binary>>, PerFieldAnalyzer} | Acc]
            end
            end,[],Fields)
        end,
    FinalAnalyzerDef = case DefaultField of
        true ->
           [{<<"default">>, DefaultFieldAnalyzer} | PerFieldAnalyzerList];
        _ ->
            PerFieldAnalyzerList
        end,
    case FinalAnalyzerDef of
        [] ->
            "keyword";
        _ ->
            {[{<<"name">>, <<"perfield">>}, {<<"default">>, DefaultAnalyzer},  {<<"fields">>, {FinalAnalyzerDef}}]}
    end.
