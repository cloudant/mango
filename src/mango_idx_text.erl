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
                {Opts0} = proplists:get_value(<<"options">>, VProps),
                Opts = lists:keydelete(<<"sort">>, 1, Opts0),
                I = #idx{
                    type = <<"text">>,
                    name = Name,
                    def = Def,
                    opts = Opts
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
    {<<"fields">>, Fields}=lists:keyfind(<<"fields">>, 1, Props),
    lists:map(fun(Field) ->
        case Field of
            B when is_binary(B) ->
                B;
            {[{Key, _}]} ->
                Key
        end
    end, Fields).
    
do_validate({Props}) ->
    {ok, Opts} = mango_opts:validate(Props, opts()),
    {ok, {Opts}};
do_validate(Else) ->
    ?MANGO_ERROR({invalid_index_text, Else}).


def_to_json({Props}) ->
    def_to_json(Props);
def_to_json([]) ->
    [];
def_to_json([{fields, Fields} | Rest]) ->
    [{<<"fields">>, mango_sort:to_json(Fields)} | def_to_json(Rest)];
def_to_json([{<<"fields">>, Fields} | Rest]) ->
    [{<<"fields">>, mango_sort:to_json(Fields)} | def_to_json(Rest)];
def_to_json([{Key, Value} | Rest]) ->
    [{Key, Value} | def_to_json(Rest)].


opts() ->
    [   
        {<<"fields">>, [
            {tag, fields},
            {validator, fun mango_opts:validate_fields/1}
        ]},
        {<<"analyzer">>, [
            {tag, analyzer},
            {optional, true},
            {default, <<"standard">>}
        ]},
         {<<"selector">>, [
            {tag, selector},
            {optional, true},
            {default, {[]}},
            {validator, fun mango_opts:validate_selector/1}
        ]}
    ].


make_text(Idx) ->
    Text= {[
        {<<"index">>, add_default_field(Idx#idx.def)},
        %% options also has a list of fields. we don't add a default field
        %% because we don't reference it during index creation or search
        %% this may change later?
        {<<"options">>, {Idx#idx.opts}}
    ]},
    {Idx#idx.name, Text}.

%% This default field is added to all indexes. It indexes
%% all fields provided by the user, including sub fields.
add_default_field({[{fields, Fields}, Analyzer, Selector]}) ->
    FinalFields=lists:foldl(fun (Field, FieldsAcc) ->
        case Field of
            {[{FieldName, {FieldOpts}}]} ->
                case couch_util:get_value(<<"doc_fields">>, FieldOpts) of
                    [] -> [FieldName | FieldsAcc];
                    undefined -> [FieldName | FieldsAcc];
                    Else -> lists:append(FieldsAcc, Else)
                end;
            FieldName when is_binary(FieldName) ->
                [FieldName | FieldsAcc];
            FieldName ->
                ?MANGO_ERROR({invalid_index_type, FieldName})
        end
    end, [], Fields),
    DefaultField = {[{<<"default">>,
        {[{<<"facet">>, false},
        {<<"index">>, true},
        {<<"doc_fields">>, FinalFields},
        {<<"store">>, false}]}}]},
    {[{fields, [DefaultField|Fields]}, Analyzer, Selector]}.
