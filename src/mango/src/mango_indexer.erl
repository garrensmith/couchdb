% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.


-module(mango_indexer).


-export([
    modify/4
]).


-export([
    write_doc/3,
    index_doc/2
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango.hrl").
-include("mango_idx.hrl").

-include_lib("couch_mrview/include/couch_mrview.hrl").


modify(Db, Change, Doc, PrevDoc) ->
    try
        modify_int(Db, Change, Doc, PrevDoc)
    catch
        Error:Reason ->
            #{
                name := DbName
            } = Db,

            io:format("ERROR ~p ~p ~p ~n", [Error, Reason, erlang:display(erlang:get_stacktrace())]),

            Id = doc_id(Doc, PrevDoc),
            couch_log:error("Mango index error for Db ~s Doc ~p ~p ~p",
                [DbName, Id, Error, Reason])
    end,
    ok.


doc_id(undefined, #doc{id = DocId}) ->
    DocId;
doc_id(undefined, _) ->
    <<"unknown_doc_id">>;
doc_id(#doc{id = DocId}, _) ->
    DocId.


% Check if design doc is mango index and kick off background worker
% to build the new index
modify_int(Db, _Change, #doc{id = <<?DESIGN_DOC_PREFIX, _/binary>>} = Doc,
        _PrevDoc) ->
    #{
        name := DbName
    } = Db,

    {Props} = JsonDoc = couch_doc:to_json_obj(Doc, []),
    case proplists:get_value(<<"language">>, Props) of
        <<"query">> ->
%%            [Idx] = mango_idx:from_ddoc(Db, JsonDoc),
            {ok, Mrst} = couch_mrview_util:ddoc_to_mrst(DbName, Doc),
            couch_views_fdb:create_build_vs(Db, Mrst),
            {ok, _} = couch_views_jobs:build_view_async(Db, Mrst, true);
        _ ->
            ok
    end;

%%modify_int(Db, deleted, _, PrevDoc)  ->
%%    remove_doc(Db, PrevDoc, json_indexes(Db));

%%modify_int(Db, updated, Doc, PrevDoc) ->
%%    Indexes = json_indexes(Db),
%%    remove_doc(Db, PrevDoc, Indexes),
%%    write_doc(Db, Doc, Indexes);
%%
modify_int(Db, _, Doc, _) ->
    write_doc(Db, Doc, json_indexes(Db));
%%
%%modify_int(Db, recreated, Doc, _) ->
%%    write_doc(Db, Doc, json_indexes(Db));

modify_int(_, _, _, _) ->
    ok.


write_doc(Db, #doc{deleted = Deleted} = Doc, Indexes) ->
    #doc{id = DocId} = Doc,
    JsonDoc = mango_json:to_binary(couch_doc:to_json_obj(Doc, [])),

    lists:foreach(fun (Idx) ->
        DocResult0 = #{
            id => DocId,
            results => []
        },

        DocResult1 = case Deleted of
            true ->
                DocResult0#{deleted => true};
            false ->
                Results = index_doc([Idx], JsonDoc),
                DocResult0#{results => Results}
        end,

        io:format("HERE ~p ~n", [DocResult1]),
        DbName = mango_idx:dbname(Idx),
        DDoc = mango_idx:ddoc(Idx),
        {ok, Mrst} = couch_mrview_util:ddoc_to_mrst(DbName, DDoc),
        #mrst{
            sig = Sig,
            views = Views
        } = Mrst,
        io:format("WRITE ~p ~n", [Views]),
        couch_views_fdb:write_doc(Db, Sig, Views, DocResult1)
    end, Indexes).


json_indexes(Db) ->
    lists:filter(fun (Idx) ->
        Idx#idx.type == <<"json">>
    end, mango_idx:list(Db)).


index_doc(Indexes, Doc) ->
    lists:map(fun(Idx) ->
        {IdxDef} = mango_idx:def(Idx),
        io:format("DEF ~p ~n", [IdxDef]),
        Results = get_index_entries(IdxDef, Doc),
        case lists:member(not_found, Results) of
            true ->
                [];
            false ->
                [{Results, null}]
        end
    end, Indexes).


get_index_entries(IdxDef, Doc) ->
    {Fields} = couch_util:get_value(<<"fields">>, IdxDef),
    Selector = get_index_partial_filter_selector(IdxDef),
    case should_index(Selector, Doc) of
        false ->
            [not_found];
        true ->
            io:format("SHOUD INDEX ~p ~n", [Fields]),
            get_index_values(Fields, Doc)
    end.


get_index_values(Fields, Doc) ->
    lists:map(fun({Field, _Dir}) ->
        case mango_doc:get_field(Doc, Field) of
            not_found -> not_found;
            bad_path -> not_found;
            Value -> Value
        end
    end, Fields).


get_index_partial_filter_selector(IdxDef) ->
    case couch_util:get_value(<<"partial_filter_selector">>, IdxDef, {[]}) of
        {[]} ->
            % this is to support legacy text indexes that had the
            % partial_filter_selector set as selector
            couch_util:get_value(<<"selector">>, IdxDef, {[]});
        Else ->
            Else
    end.


should_index(Selector, Doc) ->
    NormSelector = mango_selector:normalize(Selector),
    Matches = mango_selector:match(NormSelector, Doc),
    IsDesign = case mango_doc:get_field(Doc, <<"_id">>) of
        <<"_design/", _/binary>> -> true;
        _ -> false
    end,
    Matches and not IsDesign.
