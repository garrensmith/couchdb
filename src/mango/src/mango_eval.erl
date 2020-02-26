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


-module(mango_eval).
-behavior(couch_eval).


-export([
    acquire_map_context/1,
    release_map_context/1,
    map_docs/2
]).


-include_lib("couch/include/couch_db.hrl").
-include("mango_idx.hrl").


-define(JS, <<"javascript">>).



acquire_map_context(Opts) ->
    #{
        db_name := DbName,
        ddoc_id := DDocId,
        language := Language,
%%        sig := Sig,
%%        lib := Lib,
        map_funs := MapFuns
    } = Opts,
    Def = hd(MapFuns),
    Idx = #idx{
        type = <<"json">>,
        dbname = DbName,
        ddoc = DDocId,
        def = Def,
        partitioned = false
    },
    io:format("MANGO EVAL ~p IDx ~p ~n", [Opts, Idx]),
    {ok, Idx}.


release_map_context(Idx) ->
    ok.


map_docs(Idx, Docs) ->
    Out = {ok, lists:map(fun(Doc) ->
        Json = couch_doc:to_json_obj(Doc, []),
        Results = mango_indexer:index_doc([Idx], Json),
        {Doc#doc.id, Results}
    end, Docs)},
    io:format("MAPPED  Doc resu ~p ~n", [Out]),
    Out.
