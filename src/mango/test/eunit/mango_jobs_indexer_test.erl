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

-module(mango_jobs_indexer_test).

-include_lib("couch/include/couch_db.hrl").
-include_lib("couch/include/couch_eunit.hrl").
%%-include_lib("mango/src/mango.hrl").
-include_lib("mango/src/mango_cursor.hrl").
-include_lib("mango/src/mango_idx.hrl").
-include_lib("couch_views/include/couch_views.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("fabric/test/fabric2_test.hrl").

indexer_test_() ->
    {
        "Test indexing",
        {
            setup,
            fun setup/0,
            fun cleanup/1,
            {
                foreach,
                fun foreach_setup/0,
                fun foreach_teardown/1,
                [
                    ?TDEF_FE(index_docs),
                    ?TDEF_FE(index_lots_of_docs, 10),
                    ?TDEF_FE(index_can_recover_from_crash, 60)
                ]
            }
        }
    }.


setup() ->
    Ctx = test_util:start_couch([
        fabric,
        couch_jobs,
        couch_views,
        mango
    ]),
    Ctx.


cleanup(Ctx) ->
    test_util:stop_couch(Ctx).


foreach_setup() ->
    DbName = ?tempdb(),
    {ok, Db} = fabric2_db:create(DbName, [{user_ctx, ?ADMIN_USER}]),
    Db.


foreach_teardown(Db) ->
    meck:unload(),
    ok = fabric2_db:delete(fabric2_db:name(Db), []).


index_docs(Db) ->
    DDoc = generate_docs(Db, 5),
    wait_while_ddoc_builds(Db),
    Docs = run_query(Db, DDoc),
    ?assertEqual([
        [{id, <<"1">>}, {value, 1}],
        [{id, <<"2">>}, {value, 2}],
        [{id, <<"3">>}, {value, 3}],
        [{id, <<"4">>}, {value, 4}],
        [{id, <<"5">>}, {value, 5}]
    ], Docs).


index_lots_of_docs(Db) ->
    DDoc = generate_docs(Db, 150),
    wait_while_ddoc_builds(Db),
    Docs = run_query(Db, DDoc),
    ?assertEqual(length(Docs), 150).


index_can_recover_from_crash(Db) ->
    meck:new(mango_indexer, [passthrough]),
    meck:expect(mango_indexer, write_doc, fun (TxDb, Doc, Idxs) ->
        Id = Doc#doc.id,
        case Id == <<"2">> of
            true ->
                meck:unload(mango_indexer),
                throw({fake_crash, test_jobs_restart});
            false ->
                meck:passthrough([TxDb, Doc, Idxs])
        end
    end),
    DDoc = generate_docs(Db, 3),
    wait_while_ddoc_builds(Db),
    Docs = run_query(Db, DDoc),
    ?assertEqual([
        [{id, <<"1">>}, {value, 1}],
        [{id, <<"2">>}, {value, 2}],
        [{id, <<"3">>}, {value, 3}]
    ], Docs).


wait_while_ddoc_builds(Db) ->
    fabric2_fdb:transactional(Db, fun(TxDb) ->
        Ready = lists:filter(fun (Idx) ->
            Idx#idx.build_status == ?INDEX_READY
        end, mango_idx:list(TxDb)),

        if length(Ready) > 1 -> ok; true ->
            timer:sleep(100),
            wait_while_ddoc_builds(Db)
        end
    end).


run_query(Db, DDoc) ->
    Args = #mrargs{
        view_type = map,
        reduce = false,
        include_docs = true,
        update = false
    },
    [Idx] = mango_idx:from_ddoc(Db, couch_doc:to_json_obj(DDoc, [])),
    Cursor = #cursor{
        db = Db,
        index = Idx,
        user_acc = []
    },
    Name = mango_idx:name(Idx),
    CB = fun query_cb/2,
    {ok, Cursor1} = couch_views:query(Db, DDoc, Name, CB, Cursor, Args),
    Acc = Cursor1#cursor.user_acc,
    lists:map(fun ({Props}) ->
        [
            {id, couch_util:get_value(<<"_id">>, Props)},
            {value, couch_util:get_value(<<"value">>, Props)}
        ]

    end, Acc).


generate_docs(Db, Count) ->
    Docs = make_docs(Count),
    fabric2_db:update_docs(Db, Docs),


    DDoc = create_idx_ddoc(Db),
    fabric2_db:update_docs(Db, [DDoc]),
    DDoc.


create_idx_ddoc(Db) ->
    Opts = [
        {def, {[{<<"fields">>,{[{<<"value">>,<<"asc">>}]}}]}},
        {type, <<"json">>},
        {name, <<"idx_01">>},
        {ddoc, auto_name},
        {w, 3},
        {partitioned, db_default}
    ],

    {ok, Idx} = mango_idx:new(Db, Opts),
    {ok, DDoc} = mango_util:load_ddoc(Db, mango_idx:ddoc_id(Idx), []),
    {ok, NewDDoc} = mango_idx:add(DDoc, Idx),
    NewDDoc.


make_docs(Count) ->
    [doc(I) || I <- lists:seq(1, Count)].


doc(Id) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, list_to_binary(integer_to_list(Id))},
        {<<"value">>, Id}
    ]}).

query_cb({row, Props}, #cursor{user_acc = Acc} = Cursor) ->
    Doc = couch_util:get_value(doc, Props),
    {ok, Cursor#cursor{
        user_acc =  Acc ++ [Doc]
    }};

query_cb(_, Cursor) ->
    {ok, Cursor}.
