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

-module(couch_views_indexer).

-export([
    spawn_link/0
]).


-export([
    init/0,
    fetch_docs/2
]).

-include("couch_views.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("fabric/include/fabric2.hrl").

% TODO:
%  * Handle timeouts of transaction and other errors


spawn_link() ->
    proc_lib:spawn_link(?MODULE, init, []).


init() ->
    {ok, Job, Data} = couch_jobs:accept(?INDEX_JOB_TYPE, #{}),
    #{
        <<"db_name">> := DbName,
        <<"ddoc_id">> := DDocId,
        <<"sig">> := JobSig,
        <<"retries">> := Retries,
        <<"build_to_vs">> := BuildToVS
    } = Data,
    io:format("BUILDING ~p ~n", [Data]),

    {ok, Db} = try
        fabric2_db:open(DbName, [?ADMIN_CTX])
    catch error:database_does_not_exist ->
        couch_jobs:finish(undefined, Job, Data#{
            error => db_deleted,
            reason => "Database was deleted"
        }),
        exit(normal)
    end,

    {ok, DDoc} = case fabric2_db:open_doc(Db, DDocId) of
        {ok, DDoc0} ->
            {ok, DDoc0};
        {not_found, _} ->
            couch_jobs:finish(undefined, Job, Data#{
                error => ddoc_deleted,
                reason => "Design document was deleted"
            }),
            exit(normal)
    end,

    {ok, Mrst} = couch_views_util:ddoc_to_mrst(DbName, DDoc),
    HexSig = fabric2_util:to_hex(Mrst#mrst.sig),

    if  HexSig == JobSig -> ok; true ->
        couch_jobs:finish(undefined, Job, Data#{
            error => sig_changed,
            reason => <<"Design document was modified">>
        }),
        exit(normal)
    end,

    State = #{
        tx_db => undefined,
        db_seq => undefined,
        view_seq => undefined,
        last_seq => undefined,
        view_vs => undefined,
        build_to_vs => BuildToVS,
        job => Job,
        job_data => Data,
        count => 0,
        limit => num_changes(),
        doc_acc => [],
        design_opts => Mrst#mrst.design_opts
    },

    try
        update(Db, Mrst, State)
    catch
        exit:normal ->
            ok;
        Error:Reason  ->
            io:format("ERROR ~p ~p ~p ~n", [Error, Reason, erlang:display(erlang:get_stacktrace())]),
            NewRetry = Retries + 1,
            RetryLimit = retry_limit(),

            case should_retry(NewRetry, RetryLimit, Reason) of
                true ->
                    DataErr = Data#{<<"retries">> := NewRetry},
                    % Set the last_seq to 0 so that it doesn't trigger a
                    % successful view build for anyone listening to the
                    % couch_views_jobs:wait_for_job
                    % Note this won't cause the view to rebuild from 0 again
                    StateErr = State#{job_data := DataErr, last_seq := <<"0">>},
                    report_progress(StateErr, update);
                false ->
                    NewData = add_error(Error, Reason, Data),
                    couch_jobs:finish(undefined, Job, NewData),
                    exit(normal)
            end
    end.


% Transaction limit exceeded don't retry
should_retry(_, _, {erlfdb_error, 2101}) ->
    false;

should_retry(Retries, RetryLimit, _) when Retries < RetryLimit ->
    true;

should_retry(_, _, _) ->
    false.


add_error(error, {erlfdb_error, Code}, Data) ->
    CodeBin = couch_util:to_binary(Code),
    CodeString = erlfdb:get_error_string(Code),
    Data#{
        error => foundationdb_error,
        reason => list_to_binary([CodeBin, <<"-">>, CodeString])
    };

add_error(Error, Reason, Data) ->
    Data#{
        error => couch_util:to_binary(Error),
        reason => couch_util:to_binary(Reason)
    }.


update(#{} = Db, Mrst0, State0) ->
    {Mrst2, State4} = fabric2_fdb:transactional(Db, fun(TxDb) ->
        % In the first iteration of update we need
        % to populate our db and view sequences
        State1 = get_update_start_state(TxDb, Mrst0, State0),

        {ok, State2} = fold_changes(State1),

        #{
            count := Count,
            limit := Limit,
            doc_acc := DocAcc,
            last_seq := LastSeq,
            view_vs := ViewVs,
            build_to_vs := BuildToVS
        } = State2,

        DocAcc1 = fetch_docs(TxDb, DocAcc),
        {Mrst1, MappedDocs} = map_docs(Mrst0, DocAcc1),
        write_docs(TxDb, Mrst1, MappedDocs, State2),

        case Count < Limit of
            true ->
                if BuildToVS == false -> ok; true ->
                    couch_views_fdb:set_build_vs(TxDb, Mrst1, ViewVs, ?INDEX_READY)
                end,
                io:format("FINISHED ~n"),
                report_progress(State2, finished),
                {Mrst1, finished};
            false ->
                State3 = report_progress(State2, update),
                {Mrst1, State3#{
                    tx_db := undefined,
                    count := 0,
                    doc_acc := [],
                    view_seq := LastSeq
                }}
        end
    end),

    case State4 of
        finished ->
            couch_eval:release_map_context(Mrst2#mrst.qserver);
        _ ->
            update(Db, Mrst2, State4)
    end.


get_update_start_state(TxDb, Mrst, #{db_seq := undefined} = State) ->
    #{
        job := Job,
        job_data := Data,
        build_to_vs := BuildToVS
    } = State,

    {ViewVS, BuildState} = couch_views_fdb:get_build_vs(TxDb, Mrst),

    if BuildToVS == false -> ok; true ->
        if BuildState == ?INDEX_BUILDING -> ok; true ->
            couch_jobs:finish(undefined, Job, Data#{
                error => index_built,
                reason => <<"Index is already built">>
            }),
            exit(normal)
        end
    end,

    ViewSeq = couch_views_fdb:get_update_seq(TxDb, Mrst),

    State#{
        tx_db := TxDb,
        db_seq := fabric2_db:get_update_seq(TxDb),
        view_vs := ViewVS,
        view_seq := ViewSeq,
        last_seq := ViewSeq
    };

get_update_start_state(TxDb, _Idx, State) ->
    State#{
        tx_db := TxDb
    }.


fold_changes(State) ->
    #{
        view_seq := SinceSeq,
        limit := Limit,
        tx_db := TxDb
    } = State,

    Fun = fun process_changes/2,
    Opts = [{limit, Limit}, {restart_tx, false}],
    fabric2_db:fold_changes(TxDb, SinceSeq, Fun, State, Opts).


process_changes(Change, Acc) ->
    #{
        doc_acc := DocAcc,
        count := Count,
        design_opts := DesignOpts,
        view_vs := ViewVs,
        build_to_vs := BuildToVS
    } = Acc,

    #{
        id := Id,
        sequence := LastSeq
    } = Change,

    IncludeDesign = lists:keymember(<<"include_design">>, 1, DesignOpts),

    DocVS = fabric2_fdb:next_vs(fabric2_fdb:seq_to_vs(LastSeq)),
    case BuildToVS == true andalso ViewVs =< DocVS of
        true ->
            {stop, Acc};
        false ->
            Acc1 = case {Id, IncludeDesign} of
                {<<?DESIGN_DOC_PREFIX, _/binary>>, false} ->
                    maps:merge(Acc, #{
                        count => Count + 1,
                        last_seq => LastSeq
                    });
                _ ->
                    Acc#{
                        doc_acc := DocAcc ++ [Change],
                        count := Count + 1,
                        last_seq := LastSeq
                    }
            end,
            {ok, Acc1}
    end.


map_docs(Mrst, Docs) ->
    % Run all the non deleted docs through the view engine and
    Mrst1 = start_query_server(Mrst),
    QServer = Mrst1#mrst.qserver,

    {Deleted0, NotDeleted0} = lists:partition(fun(Doc) ->
        #{deleted := Deleted} = Doc,
        Deleted
    end, Docs),

    Deleted1 = lists:map(fun(Doc) ->
        Doc#{results => []}
    end, Deleted0),

    DocsToMap = lists:map(fun(Doc) ->
        #{doc := DocRec} = Doc,
        DocRec
    end, NotDeleted0),

    {ok, AllResults} = couch_eval:map_docs(QServer, DocsToMap),

    % The expanded function head here is making an assertion
    % that the results match the given doc
    NotDeleted1 = lists:zipwith(fun(#{id := DocId} = Doc, {DocId, Results}) ->
        Doc#{results => Results}
    end, NotDeleted0, AllResults),

    % I'm being a bit careful here resorting the docs
    % in order of the changes feed. Theoretically this is
    % unnecessary since we're inside a single transaction.
    % However, I'm concerned if we ever split this up
    % into multiple transactions that this detail might
    % be important but forgotten.
    MappedDocs = lists:sort(fun(A, B) ->
        #{sequence := ASeq} = A,
        #{sequence := BSeq} = B,
        ASeq =< BSeq
    end, Deleted1 ++ NotDeleted1),

    {Mrst1, MappedDocs}.


write_docs(TxDb, Mrst, Docs, State) ->
    #mrst{
        views = Views,
        sig = Sig
    } = Mrst,

    #{
        last_seq := LastSeq
    } = State,

    lists:foreach(fun(Doc) ->
        couch_views_fdb:write_doc(TxDb, Sig, Views, Doc)
    end, Docs),

    couch_views_fdb:set_update_seq(TxDb, Sig, LastSeq).


fetch_docs(Db, Changes) ->
    {Deleted, NotDeleted} = lists:partition(fun(Doc) ->
        #{deleted := Deleted} = Doc,
        Deleted
    end, Changes),

    RevState = lists:foldl(fun(Change, Acc) ->
        #{id := Id} = Change,
        RevFuture = fabric2_fdb:get_winning_revs_future(Db, Id, 1),
        Acc#{
            RevFuture => {Id, Change}
        }
    end, #{}, NotDeleted),

    RevFutures = maps:keys(RevState),
    BodyState = lists:foldl(fun(RevFuture, Acc) ->
        {Id, Change} = maps:get(RevFuture, RevState),
        Revs = fabric2_fdb:get_winning_revs_wait(Db, RevFuture),

        % I'm assuming that in this changes transaction that the winning
        % doc body exists since it is listed in the changes feed as not deleted
        #{winner := true} = RevInfo = lists:last(Revs),
        BodyFuture = fabric2_fdb:get_doc_body_future(Db, Id, RevInfo),
        Acc#{
            BodyFuture => {Id, RevInfo, Change}
        }
    end, #{}, erlfdb:wait_for_all(RevFutures)),

    BodyFutures = maps:keys(BodyState),
    ChangesWithDocs = lists:map(fun (BodyFuture) ->
        {Id, RevInfo, Change} = maps:get(BodyFuture, BodyState),
        Doc = fabric2_fdb:get_doc_body_wait(Db, Id, RevInfo, BodyFuture),
        Change#{doc => Doc}
    end, erlfdb:wait_for_all(BodyFutures)),

    % This combines the deleted changes with the changes that contain docs
    % Important to note that this is now unsorted. Which is fine for now
    % But later could be an issue if we split this across transactions
    Deleted ++ ChangesWithDocs.


start_query_server(#mrst{qserver = nil} = Mrst) ->
    #mrst{
        db_name = DbName,
        idx_name = DDocId,
        language = Language,
        sig = Sig,
        lib = Lib,
        views = Views
    } = Mrst,
    {ok, QServer} = couch_eval:acquire_map_context(
            DbName,
            DDocId,
            Language,
            Sig,
            Lib,
            [View#mrview.def || View <- Views]
        ),
    Mrst#mrst{qserver = QServer};

start_query_server(#mrst{} = Mrst) ->
    Mrst.


report_progress(State, UpdateType) ->
    #{
        tx_db := TxDb,
        job := Job1,
        job_data := JobData,
        last_seq := LastSeq,
        build_to_vs := BuildToVS
    } = State,

    #{
        <<"db_name">> := DbName,
        <<"ddoc_id">> := DDocId,
        <<"sig">> := Sig,
        <<"retries">> := Retries
    } = JobData,

    % Reconstruct from scratch to remove any
    % possible existing error state.
    NewData = #{
        <<"db_name">> => DbName,
        <<"ddoc_id">> => DDocId,
        <<"sig">> => Sig,
        <<"view_seq">> => LastSeq,
        <<"retries">> => Retries,
        <<"build_to_vs">> => BuildToVS
    },

    case UpdateType of
        update ->
            case couch_jobs:update(TxDb, Job1, NewData) of
                {ok, Job2} ->
                    State#{job := Job2};
                {error, halt} ->
                    couch_log:error("~s job halted :: ~w", [?MODULE, Job1]),
                    exit(normal)
            end;
        finished ->
            case couch_jobs:finish(TxDb, Job1, NewData) of
                ok ->
                    State;
                {error, halt} ->
                    couch_log:error("~s job halted :: ~w", [?MODULE, Job1]),
                    exit(normal)
            end
    end.


num_changes() ->
    config:get_integer("couch_views", "change_limit", 100).


retry_limit() ->
    config:get_integer("couch_views", "retry_limit", 3).
