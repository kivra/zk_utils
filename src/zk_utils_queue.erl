%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%%   Simple queue abstraction
%%%
%%% @copyright Bjorn Jensen-Urstad 2014
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(zk_utils_queue).

%%%_* Exports ==========================================================
-export([ enqueue/3
        , dequeue/2
        , to_list/2
        ]).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Code =============================================================
%%%_ * API -------------------------------------------------------------
-spec enqueue(term(), string(), pid()) -> ok.
enqueue(Term, Queue, Pid) ->
  {ok, _} = erlzk:create(Pid, Queue ++ "_", term_to_binary(Term),
                         persistent_sequential),
  ok.

-spec dequeue(string(), pid()) -> {ok, term()} | {error, empty}.
dequeue(Queue, Pid) ->
  case seqnos(Queue, Pid) of
    [Seqno|_] ->
      EntryPath           = Queue ++ "_" ++ Seqno,
      {ok, {TermBin, _}}  = erlzk:get_data(Pid, EntryPath),
      ok                  = erlzk:delete(Pid, EntryPath),
      {ok, ?b2t(TermBin)};
    [] ->
      {error, empty}
  end.

-spec to_list(string(), pid()) -> [term()].
to_list(Queue, Pid) ->
  lists:map(fun(Seqno) ->
                EntryPath = Queue ++ "_" ++ Seqno,
                {ok, {TermBin, _}} = erlzk:get_data(Pid, EntryPath),
                binary_to_term(TermBin)
            end, seqnos(Queue, Pid)).

%%%_ * Internals  ------------------------------------------------------
seqnos(Queue, Pid) ->
  {ok, List} = erlzk:get_children(Pid, filename:dirname(Queue)),
  QueueName  = filename:basename(Queue),
  Seqnos     = lists:filtermap(
                 fun(File) ->
                     %% TODO: support '_' in queuename
                     case string:tokens(File, "_") of
                       [QueueName, Seqno] -> {true, Seqno};
                       [_,         _    ] -> false %something else
                     end
                 end, List),
  lists:sort(fun(A, B) -> A < B end, Seqnos).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
