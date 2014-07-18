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
enqueue(Term, Queue, Pid) ->
  {ok, _} = ezk:create(Pid, Queue ++ "_", term_to_binary(Term), s),
  ok.

dequeue(Queue, Pid) ->
  case seqnos(Queue, Pid) of
    [Seqno|_] ->
      EntryPath           = Queue ++ "_" ++ Seqno,
      {ok, {TermBin, _}}  = ezk:get(Pid, EntryPath),
      {ok, _}             = ezk:delete(Pid, EntryPath),
      {ok, ?b2t(TermBin)};
    [] ->
      {error, empty}
  end.

to_list(Queue, Pid) ->
  lists:map(fun(Seqno) ->
                EntryPath = Queue ++ "_" ++ Seqno,
                {ok, {TermBin, _}} = ezk:get(Pid, EntryPath),
                binary_to_term(TermBin)
            end, seqnos(Queue, Pid)).

%%%_ * Internals  ------------------------------------------------------
seqnos(Queue, Pid) ->
  {ok, List} = ezk:ls(Pid, filename:dirname(Queue)),
  QueueName  = filename:basename(Queue),
  Seqnos     = lists:filtermap(
                 fun(File) ->
                     %% TODO: support '_' in queuename
                     case string:tokens(?b2l(File), "_") of
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
