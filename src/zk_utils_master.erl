%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%%   master/mistress election
%%%
%%% @copyright Bjorn Jensen-Urstad 2014
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(zk_utils_master).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% api
-export([ start_link/1
        , stop/1
        ]).

%% gen_server
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Macros ===========================================================

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { func     = error('s.func')
           , lock     = error('s.lock')
           , mm_pid   = undefined        %pid() | undefined
             %% zk related
           , zk_seqno = error('s.seqno')
           , zk_pid   = error('s.pid')
           , zk_state = candidate        %candidate | leader
           }).

%%%_ * API -------------------------------------------------------------
start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

stop(Ref) ->
  gen_server:call(Ref, stop).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  process_flag(trap_exit, true),
  {ok, Func}  = s2_lists:assoc(Args, func),
  {ok, Lock}  = s2_lists:assoc(Args, lock),
  {ok, ZkPid} = s2_lists:assoc(Args, zk_pid),
  Seqno       = init_lock(ZkPid, Lock),
  {ok, #s{ func     = Func
         , lock     = Lock
         , zk_seqno = Seqno
         , zk_pid   = ZkPid}, 0}.

terminate(Rsn, S) ->
  [exit(S#s.mm_pid, Rsn) || S#s.mm_pid =/= undefined],
  case ezk:delete(S#s.zk_pid, S#s.lock ++ "_" ++ S#s.zk_seqno) of
    {ok, _}                        -> ok;
    {error, {no_zk_connection, _}} -> ok
  end.

handle_call(stop, _From, S) ->
  {stop, normal, ok, S}.

handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

%% received when connection is lost
handle_info({watchlost, _Msg, _Data} = Msg, S) ->
  {stop, {watchlost, Msg}, S};
%% received if someone else changes our z-node (ie: should not happen..)
handle_info({self, {_Path, _Type, _SyncCon}} = Msg, S) ->
  {stop, {self_changed, Msg}, S};
%% received when parent node changes
handle_info({daddy, {_Path, _Type, _SyncCon}} = Msg,
            #s{zk_state=leader} = S) ->
  {stop, {unexpected_msg, Msg}, S};
handle_info({daddy, {_Path, _Type, _SyncCon}},
            #s{zk_state=candidate} = S) ->
  case try_get_lock(S#s.zk_pid, S#s.zk_seqno, S#s.lock, S#s.func) of
    {ok, Pid}     -> {noreply, S#s{zk_state=leader, mm_pid=Pid}};
    {error, wait} -> {noreply, S}
  end;
%% initial attempt to get lock
handle_info(timeout, S) ->
  case try_get_lock(S#s.zk_pid, S#s.zk_seqno, S#s.lock, S#s.func) of
    {ok, Pid}     -> {noreply, S#s{zk_state=leader, mm_pid=Pid}};
    {error, wait} -> {noreply, S}
  end;
handle_info({'EXIT', _Pid, Rsn}, S) ->
  {stop, Rsn, S};
handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
init_lock(Pid, Lock) ->
  {ok, _}    = ezk:ensure_path(Pid, filename:dirname(Lock)),
  %% create a node with ephemeral and sequence flag set
  {ok, Path} = ezk:create(Pid, Lock ++ "_", <<"">>, es),
  %% figure out which seqence number we got and put a constant watch on
  %% that one. This should not be necessary but if someone fucks with us
  %% and deletes our node we want to know.
  {_, Seqno} = split(filename:basename(Path)),
  {ok, _}    = ezk:ls(Pid, Path, self(), self),
  Seqno.

try_get_lock(Pid, MySeqno, Lock, Func) ->
  %% figure out who else is trying to grab the lock
  {ok, Files} = ezk:ls(Pid, filename:dirname(Lock)),
  Service     = filename:basename(Lock),
  Seqnos      = lists:filtermap(
                  fun(File) ->
                      case split(?b2l(File)) of
                        {Service, Seqno} -> {true, Seqno};
                        {_,       _    } -> false
                      end
                  end, Files),
  %% figure out who has largest sequence number that is still
  %% smaller than our sequence number.
  %% If we don't find any it means we are the leader otherwise
  %% the number belongs to the one 'just' before us.
  case lists:foldl(
         fun(Seqno, undefined) when Seqno < MySeqno -> Seqno;
            (Seqno, Mon) when Mon =/= undefined,
                              Seqno > Mon,
                              Seqno < MySeqno       -> Seqno;
            (_, Mon)                                -> Mon
         end, undefined, Seqnos) of
    undefined ->
      %% no smaller number found, we must be the leader
      {ok, spawn_link(Func)};
    Monitor ->
      %% monitor the one before us for change
      case ezk:ls(Pid, Lock ++ "_" ++ Monitor, self(), daddy) of
        {ok, _}         -> {error, wait};
        {error, no_dir} ->
          %% something happened to that one and it disappeared, rerun
          %% the whole function again.
          try_get_lock(Pid, MySeqno, Lock, Func)
      end
  end.

%% split seqno and service into tuple
split(File) ->
  [Seqno|Service] = string:tokens(lists:reverse(File), "_"),
  {lists:reverse(string:join(Service, "_")), lists:reverse(Seqno)}.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-else.
-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
