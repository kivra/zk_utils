%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Utility functions for zookeeper
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(zk_utils).

%%%_* Exports ==========================================================
-export([ensure_path/2]).

%%%_* API ==============================================================
-spec ensure_path(pid(), string()) -> {ok, [string()]} | {error, atom()}.
ensure_path(Pid, Path) ->
  FolderList = string:tokens(Path, "/"),
  PrefixPaths = get_prefix_paths(FolderList),
  lists:map(fun(Folder) -> ensure_folder(Pid, Folder) end, PrefixPaths),
  erlzk:get_children(Pid, Path).

%%%_* Internal =========================================================
%% Determines the path to every Node on the way to a special node (all parents).
get_prefix_paths([]) ->
  [];
get_prefix_paths([ Head | Tail]) ->
  PrefixTails = get_prefix_paths(Tail),
  HeadedPrefixTails = lists:map(fun(PathTail) ->
                                    ("/" ++ Head ++ PathTail)
                                end, PrefixTails),
  ["/" ++ Head | HeadedPrefixTails].

%% Ensures one single node exists. (parent node is expected to exist.
ensure_folder(Pid, PrefixPath) ->
  case erlzk:get_children(Pid, PrefixPath) of
    {ok, _I} ->
      ok;
    {error, _I} ->
      erlzk:create(Pid, PrefixPath)
  end.

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
