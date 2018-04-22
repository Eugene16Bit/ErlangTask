-module(map_reduce).

-export([start/1, map/2, reduce/3]).


%% Counting words in files. Using map/reduce function.
%% Spawning map's actor for each file. Map function pass on result to 
%% the reduce's actor after finish. Reduce actor collect data from all
%% map's actors and send final value to a main actor.
start(FileList) ->
	ListLength = length(FileList),
	ReducePid = spawn(?MODULE, reduce, [self(), ListLength, maps:new()]),
	StartMapFunc = fun (FileName) -> spawn(?MODULE, map, [ReducePid, FileName]) end,
	lists:foreach(StartMapFunc, FileList), 
	receive
		{ReducePid, {ok, ReduceData}} -> ReduceData;
		_ -> error
	end.


%% Map function for one file. Collect all words from text file into  
%% a map (key -> word, value -> word's count) and send it to a Reduce actor.
map(ReducePid, FileName) ->
	case file:read_file(FileName) of
		{error, Reason} -> 
			ReducePid ! {self(), {error, Reason}}; 
		{ok, BinaryData} ->
			BinList = binary:split(BinaryData, [<<"\n">>, <<" ">>], [global]),
			AllWords = [unicode:characters_to_list(X) || X <- BinList],
			Response = {ok, count_words(AllWords, maps:new())},
			ReducePid ! {self(), Response}
	end.


%% Using by map function. Easy way to couting words.
count_words([], Acc) -> Acc;
count_words([Word | T], Acc) ->
	case maps:find(Word, Acc) of
		error ->
			NewMap = maps:put(Word, 1, Acc),
			count_words(T, NewMap);
		{ok, Value} ->
			NewMap = maps:put(Word, Value + 1, Acc),
			count_words(T, NewMap)
	end.


%% Reduce fuction. Calling by map actors. Collect all map actors data
%% to a single map (key -> word, value -> word's count) and return it
%% to the master actor. 
reduce(MainPid, 0, ReduceData) -> MainPid ! {self(), {ok, ReduceData}};
reduce(MainPid, N, ReduceData) ->
	receive
		{_Pid, {ok, MapResult}} ->
			MapList = maps:to_list(MapResult),
			NewReduceData = collect_map_output(MapList, ReduceData),
			reduce(MainPid, N - 1, NewReduceData);
		{_Pid, {error, _Reason}} -> 
			reduce(MainPid, N - 1, ReduceData)
	end.


%% Using by reduce function. 
collect_map_output([], ReduceData) -> ReduceData;
collect_map_output([{Word, Count} | T], ReduceData) ->
	case maps:find(Word, ReduceData) of
		{ok, Value} ->
			NewValue = Value + Count;
		error ->
			NewValue = Count
	end,
	collect_map_output(T, maps:put(Word, NewValue, ReduceData)).

