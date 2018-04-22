-module(db).
-behaviour(gen_server).

-export([stop/1, new/1, create/2, read/2, update/2, delete/2,
		 init/1, handle_call/3, handle_cast/2, terminate/2]).


%% =======================================================================================			
%% API functions
%% =======================================================================================


stop(DbName) ->
	gen_server:stop({global, DbName}, normal, 30000).


new(DbName) ->
	gen_server:start_link({global, DbName}, ?MODULE, [], []),
	ok.


create(Record, DbName) ->
	gen_server:call({global, DbName}, {create, Record}).


read(Key, DbName) ->
	gen_server:call({global, DbName}, {read, Key}).


update(Record, DbName) ->
	gen_server:call({global, DbName}, {update, Record}).


delete(Key, DbName) -> 
	gen_server:call({global, DbName}, {delete, Key}).


%% =======================================================================================			
%% Behaviour functions
%% =======================================================================================


init(_Args) ->
	{ok, []}.


handle_call({create, Record}, _From, State) ->
	{Key, _UserName, _City} = Record,
	case lists:keyfind(Key, 1, State) of
		{Key,_,_} ->
			{reply, {error, record_already_exist}, State};
		false ->
			{reply, {ok, Record}, [Record | State]}
	end;


handle_call({read, Key}, _From, State) ->
	case lists:keyfind(Key, 1, State) of
		false ->
			{reply, {error, record_is_doesnt_exist}, State};
		Record ->
			{reply, {ok, Record}, State}
	end;


handle_call({delete, Key}, _From, State) ->
	case lists:keyfind(Key, 1, State) of
		false ->
			{reply, {error, record_is_doesnt_exist}, State};
		Record ->
			{reply, ok, lists:delete(Record, State)}
	end;


handle_call({update, Record}, _From, State) ->
	{Key, _UserName, _City} = Record,
	case lists:keyfind(Key, 1, State) of
		false ->
			{reply, {error, record_is_doesnt_exist}, State};
		FindedRecord ->
			RemovedElementList = lists:delete(FindedRecord, State),
			{reply, {ok, Record}, [Record | RemovedElementList]}
	end;


handle_call(_Request, _From, State) ->
	{reply, {error, incorrect_request}, State}.


handle_cast(_Cast, State) -> 
	{noreply, State}.


terminate(_Reason, _State) ->
	ok.