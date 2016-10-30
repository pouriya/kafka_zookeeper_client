%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% BSD 3-Clause License
%%% Copyright (c) 2016-2017, Pouriya Jahanbakhsh pouriya.jahanbakhsh@gmail.com
%%% All rights reserved.
%%%
%%% Redistribution and use in source and binary forms, with or without
%%% modification, are permitted provided that the following conditions are met:
%%%
%%% 1. Redistributions of source code must retain the above copyright notice,
%%%    this list of conditions and the following disclaimer.
%%%
%%% 2. Redistributions in binary form must reproduce the above copyright notice,
%%%    this list of conditions and the following disclaimer in the documentation
%%%    and/or other materials provided with the distribution.
%%%
%%% 3. Neither the name of the copyright holder nor the names of its
%%%    contributors may be used to endorse or promote products derived from this
%%%    software without specific prior written permission.
%%%
%%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
%%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
%%% THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
%%% PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
%%% CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
%%% EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
%%% PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
%%% OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
%%% WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
%%% OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
%%% ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @author  Pouriya Jahanbakhsh
%%% @author  <pouriya.jahanbakhsh@gmail.com>
%%% @version 0.1
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



-module(kafka_zookeeper_client).
-author('pouriya.jahanbakhsh@gmail.com').
-vsn(0.1).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Export:


%% API
-export([get_broker_address/3
        ,get_broker_address/4]).





%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Records & Macros & Includes:


-define(debug(DBG, Type, Text, Args)
,sys:handle_debug(DBG, fun debug/3, {Type, Text}, Args)).

-define(CREATE_ID_TYPE, ephemeral_sequential).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types:






-type   proplist() :: [member()].
-type       member() :: {atom(), any()}.

-type	erlzk_args() :: {zookeeper_addresses(), erlzk_timeout(), erlzk_opts()}.
-type		zookeeper_addresses() :: [zookeeper_address()].
-type			zookeeper_address() :: {host(), port()}.
-type				host() :: string().
-type				port() :: integer().
-type		erlzk_timeout() :: integer().
-type		erlzk_opts() :: proplist(). % see 'erlzk' module

-type	brokers_directory() :: string().
-type	brokers_id_directory() :: string().

-type	broker_address() :: {address, {host(), port()}}.

-type   debug() :: debug_opts() | {debug, list()}. % see debug in 'sys' module
-type   debug_opts() :: {debug_opts(), list()}. % see debug_opts in 'sys' module

-type   connection() :: pid().

-type   error_params() :: proplist().

-type   children() :: list().
-type   broker() :: string().
-type   broker_detail() :: binary(). % json





%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API:


-spec   get_broker_address(erlzk_args()
                          ,brokers_directory()
                          ,brokers_id_directory()) ->
	broker_address().

get_broker_address({_Addresses, _TimeOut, _Opts}=ErlzkArgs
                  ,BrokersDir
                  ,BrokersIdDir) ->
	Debug = {debug_options, [trace]},
	get_broker_address(ErlzkArgs, BrokersDir, BrokersIdDir, Debug).







-spec   get_broker_address(erlzk_args()
                          ,brokers_directory()
                          ,brokers_id_directory()
                          ,debug()) ->
	broker_address().

get_broker_address({Addresses, TimeOut, Opts}
                  ,BrokersDir
                  ,BrokersIdDir
                  ,Debug0) ->
	application:load(erlzk),
	ok = application:ensure_started(jsone),
	{ok, Sup} = erlzk_sup:start_link(),
	Debug = case Debug0 of
		        {debug_options, DebugOpts} ->
			        sys:debug_options(DebugOpts);
		        {debug, Debug2} ->
			        Debug2
	        end,
	{connection, Pid} = connect(Addresses, TimeOut, Opts, Debug),
	{children, Children} = get_children(Pid, BrokersDir, Debug),
	{broker, Broker} = select_broker(Pid, Children, BrokersIdDir, Debug),
	{broker_detail, BrokerDetail} = get_broker_detail(Pid
	                                                 ,BrokersDir
	                                                 ,Broker
	                                                 ,Debug),
	{address, _Address}=Address = get_address(BrokerDetail),
	unlink(Sup),
	exit(Sup, shutdown),
	Address.





%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal Calls:





-spec   connect(zookeeper_addresses(), timeout(), erlzk_opts(), debug()) ->
	{connection, connection()} |
	{error, {'NoConnection', error_params()}}.

connect([Address|Addresses], TimeOut, Options, Debug) ->
	{ok, Pid} = erlzk:connect([Address]
	                         ,TimeOut
	                         ,Options ++ [{monitor, self()}]),
	receive
		{connected, Host, Port} ->
			?debug(Debug
			      ,info
			      ,"connected to zookeeper server"
			      ,[{host, Host}, {port, Port}]),
			{connection, Pid}
	after TimeOut ->
		?debug(Debug
		      ,warning
		      ,"could not connect to given address"
		      ,[{address, Address}]),
		connect(Addresses, TimeOut, Options, Debug)
	end;

connect([], _Timeout, _Options, Debug) ->
	?debug(Debug, error, "could not connect to any addresses", []),
	exit({error, {'NoConnection', []}}).







-spec	get_children(connection(), brokers_directory(), debug()) ->
	{children, children()} |
	{error, {'CouldNotGetchildren', error_params()}} |
	{error, {'BrokerDirNotFound', error_params()}}.

get_children(Pid, BrokerDir, Debug) ->
	case erlzk:exists(Pid, BrokerDir) of
		{ok, _} ->
			case erlzk:get_children(Pid, BrokerDir) of
				{ok, Children} ->
					?debug(Debug
					      ,info
					      ,"brokers found"
					      ,[{total, length(Children)}]),
					{children, Children};
				Error ->
					Params = [{broker_directory, BrokerDir}, {error, Error}],
					?debug(Debug
					      ,error
					      ,"could not get children"
					      ,Params),
					{error, {'CouldNotGetchildren', Params}}
			end;
		Error ->
			Params = [{broker_directory, BrokerDir}, {error, Error}],
			?debug(Debug
			      ,error
			      ,"broker directory not found"
			      ,Params),
			{error, {'BrokerDirNotFound', Params}}
	end.







-spec   select_broker(connection()
                     ,children()
                     ,brokers_id_directory()
                     ,debug()) ->
	{broker, broker()} |
	{error, {'NoBroker', error_params()}}.

select_broker(Pid, Children, BrokerIdDir, Debug) when length(Children) > 0 ->
	case erlzk:exists(Pid, BrokerIdDir) of
		{ok, _} ->
			{ok, ESID} = erlzk:create(Pid, BrokerIdDir, ?CREATE_ID_TYPE),
			?debug(Debug
			      ,info
			      ,"\'ephemeral_sequetional\' id was created"
			      ,[{?CREATE_ID_TYPE, ESID}]),
			Integer       = list_to_integer(ESID -- BrokerIdDir),
			KafkaServerIndex = Integer rem length(Children),
			?debug(Debug
			      ,info
			      ,"broker selected"
			      ,[{index, KafkaServerIndex}]),
			{broker, lists:nth(KafkaServerIndex+1, Children)};
		_ ->
			?debug(Debug
			      ,warning
			      ,"broker id directory not found"
			      ,[{broker_id_directory, BrokerIdDir}]),
			ok = create_dir(Pid, BrokerIdDir, Debug),
			select_broker(Pid, Children, BrokerIdDir, Debug)
	end;
select_broker(_Pid, Children, _BrokerIdDir, Debug) ->
	ErrParams = [{brokers, Children}],
	?debug(Debug
	      ,error
	      ,"broker(s) not found"
	      ,ErrParams),
	{error, {'NoBroker', ErrParams}}.







-spec   create_dir(connection(), brokers_id_directory(), debug()) ->
	ok |
	{error, {'CouldNotCreate', error_params()}}.

create_dir(Pid, Dir, Debug) ->
	SepDir = string:tokens(Dir, "/"),
	Created = "",
	create_dir(Pid, SepDir, Created, Debug).

create_dir(Pid, [Root0|Rest], Created, Debug) ->
	Root = Created ++ "/" ++ Root0,
	case erlzk:create(Pid, Root) of
		{ok, _} ->
			create_dir(Pid, Rest, Root, Debug);
		{error,node_exists} ->
			create_dir(Pid, Rest, Root, Debug);
		Error ->
			ErrParams = [{error, Error}],
			?debug(Debug
			      ,error
			      ,"could not create new directory"
			      ,ErrParams),
			{error, {'CouldNotCreate', ErrParams}}
	end;
create_dir(_Pid, [], Created, Debug) ->
	?debug(Debug
	      ,info
	      ,"new dricetory created"
	      ,[{directory, Created}]),
	ok.







-spec   get_broker_detail(connection()
                         ,brokers_directory()
                         ,broker()
                         ,debug()) ->
	{brokers_detail, broker_detail()} |
	{error, {'NoBrokerData', error_params()}}.

get_broker_detail(Pid, BrokerDir, Broker, Debug) ->
	case erlzk:get_data(Pid, BrokerDir ++ "/" ++ Broker) of
		{ok, {BrokerDetail, _}} ->
			{broker_detail, BrokerDetail};
		Error ->
			Params = [{broker, Broker}, {error, Error}],
			?debug(Debug
			      ,error
			      ,"could not get broker data"
			      ,Params),
			{error, {'NoBrokerData', Params}}
	end.







-spec   get_address(broker_detail()) ->
	broker_address() |
	{error, {'BadData', error_params()}}.

get_address(BrokerDetail) when is_binary(BrokerDetail) ->
	case catch jsone:decode(BrokerDetail, [{object_format, tuple}]) of
		{Data} ->
			{<<"host">>, BHost} = proplists:lookup(<<"host">>,Data),
			{<<"port">>, Port} = proplists:lookup(<<"port">>, Data),
			Host = binary_to_list(BHost),
			{address, {Host, Port}};
		Error ->
			Error
	end.







-spec   debug(pid(), {atom(), string()}, proplist()) ->
	ok.

debug(IO, Params, {Type, Text}) ->
	io:format(IO, "**DEBUG** ~s~n", [get_time()]),
	io:format(IO, Type, []),
	io:format(IO, "\t~n~s~n", [Text]),
	[io:format(IO, "\t~p:~p~n", [Key,Value]) || {Key, Value} <- Params],
	io:format(IO, "~n", []),
	ok.







-spec   get_time() ->
	string().

get_time() ->
	{{Y, Mo, D}, {H, M, S}} = calendar:local_time(),
	F = fun integer_to_list/1,
	lists:concat([F(Y), "/", F(Mo), "/", F(D), " "
	             ,F(H), ":", F(M), ":", F(S)]).