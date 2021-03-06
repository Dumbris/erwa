%%
%% Copyright (c) 2014 Bas Wegh
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%

%% @private
-module(erwa_router).
-behaviour(gen_server).



-export([shutdown/1]).

-export([remove_session/2]).

-export([handle_wamp/2]).

%% API.
-export([start/1]).
-export([start_link/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).
-export([forward_messages/2]).



shutdown(Router) ->
  gen_server:cast(Router,shutdown).

remove_session(Router,Reason) ->
  gen_server:call(Router,{remove_session,Reason}).

handle_wamp(Router,Msg) ->
  gen_server:call(Router,{handle_wamp,Msg}).

start(Args) ->
  gen_server:start(?MODULE, [Args], []).

start_link(Args) ->
  gen_server:start_link(?MODULE, [Args], []).


-define(ROUTER_DETAILS,[
                        {agent,<<"Erwa-0.0.1">>},
                        {roles,[
                                {broker,[{features,[
                                                    {subscriber_blackwhite_listing,false},
                                                    {publisher_exclusion,true},
                                                    {publisher_identification,true},
                                                    {publication_trustlevels,false},
                                                    {pattern_based_subscription,false},
                                                    {partitioned_pubsub,false},
                                                    {subscriber_metaevents,false},
                                                    {subscriber_list,false},
                                                    {event_history,false}
                                                    ]} ]},
                                {dealer,[{features,[
                                                    {callee_blackwhite_listing,false},
                                                    {caller_exclusion,false},
                                                    {caller_identification,true},
                                                    {call_trustlevels,false},
                                                    {pattern_based_registration,false},
                                                    {partitioned_rpc,false},
                                                    {call_timeout,false},
                                                    {call_canceling,false},
                                                    {progressive_call_results,true}
                                                    ]}]}]}]).




-record(state, {
	realm = undefined,
  ets = undefined
}).

-record(session, {
  id = undefined,
  pid = undefined,
  monitor = undefined,
  details = undefined,
  requestId = 1,
  goodbye_sent = false,
  subscriber_blackwhite_listing,
  subscriptions = [],
  registrations = []
}).

%-record(challenge, {
%  authenticate = undefined,
%  session_id = undefined,
%  expires = never
%}).

-record(pid_session, {
  pid = undefined,
  session_id = undefined
}).

-record(monitor_session, {
  monitor = undefined,
  session_id = undefined
}).


-record(topic, {
  id = undefined,
  url = undefined,
  publishId = 1,
  subscribers = [],
  options = undefined
}).

-record(url_topic, {
  url = undefined,
  topic_id = undefined
  }).

%-record(subscription, {
%  id = undefined,
%  topicId = undefined,
%  options = undefined}).

-record(procedure, {
  id = undefined,
  url = undefined,
  options = undefined,
  session_id = undefined
}).

-record(url_procedure, {
  url = undefined,
  procedure_id = undefined
}).

-record(invocation, {
  id = undefined,
  callee_pid = undefined,
  request_id = undefined,
  caller_pid = undefined,
  progressive = false
}).

%% gen_server.

-define(TABLE_ACCESS,protected).

-spec init(Params :: list() ) -> {ok,#state{}}.
init([Realm]) ->
  Ets = ets:new(erwa_router,[?TABLE_ACCESS,set,{keypos,2}]),
  {ok,#state{realm=Realm,ets=Ets}}.


handle_call({handle_wamp,Msg},{Pid,_Ref},State) ->
  try
    ok = handle_wamp_message(Msg,Pid,State),
    {reply,ok,State}
  catch
    Error:Reason ->
       io:format("~nerror in router:~n~p~n",[erlang:get_stacktrace()]),
      {reply,{error,Error,Reason},State}
  end;
handle_call(_Msg,_From,State) ->
   {reply,shutdown,State}.

handle_cast(shutdown, State) ->
  {stop, normal, State};
handle_cast(_Request, State) ->
	{noreply, State}.

handle_info({'DOWN',Ref,process,_Pid,_Reason},State) ->
  remove_session_with_ref(Ref,State),
  {noreply,State};
handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.




-spec handle_wamp_message(Msg :: term(),Pid :: pid(), State :: #state{}) -> ok.
handle_wamp_message({hello,Realm,Details},Pid,#state{realm=Realm}=State) ->
  {ok,SessionId} = create_session(Pid,Details,State),
  send_message_to({welcome,SessionId,?ROUTER_DETAILS},Pid);
  %send_message_to({challenge,wampcra,[{challenge,JSON-Data}]},Pid);

handle_wamp_message({authenticate,_Signature,_Extra},Pid,_State) ->
  send_message_to({abort,[],not_authorized},Pid);

handle_wamp_message({goodbye,_Details,_Reason},Pid,#state{ets=Ets}=State) ->
  Session = get_session_from_pid(Pid,State),
  SessionId = Session#session.id,
  case Session#session.goodbye_sent of
    true ->
      ok;
    _ ->
      ets:update_element(Ets,SessionId,{#session.goodbye_sent,true}),
      send_message_to({goodbye,[],goodbye_and_out},Pid)
  end,
  send_message_to(shutdown,Pid);

handle_wamp_message({publish,_RequestId,Options,Topic,Arguments,ArgumentsKw},Pid,State) ->
  {ok,_PublicationId} = send_event_to_topic(Pid,Options,Topic,Arguments,ArgumentsKw,State),
  % TODO: send a reply if asked for ...
  ok;

handle_wamp_message({subscribe,RequestId,Options,Topic},Pid,State) ->
  {ok,TopicId} = subscribe_to_topic(Pid,Options,Topic,State),
  send_message_to({subscribed,RequestId,TopicId},Pid);

handle_wamp_message({unsubscribe,RequestId,SubscriptionId},Pid,State) ->
  case unsubscribe_from_topic(Pid,SubscriptionId,State) of
    true ->
      send_message_to({unsubscribed,RequestId},Pid);
    false ->
      send_message_to({error,unsubscribe,RequestId,[],no_such_subscription},Pid)
  end;

handle_wamp_message({call,RequestId,Options,Procedure,Arguments,ArgumentsKw},Pid,State) ->
  case enqueue_procedure_call( Pid, RequestId, Options,Procedure,Arguments,ArgumentsKw,State) of
    true ->
      ok;
    false ->
      send_message_to({error,call,RequestId,[],no_such_procedure,undefined,undefined},Pid)
  end;

handle_wamp_message({register,RequestId,Options,Procedure},Pid,State) ->
  case register_procedure(Pid,Options,Procedure,State) of
    {ok,RegistrationId} ->
      send_message_to({registered,RequestId,RegistrationId},Pid);
    {error,procedure_already_exists} ->
      send_message_to({register,error,RequestId,[],procedure_already_exists,undefined,undefined},Pid)
  end;

handle_wamp_message({unregister,RequestId,RegistrationId},Pid,State) ->
  case unregister_procedure(Pid,RegistrationId,State) of
    true ->
      send_message_to({unregistered,RequestId},Pid);
    false ->
      send_message_to({error,unregister,RequestId,[],no_such_registration,undefined,undefined},Pid)
  end;

handle_wamp_message({error,invocation,InvocationId,Details,Error,Arguments,ArgumentsKw},Pid,State) ->
  case dequeue_procedure_call(Pid,InvocationId,Details,Arguments,ArgumentsKw,Error,State) of
    {ok} -> ok;
    {error,not_found} -> ok;
    {error,wrong_session} -> ok
  end;
handle_wamp_message({yield,InvocationId,Options,Arguments,ArgumentsKw},Pid,State) ->
  case dequeue_procedure_call(Pid,InvocationId,Options,Arguments,ArgumentsKw,undefined,State) of
    {ok} -> ok;
    {error,not_found} -> ok;
    {error,wrong_session} -> ok
  end;
handle_wamp_message(Msg,_Pid,_State) ->
  io:format("unknown message ~p~n",[Msg]),
  ok.


-spec create_session(Pid :: pid(), Details :: list(), State :: #state{}) -> {ok,non_neg_integer()}.
create_session(Pid,Details,#state{ets=Ets}=State) ->
  Id = gen_id(),
  MonRef = monitor(process,Pid),
  case ets:insert_new(Ets,[#session{id=Id,pid=Pid,details=Details,monitor=MonRef},
                                #monitor_session{monitor=MonRef,session_id=Id},
                                #pid_session{pid=Pid,session_id=Id}]) of
    true ->
      {ok,Id};
    _ ->
      demonitor(MonRef),
      create_session(Pid,Details,State)
  end.

-spec send_event_to_topic(FromPid :: pid(), Options :: list(), Url :: binary(), Arguments :: list()|undefined, ArgumentsKw :: list()|undefined, State :: #state{} ) -> {ok,non_neg_integer()}.
send_event_to_topic(FromPid,Options,Url,Arguments,ArgumentsKw,#state{ets=Ets}=State) ->
  PublicationId =
    case ets:lookup(Ets,Url) of
      [] ->
        gen_id();
      [UrlTopic] ->
        TopicId = UrlTopic#url_topic.topic_id,
        [Topic] = ets:lookup(Ets,TopicId),
        IdToPid = fun(Id,Pids) -> [#session{pid=Pid}] = ets:lookup(Ets,Id), [Pid|Pids] end,
        Session = get_session_from_pid(FromPid,State),
        Peers =
          case lists:keyfind(exclude_me,1,Options) of
            {exclude_me,false} ->
                lists:foldl(IdToPid,[],Topic#topic.subscribers);
            _ -> lists:delete(FromPid,lists:foldl(IdToPid,[],Topic#topic.subscribers))
          end,
        SubscriptionId = Topic#topic.id,
        PublishId = gen_id(),
        Details1 =
          case lists:keyfind(disclose_me,1,Options) of
            {disclose_me,true} -> [{publisher,Session#session.id}];
            _ -> []
          end,
        Message = {event,SubscriptionId,PublishId,Details1,Arguments,ArgumentsKw},
        send_message_to(Message,Peers),
        PublishId
    end,
  {ok,PublicationId}.

-spec subscribe_to_topic(Pid :: pid(), Options :: list(), Url :: binary(), State :: #state{}) -> {ok, non_neg_integer()}.
subscribe_to_topic(Pid,Options,Url,#state{ets=Ets}=State) ->
  Session = get_session_from_pid(Pid,State),
  SessionId = Session#session.id,
  Subs = Session#session.subscriptions,
  Topic =
    case ets:lookup(Ets,Url) of
      [] ->
        % create the topic ...
        {ok,T} = create_topic(Url,Options,State),
        T;
      [UrlTopic] ->
        Id = UrlTopic#url_topic.topic_id,
        [T] = ets:lookup(Ets,Id),
        T
    end,
  #topic{id=TopicId,subscribers=Subscribers} = Topic,
  ets:update_element(Ets,TopicId,{#topic.subscribers,[SessionId|lists:delete(SessionId,Subscribers)]}),
  ets:update_element(Ets,SessionId,{#session.subscriptions,[TopicId|lists:delete(TopicId,Subs)]}),
  {ok,TopicId}.


-spec create_topic(Url :: binary(), Options :: list, State :: #state{}) -> {ok,#topic{}}.
create_topic(Url,Options,#state{ets=Ets}=State) ->
  Id = gen_id(),
  T = #topic{id=Id,url=Url,options=Options},
  Topic =
    case ets:insert_new(Ets,T) of
      true ->
        true = ets:insert_new(Ets,#url_topic{url=Url,topic_id=Id}),
        T;
      false -> create_topic(Url,Options,State)
    end,
  {ok,Topic}.


-spec unsubscribe_from_topic(Pid :: pid(), SubscriptionId :: non_neg_integer(), State :: #state{}) -> true | false.
unsubscribe_from_topic(Pid,SubscriptionId,State) ->
  Session = get_session_from_pid(Pid,State),
  case lists:member(SubscriptionId,Session#session.subscriptions) of
    false ->
      false;

    true ->
      ok = remove_session_from_topic(Session,SubscriptionId,State),
      true
  end.





-spec register_procedure(Pid :: pid(), Options :: list(), ProcedureUrl :: binary(), State :: #state{}) -> {ok,non_neg_integer()} | {error,procedure_already_exists }.
register_procedure(Pid,Options,ProcedureUrl,#state{ets=Ets}=State) ->
  Session = get_session_from_pid(Pid,State),

  case ets:lookup(Ets,ProcedureUrl) of
    [] ->
      create_procedure(ProcedureUrl,Options,Session,State);
    _ ->
      {error,procedure_already_exists}
  end.


-spec unregister_procedure( Pid :: pid(), ProcedureId :: non_neg_integer(), State :: #state{}) -> true | false.
unregister_procedure(Pid,ProcedureId,State) ->
  Session = get_session_from_pid(Pid,State),
  case lists:member(ProcedureId,Session#session.registrations) of
    true ->
      ok = remove_session_from_procedure(Session,ProcedureId,State),
      true;
    false ->
      false
  end.


enqueue_procedure_call( Pid, RequestId, Options,ProcedureUrl,Arguments,ArgumentsKw,#state{ets=Ets}=State) ->
  Session = get_session_from_pid(Pid,State),
  CallerId = Session#session.id,

  case ets:lookup(Ets,ProcedureUrl) of
    [] ->
      false;
    [#url_procedure{url=ProcedureUrl,procedure_id=ProcId}] ->
      [Procedure] = ets:lookup(Ets,ProcId),
      ProcedureId = Procedure#procedure.id,
      CalleeId = Procedure#procedure.session_id,
      [CalleeSession] = ets:lookup(Ets,CalleeId),
      CalleePid = CalleeSession#session.pid,

      Details1 =
        case lists:keyfind(disclose_me,1,Options) of
          {disclose_me,true} -> [{caller,CallerId}];
          _ -> []
        end,

      Details2 =
        case lists:keyfind(receive_progress,1,Options) of
          {receive_progress,true} -> [{receive_progress,true}|Details1];
          _ -> Details1
        end,

      {ok,InvocationId} = create_invocation(Pid,Session,CalleeSession,RequestId,Procedure,Options,Arguments,ArgumentsKw,State),
      send_message_to({invocation,InvocationId,ProcedureId,Details2,Arguments,ArgumentsKw},CalleePid),
      true
  end.


dequeue_procedure_call(Pid,Id,InOptions,Arguments,ArgumentsKw,Error,#state{ets=Ets}=State) ->
  case ets:lookup(Ets,Id) of
    [] -> {error,not_found};
    [Invocation] ->
      case Invocation#invocation.callee_pid of
       Pid ->
          #invocation{caller_pid=CallerPid, request_id=RequestId, progressive=Progressive} = Invocation,

         Progress
           = case lists:keyfind(progress,1,InOptions) of
               {progress, true} -> true;
               _ -> false
            end,

          BoolError
             = case Error of
                 undefined -> false;
                 _ -> true
               end,

          case {Progressive,Progress,BoolError} of
            {_,_,true} -> remove_invocation(Id,State);
            {false,_,_} -> remove_invocation(Id,State);
            {true,false,_} -> remove_invocation(Id,State);
            _ -> ok
          end,
         OutDetails =
           case {Progress,BoolError} of
             {true,false} -> [{progress,true}];
             _ -> []
           end,
         Msg = case Error of
                  undefined ->
                    {result,RequestId,OutDetails,Arguments,ArgumentsKw};
                  Uri ->
                    {error,call,RequestId,OutDetails,Uri,Arguments,ArgumentsKw}
                end,

         send_message_to(Msg,CallerPid),
          {ok};
        _ ->
          {error,wrong_session}
      end
  end.



-spec remove_session_with_ref(MonRef :: reference(), State :: #state{}) -> ok.
remove_session_with_ref(MonRef,#state{ets=Ets}=State) ->
  case ets:lookup(Ets,MonRef) of
    [RefSession] ->
      Id = RefSession#monitor_session.session_id,
      [Session] = ets:lookup(Ets,Id),
      remove_given_session(Session,State);
    [] ->
      ok
  end.


-spec remove_given_session(Session :: #session{}, State :: #state{}) -> ok.
remove_given_session(Session,#state{ets=Ets}=State) ->
  Id = Session#session.id,
  MonRef = Session#session.monitor,
  RemoveTopic = fun(TopicId,Results) ->
        Result = remove_session_from_topic(Session,TopicId,State),
        [{TopicId,Result}|Results]
        end,
  _ResultTopics = lists:foldl(RemoveTopic,[],Session#session.subscriptions),

  RemoveRegistration = fun(RegistrationId,Results) ->
        Result = remove_session_from_procedure(Session,RegistrationId,State),
        [{RegistrationId,Result}|Results]
        end,
  _ResultRegistrations = lists:foldl(RemoveRegistration,[],Session#session.registrations),

  ets:delete(Ets,Id),
  ets:delete(Ets,MonRef),
  ets:delete(Ets,Session#session.pid),
  ok.


-spec remove_session_from_topic(Session :: #session{}, TopicId :: non_neg_integer(), State :: #state{}) -> ok | not_found.
remove_session_from_topic(Session,TopicId,#state{ets=Ets}) ->
  SessionId = Session#session.id,
  [Topic] = ets:lookup(Ets,TopicId),
  ets:update_element(Ets,TopicId,{#topic.subscribers,lists:delete(SessionId,Topic#topic.subscribers)}),
  ets:update_element(Ets,SessionId,{#session.subscriptions,lists:delete(TopicId,Session#session.subscriptions)}),
  ok.


-spec create_procedure(Url :: binary(), Options :: list(), Session :: #session{}, State :: #state{} ) -> {ok,non_neg_integer()}.
create_procedure(Url,Options,Session,#state{ets=Ets}=State) ->
  SessionId = Session#session.id,
  ProcedureId = gen_id(),
  case ets:insert_new(Ets,#procedure{id=ProcedureId,url=Url,session_id=SessionId,options=Options}) of
    true ->
      true = ets:insert_new(Ets,#url_procedure{url=Url,procedure_id=ProcedureId}),
      true = ets:update_element(Ets,SessionId,{#session.registrations, [ProcedureId| Session#session.registrations]}),
      {ok,ProcedureId};
    _ ->
      create_procedure(Url,Options,Session,State)
  end.

-spec remove_session_from_procedure( Session :: #session{}, ProcedureId :: non_neg_integer(), State :: #state{}) -> ok | not_found.
remove_session_from_procedure(Session,ProcedureId,#state{ets=Ets}) ->
  SessionId = Session#session.id,
  [Procedure] = ets:lookup(Ets,ProcedureId),
  ets:delete(Ets,ProcedureId),
  ets:delete(Ets,Procedure#procedure.url),
  ets:update_element(Ets,SessionId,{#session.registrations,lists:delete(ProcedureId,Session#session.registrations)}),
  ok.


-spec create_invocation(Pid :: pid(), Session :: #session{}, CalleeSession :: #session{}, RequestId :: non_neg_integer(), Procedure :: #procedure{}, Options :: list(), Arguments :: list(), ArgumentsKw :: list(), State :: #state{}) -> {ok, non_neg_integer()}.
create_invocation(Pid,Session,CalleeSession,RequestId,Procedure,Options,Arguments,ArgumentsKw,#state{ets=Ets}=State) ->
  Id = gen_id(),

  Progressive =
    case lists:keyfind(receive_progress,1,Options) of
      {receive_progress,true} -> true;
      _ -> false
    end,

  Invocation = #invocation{
        id = Id,
        request_id = RequestId,
        caller_pid = Pid,
        callee_pid = CalleeSession#session.pid,
        progressive = Progressive },
  case ets:insert_new(Ets,Invocation) of
    true ->
      {ok,Id};
    false ->
      create_invocation(Pid,Session,CalleeSession,RequestId,Procedure,Options,Arguments,ArgumentsKw,State)
  end.

-spec remove_invocation(Id :: non_neg_integer(), State :: #state{}) -> ok.
remove_invocation(InvocationId,#state{ets=Ets}) ->
  true = ets:delete(Ets,InvocationId),
  ok.


-spec send_message_to(Msg :: term(), Peer :: list() |  pid()) -> ok.
send_message_to(Msg,Pid) when is_pid(Pid) ->
  send_message_to(Msg,[Pid]);
send_message_to(Msg,Peers) when is_list(Peers) ->
  Send = fun(Pid) ->
           Pid ! {erwa,Msg} end,
  lists:foreach(Send,Peers),
  ok.


-spec get_session_from_pid(Pid :: pid(), State :: #state{}) -> #session{}|undefined.
get_session_from_pid(Pid,#state{ets=Ets}) ->
  case ets:lookup(Ets,Pid) of
    [PidSession] ->
        case ets:lookup(Ets,PidSession#pid_session.session_id) of
          [Session] -> Session;
          [] -> undefined
        end;
      [] -> undefined
  end.


-spec gen_id() -> non_neg_integer().
gen_id() ->
  crypto:rand_uniform(0,9007199254740992).

-spec forward_messages(Messages :: list(), Router :: pid() | undefined) -> {ok,Router :: pid() | undefined} | {error,not_found}.
forward_messages([],Router) ->
  {ok,Router};
forward_messages([{hello,Realm,_}|_]=Messages,undefined) ->
  case erwa_realms:get_router(Realm) of
    {ok,Pid} ->
      forward_messages(Messages,Pid);
    {error,not_found} ->
      self() ! {erwa,{abort,[{}],no_such_realm}},
      self() ! {erwa, shutdown},
      {error,undefined}
  end;
forward_messages([Msg|T],Router) when is_pid(Router) ->
  ok = erwa_router:handle_wamp(Router,Msg),
  forward_messages(T,Router);
forward_messages(_,undefined)  ->
  self() ! {erwa,{abort,[{}],no_such_realm}},
  self() ! {erwa, shutdown},
  {error,undefined}.






-ifdef(TEST).

hello_test() ->
  Realm = <<"realm1">>,
  {ok,Pid} = start(Realm),
  ok = handle_wamp(Pid,{hello,Realm,[]}),
  {ok,Msg} =
    receive
      {erwa,M} -> {ok,M}
    after 2000 ->
      {ok,timeout}
    end,
  {welcome,_SessionId,_Details} = Msg,
  shutdown(Pid),
  ok.


subscribe_test() ->
  Realm = <<"realm2">>,
  {ok,Pid} = start(Realm),
  ok = handle_wamp(Pid,{hello,Realm,[]}),
  ok = receive
    {erwa,{welcome, _SessionId, _Details }} -> ok
  after 2000 ->
    timeout
  end,

  ok = handle_wamp(Pid,{subscribe,1,[],<<"test">>}),
  {ok,SubscriptionId} = receive
    {erwa,{subscribed, 1, SID }} -> {ok,SID}
  after 2000 ->
    timeout
  end,

  ok = handle_wamp(Pid,{unsubscribe,2,SubscriptionId}),
  ok = receive
    {erwa,{unsubscribed, 2 }} -> ok
  after 2000 ->
    timeout
  end,
  shutdown(Pid),
  ok.

-endif.

