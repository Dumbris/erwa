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

-module(erwa_routing).

-export([initialize/0]).
-export([start_realm/1]).
-export([stop_realm/1]).


-export([create_state/0]).
-export([handle_messages/2]).


-define(ROUTER_DETAILS,[
                        {agent,<<"Erwa-0.0.1">>},
                        {roles,[
                                {broker,[{features,[
                                                    {subscriber_blackwhite_listing,false},
                                                    {publisher_exclusion,false},
                                                    {publisher_identification,false},
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
                                                    {caller_identification,false},
                                                    {call_trustlevels,false},
                                                    {pattern_based_registration,false},
                                                    {partitioned_rpc,false},
                                                    {call_timeout,false},
                                                    {call_canceling,false},
                                                    {progressive_call_results,false}
                                                    ]}]}]}]).




-record(erwa_realm, {
  name = undefined,
  accept_new = true,
  pids = []
}).

-record(erwa_sess,{
  id=undefined,
  pid=undefined,

  realm=undefined,
  details=[]
}).

-record(erwa_topic, {
  uri = undefined,

  subscribers = []
}).

%-record(erwa_pub ,{
%  id = undefined,
%  uri = undefined
%}).

-record(erwa_exact_sub, {
  id = undefined,
  uri = undefined,

  subscribers = []
}).




-record(erwa_proc, {
  id = undefined,
  uri = undefined,

  options = undefined,
  session_id = undefined
}).

-record(erwa_invoc, {
  id = undefined,
  callee_pid = undefined,
  request_id = undefined,
  caller_pid = undefined,
  progressive = false
}).



-record(state,{
  sess_id = undefined,
  goodbye_sent = false,
  details = [],
  subs = []
}).


initialize() ->
  case is_fresh_startup() of
    true -> init_db();
    {exists, Tbls} ->
       ok = mnesia:wait_for_tables(Tbls, 1000000)
  end.

-spec start_realm(Name :: binary()) -> ok.
start_realm(Name) ->
  T = fun() ->
        ok = mnesia:write(#erwa_realm{name=Name})
      end,
  {atomic, Result} = mnesia:transaction(T),
  Result.

stop_realm(Name) ->
  %% @todo Need to add a timer to forcefully shut down all existing connections
  %% @todo update state of sessions that goodbye is sent
  T = fun() ->
        case mnesia:read(erwa_realm,Name) of
          [Realm] ->
            NewRealm = Realm#erwa_realm{accept_new = false},
            ok = mnesia:write(NewRealm),
            Realm#erwa_realm.pids;
          _ ->
            []
        end
      end,
  {atomic,Pids} = mnesia:transaction(T),
  send_message_to({goodbye,[],close_realm},Pids).



-spec create_state() -> #state{}.
create_state() ->
  #state{}.

-spec handle_messages([term()],#state{}) -> {[any()],#state{}}.
handle_messages(Messages,State) ->
  handle_messages(Messages,[],State).

-spec handle_messages([term()],[any()],#state{}) -> {[any()],#state{}}.
handle_messages([],Outgoing,State) ->
  {lists:reverse(Outgoing),State};
handle_messages([Message|Tail],Outgoing,State) ->
  {Reply,NewState} = handle_wamp_message(Message,State),
  NewOutgoing =
    case {Reply,is_list(Reply)} of
      {noreply,_} -> Outgoing;
      {RL,true} -> lists:reverse(RL) ++ Outgoing;
      {R,_} -> [R|Outgoing]
    end,
  handle_messages(Tail,NewOutgoing,NewState).


-spec handle_wamp_message(Msg :: term(), #state{}) -> {term() | noreply | [any()], #state{}}.
handle_wamp_message({hello,RealmName,Details},#state{sess_id=undefined}=State) ->
  %% handle the incomming "hello" message
  %% this should be the first message to come index
  %% @todo implement a way to chek if authentication is needed
  %send_message_to({challenge,wampcra,[{challenge,JSON-Data}]},self());
  T_Realm = fun() ->
              case mnesia:read(erwa_realm,RealmName) of
                [RealmData] -> RealmData;
                [] -> undefined
              end
            end,
  {atomic,Realm} = mnesia:transaction(T_Realm),
  RealmAccepting =
    case Realm of
      undefined -> false;
      R -> R#erwa_realm.accept_new
    end,

  %% @todo validate the peer details
  %NewState = validate_peer_details(Details);

  case RealmAccepting of
    true ->
      {ok,SessionId} = create_session(Realm,Details),
      ok = add_this_to_realm(Realm),
      NewState = State#state{sess_id=SessionId},
      {{welcome,SessionId,?ROUTER_DETAILS},NewState};
    _ ->
      {[{abort,[],no_such_realm},shutdown],State}
  end;
handle_wamp_message({hello,_RealmName,_Details},State) ->
  % if the hello message is sent twice close the connection
  {shutdown,State};

handle_wamp_message({authenticate,_Signature,_Extra},State) ->
  {[{abort,[],no_such_realm},shutdown],State};

handle_wamp_message({goodbye,_Details,_Reason},#state{goodbye_sent=GB_Sent}=State) ->
  case GB_Sent of
    true ->
      {shutdown,State};
    _ ->
      {[{goodbye,[],goodbye_and_out},shutdown],State#state{goodbye_sent=true}}
  end;

handle_wamp_message({subscribe,RequestId,Options,SubscriptionURI},#state{subs=Subscriptions}=State) ->
  % there are three different kinds of subscription
  % - basic subscription eg com.example.uri
  % - pattern_based_subscription, using the details match "prefix" and "wildcard"
  % - partitioned ones using nkey and rkey ... not yet understood

  case proplists:get_value(match,Options,exact) of
    exact ->
      % a basic subscription
      case lists:keyfind(SubscriptionURI,2,Subscriptions) of
        {SubscriptionId,SubscriptionURI} ->
          %% @todo check if it is an error to re-subscribe
          %% if it is an error, what is the message?
          {{subscribed,RequestId,SubscriptionId},State};
        false ->
          {ok,SubscriptionId,NewState} = add_exact_subscription(SubscriptionURI,State),
          {{subscribed,RequestId,TopicId},NewState}
      end;

    %% @todo think of a way to implement pattern based subscriptions
    %prefix ->
      % a prefix subscription


    %wildcard ->
      % a wildcard subscription

    _ ->
      % unsupported match
      {{error,subscribe,RequestId,[],invalid_argument},State}
  end;



handle_wamp_message({unsubscribe,RequestId,SubscriptionId},#state{topics=Topics} = State) ->
  case lists:keyfind(SubscriptionId,1,Topics) of
    {SubscriptionId,_TopicUri} ->
      {ok,NewState} = unsubscribe_from_topic(SubscriptionId,State),
      {{unsubscribed,RequestId},NewState};
    false ->
      {{error,unsubscribe,RequestId,[],no_such_subscription},State}
  end;


handle_wamp_message({publish,_RequestId,Options,Topic,Arguments,ArgumentsKw},State) ->
  {ok,PublicationId} = send_event_to_topic(Options,Topic,Arguments,ArgumentsKw,State),
  % TODO: send a reply if asked for ...
  ok;

%handle_wamp_message({call,RequestId,Options,Procedure,Arguments,ArgumentsKw}) ->
  %case enqueue_procedure_call( Pid, RequestId, Options,Procedure,Arguments,ArgumentsKw,State) of
%    true ->
%      ok;
%    false ->
%      send_message_to({error,call,RequestId,[],no_such_procedure,undefined,undefined},Pid)
%  end;

%handle_wamp_message({register,RequestId,Options,Procedure}) ->
  %case register_procedure(Pid,Options,Procedure,State) of
%    {ok,RegistrationId} ->
%      send_message_to({registered,RequestId,RegistrationId},Pid);
%    {error,procedure_already_exists} ->
%      send_message_to({register,error,RequestId,[],procedure_already_exists,undefined,undefined},Pid)
%  end;

%handle_wamp_message({unregister,RequestId,RegistrationId}) ->
  %case unregister_procedure(Pid,RegistrationId,State) of
%    true ->
%      send_message_to({unregistered,RequestId},Pid);
%    false ->
%      send_message_to({error,unregister,RequestId,[],no_such_registration,undefined,undefined},Pid)
%  end;


%handle_wamp_message({error,invocation,InvocationId,Details,Error,Arguments,ArgumentsKw}) ->
  %case dequeue_procedure_call(Pid,InvocationId,Details,Arguments,ArgumentsKw,Error,State) of
%    {ok} -> ok;
%    {error,not_found} -> ok;
%    {error,wrong_session} -> ok
%  end;


% handle_wamp_message({yield,InvocationId,Options,Arguments,ArgumentsKw}) ->
%  case dequeue_procedure_call(Pid,InvocationId,Options,Arguments,ArgumentsKw,undefined,State) of
%    {ok} -> ok;
%    {error,not_found} -> ok;
%    {error,wrong_session} -> ok
%  end;


handle_wamp_message(Msg,State) ->
  io:format("unknown message ~p~n",[Msg]),
  {shutdown,State}.


create_session(Realm,Details) ->
  Id = gen_id(),
  T = fun() ->
        case mnesia:read(erwa_sess,Id) of
          [Found] ->
            false;
          [] ->
            ok = mnesia:write(#erwa_sess{id=id,realm=Realm,details=Details,pid=self()}),
            true
        end
      end,
  {atomic,Done} =mnesia:transaction(T).
  case Done of
    true ->
      {ok,Id};
    false ->
      create_session(Realm,Details)
  end.

add_this_to_realm(Name) ->
  T = fun() ->
        case mnesia:match_object(#erwa_realm{name=Name,_='_'}) of
          [Realm] ->
            NewRealm = Realm#erwa_realm{pids=[self()|Realm#erwa_realm.pids]},
            ok = mnesia:write(NewRealm);
          [] ->
            ok
        end
      end,
  {atomic,ok} =mnesia:transaction(T).

maybe_create_topic(TopicUri) ->
  T = fun() ->
        case mnesia:match_object(#erwa_topic{uri=TopicUri,_='_'}) of
          [_Topic] -> true;
          [] -> false
        end
      end,
  {atomic,Exists} = mnesia:transaction(T),
  case Exists of
    false -> create_topic(TopicUri);
    true -> ok
  end.

create_topic(TopicUri) ->
  Id = gen_id(),
  T = fun() ->
        case mnesia:match_object(#erwa_topic{id=Id,_='_'}) of
          [_Topic] ->
            false;
          [] ->
            ok = mnesia:write(#erwa_topic{id=Id,uri=TopicUri}),
            true
        end
      end,
  {atomic,Created} = mnesia:transaction(T),
  case Created of
    true -> ok;
    false -> create_topic(TopicUri)
  end.




%% subscribe a new session to an existing topic
-spec add_to_subscription(SubscriptionUri :: binary() ,State :: #state{}) -> {ok,non_neg_integer(),#state{}}.
add_exact_subscription(SubscriptionURI,#state{sess_id=SessionId, subs=Subscriptions} = State) ->
  T = fun() ->
        [Subscription] = mnesia:match_object(erwa_exact_sub,#erwa_exact_sub{uri=SubscriptionURI,_='_'},write),
        #erwa_exact_sub{id=S_Id,uri=Subscriptions}=Subscription,
        Subs = [SessionId|Subscription#erwa_exact_sub.subscribers],
        ok = mnesia:write(Subscription#erwa_exact_sub{subscribers=Subs}),
        S_Id
      end,
  {atomic,SubscriptionId} = mnesia:transaction(T),
  {ok,TopicId,State#state{subs=[{SubscriptionId,SubscriptionURI}|Subscriptions]}}.

-spec remove_from_subscription(TopicId :: non_neg_integer() ,State :: #state{}) -> {ok,#state{}}.
remove_from_subscription(TopicId,#state{sess_id=SessionId, topics=Topics} = State) ->
  T =
    fun() ->
      [Topic] = mnesia:match_object(topic,#erwa_topic{id=TopicId,_='_'},write),
      #erwa_topic{id=TopicId,uri=Uri}=Topic,
      Subs = lists:delete(SessionId,Topic#erwa_topic.subscribers),
      ok = mnesia:write(Topic#erwa_topic{subscribers=Subs}),
      Uri
    end,
  {atomic,TopicUri} = mnesia:transaction(T),
  {ok,State#state{topics=lists:delete({TopicId,TopicUri},Topics)}}.



send_event_to_topic(Options,TopicUri,Arguments,ArgumentsKw,State) ->
  ok = maybe_create_topic(TopicUri),
  T = fun() ->
        [Topic] = mnesia:match_object(#erwa_topic{uri=TopicUri,_='_'}),
        #erwa_topic{id=T_Id,uri=TopicUri}=Topic,
        T_Id
      end,
  {atomic,TopicId} = mnesia:transaction(T),
  ok.



-spec send_message_to(Msg :: term(), Peer :: list() |  pid()) -> ok.
send_message_to(Msg,Pid) when is_pid(Pid) ->
  send_message_to(Msg,[Pid]);
send_message_to(Msg,Peers) when is_list(Peers) ->
  Send = fun(Pid) ->
           Pid ! {erwa,Msg} end,
  lists:foreach(Send,Peers),
  ok.

-spec gen_id() -> non_neg_integer().
gen_id() ->
  crypto:rand_uniform(0,9007199254740992).

is_fresh_startup() ->
  Node = node(),
  case mnesia:system_info(tables) of
    [schema] -> true;
    Tbls ->
      case mnesia:table_info(schema, cookie) of
        {_, Node}  -> {exists, Tbls};
        _            -> true
      end
  end.

init_db() ->
  mnesia:stop(),
  mnesia:create_schema([node()]),
  mnesia:start(),

  {atomic,ok} = mnesia:create_table(erwa_realm,[{attributes,record_info(fields,erwa_realm)},
                                             {type,set}]),
  {atomic,ok} = mnesia:create_table(erwa_topic,[{attributes,record_info(fields,erwa_topic)},
                                           {type,set}]),
  {atomic,ok} = mnesia:create_table(erwa_sess,[{attributes,record_info(fields,erwa_sess)},
                                           {index,[pid]},
                                           {type,set}]),
  {atomic,ok} = mnesia:create_table(erwa_exact_sub,[{attributes,record_info(fields,erwa_exact_sub)},
                                           {index,[uri]},
                                           {type,set}]),
  {atomic,ok} = mnesia:create_table(erwa_proc,[{attributes,record_info(fields,erwa_proc)},
                                               {index,[uri]},
                                               {type,set}]),
  {atomic,ok} = mnesia:create_table(erwa_invoc,[{attributes,record_info(fields,erwa_invoc)},
                                                {type,set}]),
  ok.

-ifdef(TEST).

abort_test() ->
  State = create_state(),
  Msg = {hello,<<"realm1">>,[]},
  {[{abort,[],no_such_realm},shutdown],_}=handle_wamp_message(Msg,State).

welcome_test() ->
  State = create_state(),
  Realm = <<"realm1">>,
  Msg = {hello,Realm,[]},
  ok = start_realm(<<"realm1">>),
  {{welcome,_,?ROUTER_DETAILS},_}=handle_wamp_message(Msg,State).

shutdown_test() ->
  State = create_state(),
  Realm = <<"realm1">>,
  Msg = {hello,Realm,[]},
  ok = start_realm(<<"realm1">>),
  {{welcome,_,?ROUTER_DETAILS},NewState}=handle_wamp_message(Msg,State),
  {shutdown,_} = handle_wamp_message(Msg,NewState).


subscribe_test() ->
  State = create_state(),
  Realm = <<"realm1">>,
  Msg = {hello,Realm,[]},
  ok = start_realm(<<"realm1">>),
  {{welcome,_SessionId,?ROUTER_DETAILS},State2}=handle_wamp_message(Msg,State),
  TopicUri = <<"topic1">>,
  RequestId = gen_id(),
  {{subscribed,RequestId,_},_} = handle_wamp_message({subscribe,RequestId,[],TopicUri},State2).

unsubscribe_test() ->
  State = create_state(),
  Realm = <<"realm1">>,
  Msg = {hello,Realm,[]},
  ok = start_realm(<<"realm1">>),
  {{welcome,_,?ROUTER_DETAILS},State2}=handle_wamp_message(Msg,State),
  TopicUri = <<"topic1">>,
  SubRequestId = gen_id(),
  {{subscribed,SubRequestId,TopicId},State3} = handle_wamp_message({subscribe,SubRequestId,[],TopicUri},State2),
  UnsubRequestId = gen_id(),
  {{unsubscribed,UnsubRequestId},_} = handle_wamp_message({unsubscribe,UnsubRequestId,TopicId},State3).

unsubscribe_error_test() ->
  State = create_state(),
  Realm = <<"realm1">>,
  Msg = {hello,Realm,[]},
  ok = start_realm(<<"realm1">>),
  {{welcome,_,?ROUTER_DETAILS},State2}=handle_wamp_message(Msg,State),
  RequestId = gen_id(),
  TopicId = gen_id(),
  {{error,unsubscribe,RequestId,_,no_such_subscription},_} = handle_wamp_message({unsubscribe,RequestId,TopicId},State2).


-endif.
