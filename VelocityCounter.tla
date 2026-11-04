------------------------------ MODULE VelocityCounter ------------------------------
(***************************************************************************)
(* A finite model of a velocity counter. One event names two keys. The    *)
(* worker records one key per step and can crash between them. A later    *)
(* delivery finishes the key that was skipped and leaves a finished key   *)
(* alone.                                                                  *)
(*                                                                         *)
(* Both extractors are always present. Redis is always up. A missing      *)
(* field is left to the later plugin. A missing traffic-source stamp is   *)
(* not a third population value: the plugin treats a missing stamp as     *)
(* human, and this model only contains "human" and "api".                 *)
(*                                                                         *)
(* Crash sequence the invariants allow:                                   *)
(*   1. Deliver of an event whose route is {userKey, ipKey}.              *)
(*   2. Apply on userKey.                                                  *)
(*   3. Crash.                                                             *)
(*   4. userKey contains the event. ipKey does not. OnlyRouted holds.     *)
(*   5. Deliver of the same event, Apply on ipKey, Apply on userKey.      *)
(*   6. Both keys contain the event once.                                  *)
(* An API delivery routes to {apiClientKey, ipKey} and leaves userKey     *)
(* empty of that event.                                                   *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets

CONSTANTS
  MaxEvents,    \* model bound: event ids are 1..MaxEvents
  Retention,    \* a tick stays eligible for a query this long
  MaxNow        \* now stops here so cleanup is reachable

ASSUME MaxEvents \in Nat /\ MaxEvents >= 1
ASSUME Retention \in Nat /\ Retention >= 1
ASSUME MaxNow \in Nat /\ MaxNow >= MaxEvents

UserId == "user-1"
IpId == "ip-1"
ApiKeyId == "key-1"
UserBehavior == "message_sent"
IpBehavior == "request"
\* 0 is not an event id. TLC rejects equality between an integer and a string,
\* so the idle marker stays a number.
None == 0

Populations == {"human", "api"}
EventIds == 1..MaxEvents

\* The score is the event id. A second apply does not store a new score.
ts(e) == e

userKey == <<"user", UserId, UserBehavior>>
ipKey == <<"ip", IpId, IpBehavior>>
apiClientKey == <<"api-client", ApiKeyId, UserBehavior>>
Keys == {userKey, ipKey, apiClientKey}

VARIABLES
  population,  \* event id -> "human" or "api"; chosen once
  now,         \* clock; starts at MaxEvents and only increases
  ticks,       \* key -> set of event ids currently stored
  recorded,    \* key -> set of event ids ever stored; cleanup does not touch it
  inflight,    \* None, or the event id of the delivery being applied
  todo         \* keys still to apply for inflight

vars == <<population, now, ticks, recorded, inflight, todo>>

\* API traffic replaces the user key and still names the IP key.
Route(e) ==
  IF population[e] = "api"
  THEN {apiClientKey, ipKey}
  ELSE {userKey, ipKey}

InWindow(e, W) == now - W < ts(e) /\ ts(e) <= now
Expired(e) == ts(e) <= now - Retention
Count(k, W) == Cardinality({e \in ticks[k] : InWindow(e, W)})

Init ==
  /\ population \in [EventIds -> Populations]
  /\ now = MaxEvents
  /\ ticks = [k \in Keys |-> {}]
  /\ recorded = [k \in Keys |-> {}]
  /\ inflight = None
  /\ todo = {}

\* A delivery, including a Kafka redelivery of an event already recorded.
Deliver(e) ==
  /\ inflight = None
  /\ e \in EventIds
  /\ inflight' = e
  /\ todo' = Route(e)
  /\ UNCHANGED <<population, now, ticks, recorded>>

\* One key, one atomic step. The score is ts(e). A second apply does not
\* replace it. There is no claimed-but-not-ticked state.
Apply ==
  /\ inflight # None
  /\ inflight \in EventIds
  /\ todo # {}
  /\ \E k \in todo:
       /\ ticks' = [ticks EXCEPT ![k] = ticks[k] \union {inflight}]
       /\ recorded' = [recorded EXCEPT ![k] = recorded[k] \union {inflight}]
       /\ todo' = todo \ {k}
       /\ inflight' = IF (todo \ {k}) = {} THEN None ELSE inflight
  /\ UNCHANGED <<population, now>>

Crash ==
  /\ inflight # None
  /\ inflight \in EventIds
  /\ inflight' = None
  /\ todo' = {}
  /\ UNCHANGED <<population, now, ticks, recorded>>

AdvanceNow ==
  /\ now < MaxNow
  /\ now' = now + 1
  /\ UNCHANGED <<population, ticks, recorded, inflight, todo>>

\* Removes expired ids from one key. recorded is unchanged, so a later
\* delivery may put the same id back at its original timestamp.
Cleanup(k) ==
  /\ k \in Keys
  /\ \E e \in ticks[k] : Expired(e)
  /\ ticks' = [ticks EXCEPT ![k] = {e \in ticks[k] : ~Expired(e)}]
  /\ UNCHANGED <<population, now, recorded, inflight, todo>>

Next ==
  \/ \E e \in EventIds : Deliver(e)
  \/ Apply
  \/ Crash
  \/ AdvanceNow
  \/ \E k \in Keys : Cleanup(k)

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ population \in [EventIds -> Populations]
  /\ now \in MaxEvents..MaxNow
  /\ ticks \in [Keys -> SUBSET EventIds]
  /\ recorded \in [Keys -> SUBSET EventIds]
  /\ inflight = None \/ inflight \in EventIds
  /\ todo \subseteq Keys
  /\ inflight = None => todo = {}
  /\ inflight = None \/ todo \subseteq Route(inflight)

RouteShape ==
  \A e \in EventIds :
    /\ ipKey \in Route(e)
    /\ Cardinality(Route(e)) = 2
    /\ IF population[e] = "api"
       THEN /\ apiClientKey \in Route(e)
            /\ userKey \notin Route(e)
       ELSE /\ userKey \in Route(e)
            /\ apiClientKey \notin Route(e)

OnlyRouted ==
  \A k \in Keys :
    \A e \in (ticks[k] \union recorded[k]) :
      k \in Route(e)

RecordedAgrees ==
  \A k \in Keys : ticks[k] \subseteq recorded[k]

WindowBoundary ==
  \A e \in EventIds :
    /\ (ts(e) <= now - Retention) =>
         (\A W \in 1..Retention : ~InWindow(e, W))
    /\ (ts(e) = now - Retention + 1) => InWindow(e, Retention)

ExpiredNotCounted ==
  \A e \in EventIds :
    Expired(e) => (\A W \in 1..Retention : ~InWindow(e, W))

CountMatches ==
  \A k \in Keys :
    \A W \in 1..Retention :
      Count(k, W) = Cardinality({e \in ticks[k] : InWindow(e, W)})

ActivePreserved ==
  [][\A k \in Keys :
       \A e \in ticks[k] :
         (ts(e) > now - Retention) => e \in ticks'[k]]_vars

OneKeyPerStep ==
  [][Cardinality({k \in Keys : ticks[k] # ticks'[k]}) <= 1]_vars

PopulationStable ==
  [][UNCHANGED population]_vars

=============================================================================
