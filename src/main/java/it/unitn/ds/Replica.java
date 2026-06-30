package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.IllegalActorStateException;
import akka.actor.Props;
import vozza_lech.datastore.PersonOfInterest;
import vozza_lech.datastore.PositionList;
import vozza_lech.datastore.UpdateLog;
import vozza_lech.datastore.UpdateTimestamp;
import vozza_lech.datastore.PendingUpdate;
import vozza_lech.datastore.Update;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Replica extends AbstractReplica {

    //////////////////////////////////////////////////////////////

    public static class Epoch {
        public int                    id;
        /// Local map of active replicas.
        /// Only useful to avoid sending messages to them
        public Map<Integer, ActorRef> active_replicas;
        public int                    coordinator_id;
    }

    private enum Status {
        STARTED,
        IDLE,
        CRASHED,
        ELECTION
    }

    private static class CrashRequest {
        public Crash crash;
        public int   curr_message_count;
    }

    Epoch                  m_curr_epoch;
    Status                 m_curr_status;
    /// Possibly pending crash request if it has delayed effect
    Optional<CrashRequest> m_crash_request;

    /// Periodic event used to then send heartbeats and schedule timeouts
    Optional<Cancellable>         m_pending_heartbeat;
    /// Timeout events for sent heartbeats and their replica
    HashMap<Integer, Cancellable> m_heartbeat_timeouts;

    /// Timeout event used to detect silent coordinator failures
    Optional<Cancellable>         m_recv_heartbeat_timeout;

    PositionList           m_position_list;
    UpdateLog              m_updates;

    int m_next_sn = 0;
    Map<UpdateTimestamp, PendingUpdate> m_pending_updates = new HashMap<>();

    //////////////////////////////////////////////////////////////

    public static class RunHeartbeat implements Serializable { }

    public static class HeartbeatRequest implements Serializable {
        public ActorRef coordinator;

        public HeartbeatRequest(ActorRef _coord) {
            coordinator = _coord;
        }
    }

    public static class HeartbeatResponse implements Serializable {
        public ActorRef replica;
        public int      replica_id;

        public HeartbeatResponse(ActorRef _replica, int _replica_id) {
            replica    = _replica;
            replica_id = _replica_id;
        }
    }

    public static class HeartbeatRequestTimeout implements Serializable {
        public ActorRef replica;
        public int      replica_id;

        public HeartbeatRequestTimeout(ActorRef _replica, int _replica_id) {
            replica    = _replica;
            replica_id = _replica_id;
        }
    }

    public static class HeartbeatReceiveTimeout implements Serializable { }

    // UPDATE message: coordinator to all replicas, carries the write to apply
    public static class UpdateMsg implements Serializable {
        public final UpdateTimestamp timestamp;
        public final Update data;

        public UpdateMsg(UpdateTimestamp _timestamp, Update _data) {
            timestamp = _timestamp;
            data = _data;
        }
    }

    // ACK message: replica to coordinator, confirms receipt of an UpdateMsg
    public static class AckMsg implements Serializable {
        public final UpdateTimestamp timestamp;
        public final int replicaId;

        public AckMsg(UpdateTimestamp _timestamp, int _replicaId) {
            timestamp = _timestamp;
            replicaId = _replicaId;
        }
    }

    // WRITEOK message: coordinator to all replicas, confirms quorum reached, apply now
    public static class WriteOkMsg implements Serializable {
        public final UpdateTimestamp timestamp;
        public final Update data;

        public WriteOkMsg(UpdateTimestamp _timestamp, Update _data) {
            timestamp = _timestamp;
            data = _data;
        }
    }
    //////////////////////////////////////////////////////////////



    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        m_curr_epoch        = new Epoch();
        m_curr_status       = Status.STARTED;
        m_crash_request     = Optional.empty();
        m_position_list     = new PositionList();
        m_updates           = new UpdateLog();
        m_next_sn = 0;
        m_pending_updates = new HashMap<>();
        m_pending_heartbeat = Optional.empty();
        m_heartbeat_timeouts     = new HashMap<>();
        m_recv_heartbeat_timeout = Optional.empty();
    }

    public void onWriteRequest(AbstractClient.WriteRequest _request) {
        if (Status.CRASHED == m_curr_status) {
            return;
        }

        if (id == m_curr_epoch.coordinator_id) {
            // We are the coordinator, start the two-phase broadcast ourselves.
            var data = new Update(_request.index, _request.value);
            var timestamp = new UpdateTimestamp(m_curr_epoch.id, m_next_sn);
            m_next_sn++;

            var pending = new PendingUpdate(data, timestamp, getSender(), id);
            m_pending_updates.put(timestamp, pending);

            debug(String.format("broadcasting UPDATE %d:%d (%d, %d)",
                    timestamp.getEpoch(), timestamp.getSn(), _request.index, _request.value));

            m_curr_epoch.active_replicas.forEach((_id, _ref) -> {
                if (_id == id) {
                    return; // don't send the message to ourselves
                }
                tell(new UpdateMsg(timestamp, data), _ref);
            });
        } else {
            // Not the coordinator: just forward the request along.
            var coordinator_ref = m_curr_epoch.active_replicas.get(m_curr_epoch.coordinator_id);
            tell(_request, coordinator_ref);
            // TODO (later, with crash handling): start a timeout here to detect a C that never initiates the broadcast.
        }
    }

    public void onUpdateMsg(UpdateMsg _msg) {
        if (Status.CRASHED == m_curr_status) {
            return;
        }

        debug(String.format("received UPDATE %d:%d (%d, %d)",
                _msg.timestamp.getEpoch(), _msg.timestamp.getSn(),
                _msg.data.getIndex(), _msg.data.getPosition()));

        // Just ACK; update applied upon WRITEOK.
        tell(new AckMsg(_msg.timestamp, id), getSender());

        // TODO (later, with crash handling): start a timeout here to detect a C that never sends WRITEOK after this UPDATE.
    }

    public void onAckMsg(AckMsg _msg) {
        if (Status.CRASHED == m_curr_status) {
            return;
        }

        var pending = m_pending_updates.get(_msg.timestamp);
        if (pending == null) {
            // Already completed or stale ack, nothing to do
            debug(String.format("received late/duplicate ACK for %d:%d from %d",
                    _msg.timestamp.getEpoch(), _msg.timestamp.getSn(), _msg.replicaId));
            return;
        }

        pending.addAck(_msg.replicaId);

        int quorumSize = (getSystemNumberOfActors() / 2) + 1;
        if (pending.hasQuorum(quorumSize)) {
            m_pending_updates.remove(_msg.timestamp);
            m_updates.addLog(pending.getData(), pending.getTimestamp());

            debug(String.format("quorum reached for %d:%d, broadcasting WRITEOK",
                    _msg.timestamp.getEpoch(), _msg.timestamp.getSn()));

            m_curr_epoch.active_replicas.forEach((_id, _ref) -> {
                if (_id == id) {
                    return;
                }
                tell(new WriteOkMsg(pending.getTimestamp(), pending.getData()), _ref);
            });

            // Apply locally too (coordinator is also a replica) and reply to client
            applyUpdate(pending.getData(), pending.getTimestamp());

            var result = new AbstractClient.WriteResult(true,
                    pending.getData().getIndex(), pending.getData().getPosition(), id);
            pending.getClient().tell(result, getSelf());
        }
    }

    public void onWriteOkMsg(WriteOkMsg _msg) {
        if (Status.CRASHED == m_curr_status) {
            return;
        }

        debug(String.format("received WRITEOK %d:%d (%d, %d)",
                _msg.timestamp.getEpoch(), _msg.timestamp.getSn(),
                _msg.data.getIndex(), _msg.data.getPosition()));

        m_updates.addLog(_msg.data, _msg.timestamp);
        applyUpdate(_msg.data, _msg.timestamp);
    }

    private void applyUpdate(Update _data, UpdateTimestamp _timestamp) {
        m_position_list.updatePerson(_data.getIndex(), _data.getPosition(), _timestamp);
        callbackOnUpdateApplied(_data.getIndex(), _data.getPosition());
        log(String.format("applied update %d:%d (%d, %d)",
                _timestamp.getEpoch(), _timestamp.getSn(), _data.getIndex(), _data.getPosition()));
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    /**
     * Called when a crash truly takes effect
     */
    public void onCrashInEffect() {
        // Cancel all events and mark this
        // replica as crashed
        m_curr_status = Status.CRASHED;
        m_pending_heartbeat.ifPresent(Cancellable::cancel);
        m_pending_heartbeat = Optional.empty();
        m_heartbeat_timeouts.forEach((_i, _cancel) -> _cancel.cancel());
        m_heartbeat_timeouts.clear();
        m_crash_request = Optional.empty();
        m_recv_heartbeat_timeout.ifPresent(Cancellable::cancel);
        m_recv_heartbeat_timeout = Optional.empty();
    }

    @Override
    public int getSystemNumberOfActors() {
        // TODO: Change this
        return m_curr_epoch.active_replicas.size();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        if (Status.CRASHED == m_curr_status) {
            debug(String.format("CRASH requested for replica %d, but already crashed", id));
            return;
        }

        // TODO: Verify this is ok
        if(m_crash_request.isPresent()) {
            throw new IllegalActorStateException("Crash requested even though a crash request already exists");
        }

        // Crash immediately
        if(Crash.Type.Now == how_to_crash.type) {
            onCrashInEffect();
            return;
        }

        // Schedule crash in the future
        var crash_req = new CrashRequest();
        crash_req.crash = new Crash(how_to_crash.type, how_to_crash.after_n_messages_of_type);
        crash_req.curr_message_count = 0;
        m_crash_request = Optional.of( crash_req );
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        /// It should not be possible for the
        /// replica to be crashed here
        m_curr_epoch.active_replicas = Map.copyOf(sysInit.group);
        m_curr_epoch.id              = 0;
        m_curr_epoch.coordinator_id  = sysInit.coordinator_id;
        m_curr_status = Status.IDLE;

        for(var person_id = 0; person_id < POSITIONS_LIST_LENGTH; person_id++) {
            m_position_list.addPerson(0);
        }

        if(id == m_curr_epoch.coordinator_id) {
            // Schedule periodic heartbeat events
             m_pending_heartbeat = Optional.of( getContext().getSystem()
                    .getScheduler()
                    .scheduleAtFixedRate(
                            Duration.ofMillis(getCoordinatorBeatInterval()),
                            Duration.ofMillis(getCoordinatorBeatInterval()),
                            getSelf(),
                            new RunHeartbeat(),
                            getContext().getDispatcher(),
                            getSelf()
                    )
             );
        } else {
            // Schedule timeout for heartbeat
            m_recv_heartbeat_timeout = Optional.of(
                    getContext().getSystem()
                            .getScheduler()
                            .scheduleOnce(
                                    Duration.ofMillis(getCoordinatorBeatInterval() * 2L),
                                    getSelf(),
                                    new HeartbeatReceiveTimeout(),
                                    getContext().getDispatcher(),
                                    getSelf()
                            )
            );
        }
    }

    public void onReadRequest(AbstractClient.ReadRequest _request) {
        if(Status.CRASHED == m_curr_status) {
            return;
        }

        var maybe_person = m_position_list.getPerson(_request.index);
        var result = new AbstractClient.ReadResult(maybe_person.isPresent(), _request.index,
                maybe_person.orElse(new PersonOfInterest(new UpdateTimestamp(), 0)).position,
                id);

        getSender().tell(result, getSelf());
    }

    public void onRunHeartbeat(RunHeartbeat _beat) {
        if(m_curr_epoch.coordinator_id != id) {
            // Uhm... what?
            throw new IllegalActorStateException("Running heartbeat on non-coordinator replica");
        }

        // Check if we should crash before sending heartbeats
        if(m_crash_request.isPresent() && Crash.Type.Heartbeat == m_crash_request.get().crash.type) {
            var crash_internal = m_crash_request.get();
            crash_internal.curr_message_count++;
            if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                onCrashInEffect();
                return;
            }
        }

        debug(String.format("running HEARTBEAT from %d", id));

        m_curr_epoch
                .active_replicas
                .forEach((_id, _ref) -> {
                    // Do not send to self
                    if(_id == id) {
                        return;
                    }
                    // Send heartbeat and schedule timeout
                    _ref.tell(new HeartbeatRequest(getSelf()), getSelf());
                    m_heartbeat_timeouts.put(_id,
                            getContext().getSystem()
                                    .getScheduler()
                                    .scheduleOnce(
                                            Duration.ofMillis(getMaxLatency() * 2L),
                                            getSelf(),
                                            new HeartbeatRequestTimeout(_ref, _id),
                                            getContext().getDispatcher(),
                                            getSelf()
                                    )
                            );
                });
    }

    public void onHeartbeatRequestTimeout(HeartbeatRequestTimeout _timeout) {
        if(m_curr_epoch.coordinator_id != id) {
            throw new IllegalActorStateException("Received heartbeat timeout not on coordinator");
        }

        debug(String.format("TIMEOUT for heartbeat to %d", _timeout.replica_id));
        // Remove timeout event from the map
        // and locally mark the replica as dead
        m_heartbeat_timeouts.remove(_timeout.replica_id);
        m_curr_epoch.active_replicas.remove(_timeout.replica_id);
    }

    public void onHeartbeatResponse(HeartbeatResponse _response) {
        if(Status.CRASHED == m_curr_status) {
            return;
        }

        if(m_curr_epoch.coordinator_id != id) {
            throw new IllegalActorStateException("Received heartbeat response not on coordinator");
        }

        if(!m_heartbeat_timeouts.containsKey(_response.replica_id)) {
            debug(String.format("received late heartbeat RESPONSE from %d", _response.replica_id));
            // TODO: Should we reinsert the replica in the active list?
            return;
        }

        debug(String.format("heartbeat RESPONSE from %d", _response.replica_id));
        // Remove timeout event
        m_heartbeat_timeouts.get(_response.replica_id).cancel();
        m_heartbeat_timeouts.remove(_response.replica_id);
    }

    public void onHeartbeatRequest(HeartbeatRequest _request) {
        if(Status.CRASHED == m_curr_status || Status.ELECTION == m_curr_status) {
            return;
        }

        if(m_curr_epoch.coordinator_id == id) {
            return;
        }

        var response = new HeartbeatResponse(getSelf(), id);
        getSender().tell(response, getSelf());

        // Remove coordinator crash failure detection and put
        // a renewed one in its place
        m_recv_heartbeat_timeout.ifPresent(Cancellable::cancel);
        m_recv_heartbeat_timeout = Optional.of(
                getContext().getSystem()
                        .getScheduler()
                        .scheduleOnce(
                                Duration.ofMillis(getCoordinatorBeatInterval() * 2L),
                                getSelf(),
                                new HeartbeatReceiveTimeout(),
                                getContext().getDispatcher(),
                                getSelf()
                        )
        );


        // Verify if we should crash
        if(m_crash_request.isPresent() && Crash.Type.Heartbeat == m_crash_request.get().crash.type) {
            var crash_internal = m_crash_request.get();
            crash_internal.curr_message_count++;
            if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                onCrashInEffect();
            }
        }
    }

    public void onHeartbeatReceiveTimeout(HeartbeatReceiveTimeout _timeout) {
        /// Coordinator silently crashed, run election algorithm
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(AbstractClient.ReadRequest.class, this::onReadRequest)
                .match(AbstractClient.WriteRequest.class, this::onWriteRequest)
                .match(UpdateMsg.class, this::onUpdateMsg)
                .match(AckMsg.class, this::onAckMsg)
                .match(WriteOkMsg.class, this::onWriteOkMsg)
                .match(RunHeartbeat.class, this::onRunHeartbeat)
                .match(HeartbeatRequestTimeout.class, this::onHeartbeatRequestTimeout)
                .match(HeartbeatResponse.class, this::onHeartbeatResponse)
                .match(HeartbeatRequest.class, this::onHeartbeatRequest)
                .match(HeartbeatReceiveTimeout.class, this::onHeartbeatReceiveTimeout)
                .build();
    }

}
