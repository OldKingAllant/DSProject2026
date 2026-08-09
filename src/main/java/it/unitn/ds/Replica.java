package it.unitn.ds;

import akka.actor.*;

import vozza_lech.datastore.PersonOfInterest;
import vozza_lech.datastore.PositionList;
import vozza_lech.datastore.UpdateLog;
import vozza_lech.datastore.UpdateTimestamp;
import vozza_lech.datastore.PendingUpdate;
import vozza_lech.datastore.Update;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

public class Replica extends AbstractReplica {

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
        m_broadcast_timeout = Optional.empty();
        m_writeok_timeout   = Optional.empty();
        m_apply_timeouts = new HashMap<>();
    }

    @Override
    public int getSystemNumberOfActors() {
        return m_total_replicas;
    }

    //region INNER CLASSES — State

    public static class Epoch {
        public int                    id;
        /// Local map of active replicas.
        /// Only useful to avoid sending messages to crashed replicas
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

    // Pairs a write request with the client that sent it
    public static class QueuedWrite {
        public final AbstractClient.WriteRequest request;
        public final ActorRef client;

        public QueuedWrite(AbstractClient.WriteRequest _request, ActorRef _client) {
            request = _request;
            client = _client;
        }
    }

    //endregion

    //region INNER CLASSES — Messages

    public static class RunHeartbeat implements Serializable { }

    public static class HeartbeatRequest implements Serializable {
        public final ActorRef coordinator;

        public HeartbeatRequest(ActorRef _coord) {
            coordinator = _coord;
        }
    }

    public static class HeartbeatResponse implements Serializable {
        public final ActorRef replica;
        public final int      replica_id;

        public HeartbeatResponse(ActorRef _replica, int _replica_id) {
            replica    = _replica;
            replica_id = _replica_id;
        }
    }

    public static class HeartbeatRequestTimeout implements Serializable {
        public final ActorRef replica;
        public final int      replica_id;

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
        public final ActorRef initiator;
        public final ActorRef client;

        public UpdateMsg(UpdateTimestamp _timestamp, Update _data, ActorRef _initiator, ActorRef _client) {
            timestamp = _timestamp;
            data = _data;
            initiator = _initiator;
            client = _client;
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
        public final ActorRef initiator;
        public final ActorRef client;

        public WriteOkMsg(UpdateTimestamp _timestamp, Update _data, ActorRef _initiator, ActorRef _client) {
            timestamp = _timestamp;
            data = _data;
            initiator = _initiator;
            client = _client;
        }
    }

    public static class BroadcastTimeout implements Serializable {}

    public static class WriteOKTimeout implements Serializable {}

    public static class ApplyConfirmation implements Serializable {
        public final UpdateTimestamp timestamp;
        public final ActorRef initiator;
        public final int initiator_id;

        public ApplyConfirmation(UpdateTimestamp _timestamp, ActorRef _initiator, int _id) {
            timestamp = _timestamp;
            initiator = _initiator;
            initiator_id = _id;
        }
    }

    public static class ApplyTimeout implements Serializable {
        public final UpdateTimestamp timestamp;
        public final int initiator_id;
        public final Update orig_update;
        public final ActorRef client;

        public ApplyTimeout(UpdateTimestamp _timestamp, int _initiator_id, Update _update, ActorRef _client) {
            timestamp = _timestamp;
            initiator_id = _initiator_id;
            orig_update = _update;
            client = _client;
        }
    }

    public static class CandidateEntry implements Serializable {
        public final int replicaId;
        public final UpdateTimestamp lastKnownUpdate;
        public CandidateEntry(int _replicaId, UpdateTimestamp _lastKnownUpdate) {
            replicaId = _replicaId;
            lastKnownUpdate = _lastKnownUpdate;
        }
    }

    public static class ElectionMsg implements Serializable {
        public final List<CandidateEntry> candidates;
        public final int for_crashed_coordinator;
        public final int old_epoch_id;

        public ElectionMsg(List<CandidateEntry> _candidates, int _crashed_coord, int _old_epoch) {
            candidates = List.copyOf(_candidates);
            for_crashed_coordinator = _crashed_coord;
            old_epoch_id = _old_epoch;
        }
        // returns a new ElectionMsg with one entry appended
        public ElectionMsg withEntry(CandidateEntry _entry) {
            var list = new ArrayList<>(candidates);
            list.add(_entry);
            return new ElectionMsg(list, this.for_crashed_coordinator, this.old_epoch_id);
        }
    }

    public static class ElectionAckMsg implements Serializable {
        public final int fromReplicaId;
        public ElectionAckMsg(int _fromReplicaId) {
            fromReplicaId = _fromReplicaId;
        }
    }

    public static class ElectionAckTimeout implements Serializable {
        public final int targetReplicaId;
        public ElectionAckTimeout(int _targetReplicaId) {
            targetReplicaId = _targetReplicaId;
        }
    }

    //since we do one update at a time there can be only one update that is not applied to everyone
    public static class SynchronizationMsg implements Serializable {
        public final int newCoordinatorId;
        public final UpdateLog.UpdateInfo missingUpdate;
        public final int newEpoch;
        public SynchronizationMsg(int _newCoordinator, int _newEpoch, UpdateLog.UpdateInfo _missingUpdate) {
            newCoordinatorId = _newCoordinator;
            newEpoch = _newEpoch;
            missingUpdate = _missingUpdate;
        }
    }

    //endregion

    //region FIELDS

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

    Queue<QueuedWrite> m_requested_updates = new LinkedList<>();

    int m_next_sn;
    Map<UpdateTimestamp, PendingUpdate> m_pending_updates;

    Optional<Cancellable>               m_broadcast_timeout;
    Optional<Cancellable>               m_writeok_timeout;

    // Queue of write requests waiting to be broadcast.
    // The coordinator processes only one update at a time to preserve total order:
    // the next request is dequeued only after WRITEOK for the current one is sent.
    Queue<QueuedWrite> m_write_queue = new LinkedList<>();

    // Maps update timestamp to a timeout event which is setup
    // by the coordinator when sending a writeok to the update
    // initiator. If the initiator does not respond,
    // the coordinator itself will send confirmation
    // to the client
    Map<UpdateTimestamp, Cancellable> m_apply_timeouts;

    boolean m_broadcast_in_progress = false;

    boolean m_in_election = false;
    Optional<Cancellable> m_election_ack_timeout    = Optional.empty();
    Optional<Cancellable> m_election_global_timeout = Optional.empty();
    Optional<ElectionMsg> m_last_election_msg       = Optional.empty();
    Optional<Integer> m_possible_winner             = Optional.empty();

    public record Pair<K, V>(K key, V value)
    {
        // intentionally empty
    }

    Optional<Pair<UpdateTimestamp, UpdateMsg>> m_in_flight_update = Optional.empty();
    // Map<UpdateTimestamp, Update> m_seen_updates = new HashMap<>();

    private Set<Integer> m_skipped_in_ring = new HashSet<>();

    int m_total_replicas = 0;

    //endregion

    //region LIFECYCLE

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    /**
     * Initializes epoch, coordinator, and position list from the system setup message.
     * Starts the heartbeat sender if this replica is the coordinator, or the heartbeat receive timeout otherwise.
     */
    @Override
    public void initSystem(InitSystem sysInit) {
        /// It should not be possible for the
        /// replica to be crashed here
        //TODO check that this is ok
        //m_curr_epoch.active_replicas = Map.copyOf(sysInit.group);
        m_curr_epoch.active_replicas = new HashMap<>(sysInit.group);

        m_total_replicas = sysInit.group.size();

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

    /**
     * Registers all message handlers for client requests, the update broadcast protocol,
     * heartbeats, and the election/synchronization protocol.
     */
    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(AbstractClient.ReadRequest.class, this::onReadRequest)
                .match(AbstractClient.WriteRequest.class, this::onWriteRequest)
                .match(QueuedWrite.class, this::onQueuedWrite)
                .match(UpdateMsg.class, this::onUpdateMsg)
                .match(AckMsg.class, this::onAckMsg)
                .match(WriteOkMsg.class, this::onWriteOkMsg)
                .match(BroadcastTimeout.class, this::onBroadcastTimeout)
                .match(WriteOKTimeout.class, this::onWriteOKTimeout)
                .match(ApplyConfirmation.class, this::onApplyConfirmation)
                .match(ApplyTimeout.class, this::onApplyConfirmationTimeout)
                .match(RunHeartbeat.class, this::onRunHeartbeat)
                .match(HeartbeatRequestTimeout.class, this::onHeartbeatRequestTimeout)
                .match(HeartbeatResponse.class, this::onHeartbeatResponse)
                .match(HeartbeatRequest.class, this::onHeartbeatRequest)
                .match(HeartbeatReceiveTimeout.class, this::onHeartbeatReceiveTimeout)
                .match(ElectionMsg.class,        this::onElectionMsg)
                .match(ElectionAckMsg.class,     this::onElectionAckMsg)
                .match(ElectionAckTimeout.class, this::onElectionAckTimeout)
                .match(SynchronizationMsg.class, this::onSynchronizationMsg)
                .match(ElectionGlobalTimeout.class, this::onElectionGlobalTimeout)
                .build();
    }

    //endregion

    //region CRASH MANAGEMENT

    /**
     * Create the crash request: immediate if type is Now, otherwise deferred until
     * the Nth message of the given type is processed.
     */
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

    /**
     * Called when a crash truly takes effect
     * Puts the replica in CRASHED state and cancels every pending scheduled timeout, so it stops reacting to and sending any further message.
     */
    public void onCrashInEffect() {
        // Cancel all events and mark this
        // replica as crashed
        debug(String.format("replica %d CRASHED", id));
        m_curr_status = Status.CRASHED;
        m_pending_heartbeat.ifPresent(Cancellable::cancel);
        m_pending_heartbeat = Optional.empty();
        m_heartbeat_timeouts.forEach((_i, _cancel) -> _cancel.cancel());
        m_heartbeat_timeouts.clear();
        m_crash_request = Optional.empty();
        m_recv_heartbeat_timeout.ifPresent(Cancellable::cancel);
        m_recv_heartbeat_timeout = Optional.empty();
        m_broadcast_timeout.ifPresent(Cancellable::cancel);
        m_broadcast_timeout = Optional.empty();
        m_writeok_timeout.ifPresent(Cancellable::cancel);
        m_writeok_timeout = Optional.empty();
        m_election_ack_timeout.ifPresent(Cancellable::cancel);
        m_election_ack_timeout = Optional.empty();
        m_election_global_timeout.ifPresent(Cancellable::cancel);
        m_election_global_timeout = Optional.empty();
        m_apply_timeouts.forEach((_timestamp, _event) -> _event.cancel());
        m_apply_timeouts.clear();
    }

    //endregion

    //region CLIENT REQUESTS

    /**
     * Answers a client READ immediately from local state, no coordination needed.
     */
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

    /**
     * Entry point for a client WRITE. If this replica is the coordinator, enqueues the write
     * and tries to start a broadcast; otherwise forwards it to the coordinator and starts a BroadcastTimeout.
     */
    public void onWriteRequest(AbstractClient.WriteRequest _request) {
        if (Status.CRASHED == m_curr_status) {
            return;
        }

        _request.replica = getSelf();

        if (id == m_curr_epoch.coordinator_id) {
            // Enqueue and try to start broadcast (starts immediately if nothing in flight)
            m_write_queue.add(new QueuedWrite(_request, getSender()));
            tryStartNextBroadcast();

            if(m_crash_request.isPresent() && Crash.Type.Update == m_crash_request.get().crash.type) {
                var crash_internal = m_crash_request.get();
                crash_internal.curr_message_count++;
                if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                    onCrashInEffect();
                }
            }
        } else {
            var queued_write = new QueuedWrite(_request, getSender());

            // Can simply put them in the queue because:
            // messages are removed from the queue
            // only on apply update. If the
            // coordinator crashes, messages
            // in its queue that are not sent
            // will be lost, either way, messages
            // that have not been sent or cannot
            // be completed will never be removed
            // from the queue, but they will be
            // sent in bulk when the new
            // coordinator is elected.
            // So if we put both sent and
            // not sent messages in this queue,
            // it will be ok
            m_requested_updates.add(queued_write);

            if(Status.ELECTION != m_curr_status) {
                // Not the coordinator: just forward the request along.
                var coordinator_ref = m_curr_epoch.active_replicas.get(m_curr_epoch.coordinator_id);
                coordinator_ref.tell(queued_write, getSelf());

                m_broadcast_timeout.ifPresent(Cancellable::cancel);
                m_broadcast_timeout = Optional.of(
                        getContext().getSystem()
                                .getScheduler()
                                .scheduleOnce(
                                        Duration.ofMillis(getMaxLatencyPlusTolerance() * 2),
                                        getSelf(),
                                        new BroadcastTimeout(),
                                        getContext().getDispatcher(),
                                        getSelf()
                                )
                );
            }
            // Else they will be sent after the election is complete

            if(m_crash_request.isPresent() && Crash.Type.Update == m_crash_request.get().crash.type) {
                var crash_internal = m_crash_request.get();
                crash_internal.curr_message_count++;
                if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                    onCrashInEffect();
                }
            }

        }
    }

    /**
     * Coordinator-only: receives a write forwarded by another replica and enqueues it
     * for the next broadcast round.
     */
    public void onQueuedWrite(QueuedWrite _write) {
        if(Status.CRASHED == m_curr_status) {
            return;
        }

        if(id != m_curr_epoch.coordinator_id) {
            throw new IllegalActorStateException("Non-coordinator received queue write");
        }

        m_write_queue.add(_write);
        tryStartNextBroadcast();

        if(m_crash_request.isPresent() && Crash.Type.Update == m_crash_request.get().crash.type) {
            var crash_internal = m_crash_request.get();
            crash_internal.curr_message_count++;
            if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                onCrashInEffect();
            }
        }
    }

    /**
     * Dequeues the next pending write and starts a new UPDATE broadcast round,
     * only if no other update is currently in flight (enforces total order).
     * Satisfies Total Order property, only one update in flight per coordinator at a time.
     */
    private void tryStartNextBroadcast() {
        // Only start a new broadcast if nothing is currently in flight
        if (m_broadcast_in_progress || m_write_queue.isEmpty()) {
            return;
        }

        var queuedWrite = m_write_queue.poll();
        m_broadcast_in_progress = true;

        var data = new Update(queuedWrite.request.index, queuedWrite.request.value);
        var timestamp = new UpdateTimestamp(m_curr_epoch.id, m_next_sn);
        m_next_sn++;

        var pending = new PendingUpdate(data, timestamp, queuedWrite.client, id, queuedWrite.request.replica);
        m_pending_updates.put(timestamp, pending);

        debug(String.format("broadcasting UPDATE %d:%d (%d, %d)",
                timestamp.getEpoch(), timestamp.getSn(), queuedWrite.request.index, queuedWrite.request.value));

        m_curr_epoch.active_replicas.forEach((_id, _ref) -> {
            if (_id == id) return;
            tell(new UpdateMsg(timestamp, data, queuedWrite.request.replica, queuedWrite.client), _ref);
        });
    }

    //endregion

    //region UPDATE BROADCAST

    /**
     * Cancels the current BroadcastTimeout and reschedules it for the next
     * queued write request, if any (handles multiple pending writes correctly).
     */
    public void removeBroadcastTimeout(boolean _schedule_new_timeout) {
        m_broadcast_timeout.ifPresent(Cancellable::cancel);
        m_broadcast_timeout = Optional.empty();

        var next_update = m_requested_updates.stream().skip(1).findFirst();

        if(_schedule_new_timeout && next_update.isPresent()) {
            m_broadcast_timeout = Optional.of(
                    getContext().getSystem()
                            .getScheduler()
                            .scheduleOnce(
                                    Duration.ofMillis(getMaxLatencyPlusTolerance() * 2),
                                    getSelf(),
                                    new BroadcastTimeout(),
                                    getContext().getDispatcher(),
                                    getSelf()
                            )
            );
        }
    }

    /**
     * Replica-side handler for the coordinator's UPDATE broadcast: records the update as in-flight,
     * replies with an ACK and starts a WriteOKTimeout to detect a coordinator crash mid-broadcast.
     * Satisfies the two-phase update protocol (UPDATE/ACK, then WRITEOK)
     */
    public void onUpdateMsg(UpdateMsg _msg) {
        if (Status.CRASHED == m_curr_status) {
            return;
        }

        m_in_flight_update = Optional.of(new Pair<>(_msg.timestamp, _msg));

        // Do not schedule a new broadcast timeout,
        // since this update still has to complete,
        // and the new broadcast might go in timeout
        // before completion (also if other replicas
        // request updates in the meantime)
        removeBroadcastTimeout(false);

        debug(String.format("received UPDATE %d:%d (%d, %d)",
                _msg.timestamp.getEpoch(), _msg.timestamp.getSn(),
                _msg.data.getIndex(), _msg.data.getPosition()));

        // Just ACK; update applied upon WRITEOK.
        tell(new AckMsg(_msg.timestamp, id), getSender());

        // Crash replica if crash type is after update
        if(m_crash_request.isPresent() && Crash.Type.Update == m_crash_request.get().crash.type) {
            var crash_internal = m_crash_request.get();
            crash_internal.curr_message_count++;
            if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                onCrashInEffect();
                return;
            }
        }

        m_writeok_timeout.ifPresent(Cancellable::cancel);
        m_writeok_timeout = Optional.of(
                getContext().getSystem()
                        .getScheduler()
                        .scheduleOnce(
                                Duration.ofMillis(getMaxLatencyPlusTolerance() * 2),
                                getSelf(),
                                new WriteOKTimeout(),
                                getContext().getDispatcher(),
                                getSelf()
                        )
        );
    }

    /**
     * Coordinator-side handler for ACKs: once a quorum is reached, logs the update, broadcasts WRITEOK, applies it locally
     * and starts the next queued broadcast.
     * Satisfies quorum size |Q| = floor(N/2)+1 (strict majority)
     */
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
            m_updates.addLog(pending.getData(), pending.getTimestamp(), pending.getInitiator(),
                    pending.getClient());

            debug(String.format("quorum reached for %d:%d, broadcasting WRITEOK",
                    _msg.timestamp.getEpoch(), _msg.timestamp.getSn()));

            var initiator_id_ref = m_curr_epoch.active_replicas.entrySet()
                            .stream()
                            .filter((_id_ref) -> _id_ref.getValue().equals(pending.getInitiator()))
                            .findFirst();

            m_curr_epoch.active_replicas.forEach((_id, _ref) -> {
                if (_id == id || Status.CRASHED == m_curr_status) {
                    return;
                }

                tell(new WriteOkMsg(pending.getTimestamp(), pending.getData(), pending.getInitiator(),
                        pending.getClient()), _ref);

                if(m_crash_request.isPresent() && Crash.Type.WriteOK == m_crash_request.get().crash.type) {
                    var crash_internal = m_crash_request.get();
                    crash_internal.curr_message_count++;
                    if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                        onCrashInEffect();
                    }
                }
            });

            if(Status.CRASHED == m_curr_status) {
                return;
            }

            // Apply locally too (coordinator is also a replica) and reply to client
            applyUpdate(pending.getData(), pending.getTimestamp(), pending.getInitiator(), pending.getClient());

            if(!pending.getInitiator().equals(getSelf())) {
                // Initiator crashed sometime before the quorum was reached
                // Send the confirmation to the client
                if(initiator_id_ref.isEmpty()) {
                    var result = new AbstractClient.WriteResult(true,
                            pending.getData().getIndex(), pending.getData().getPosition(), id);
                    pending.getClient().tell(result, getSelf());
                } else {
                    // Set up confirmation timeout
                    m_apply_timeouts.put(_msg.timestamp,
                            getContext().getSystem()
                                    .getScheduler()
                                    .scheduleOnce(
                                            Duration.ofMillis(getMaxLatencyPlusTolerance()),
                                            getSelf(),
                                            new ApplyTimeout(_msg.timestamp, initiator_id_ref.get().getKey(), pending.getData(), pending.getClient()),
                                            getContext().getDispatcher(),
                                            getSelf()
                                    )
                    );
                }
            }

            m_broadcast_in_progress = false;
            tryStartNextBroadcast(); // start next write if any are queued
        }
    }

    /**
     * Replica-side handler for WRITEOK: clears in-flight/timeout state, logs the update and applies it locally.
     * Satisfies the two-phase update protocol (UPDATE/ACK, then WRITEOK)
     */
    public void onWriteOkMsg(WriteOkMsg _msg) {
        if (Status.CRASHED == m_curr_status) {
            return;
        }

        m_in_flight_update = Optional.empty();

        removeBroadcastTimeout(true);

        m_writeok_timeout.ifPresent(Cancellable::cancel);
        m_writeok_timeout = Optional.empty();

        debug(String.format("received WRITEOK %d:%d (%d, %d)",
                _msg.timestamp.getEpoch(), _msg.timestamp.getSn(),
                _msg.data.getIndex(), _msg.data.getPosition()));

        m_updates.addLog(_msg.data, _msg.timestamp, _msg.initiator, _msg.client);
        applyUpdate(_msg.data, _msg.timestamp, _msg.initiator, _msg.client);

        if(m_crash_request.isPresent() && Crash.Type.WriteOK == m_crash_request.get().crash.type) {
            var crash_internal = m_crash_request.get();
            crash_internal.curr_message_count++;
            if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                onCrashInEffect();
            }
        }
    }

    /**
     * Applies a confirmed update to the local position list; if this replica is the initiator,
     * also removes it from the pending queue and replies to the client with the WriteResult.
     * Satisfies the integrity property, a client's write is completed (WriteResult sent) exactly once,
     * on the replica that originally received it, regardless of which path completed the update.
     */
    private void applyUpdate(Update _data, UpdateTimestamp _timestamp, ActorRef _initiator, ActorRef _client) {
        if(_initiator.equals(getSelf())) {
            // Message was propagated by us, remove
            // it from the queue. 
            m_requested_updates.poll();

            var result = new AbstractClient.WriteResult(true,
                    _data.getIndex(), _data.getPosition(), id);
            _client.tell(result, getSelf());

            if(m_curr_epoch.coordinator_id != id) {
                getSender().tell(new ApplyConfirmation(_timestamp, getSelf(), id), getSelf());
            }
        }

        m_position_list.updatePerson(_data.getIndex(), _data.getPosition(), _timestamp);
        callbackOnUpdateApplied(_data.getIndex(), _data.getPosition());
        log(String.format("applied update %d:%d (%d, %d)",
                _timestamp.getEpoch(), _timestamp.getSn(), _data.getIndex(), _data.getPosition()));
    }

    public void onApplyConfirmation(ApplyConfirmation _confirm) {
        if(Status.CRASHED == m_curr_status) {
            return;
        }

        debug(String.format("update APPLY confirmed by initiator %d", _confirm.initiator_id));

        if(!m_apply_timeouts.containsKey(_confirm.timestamp)) {
            return;
        }

        m_apply_timeouts.remove(_confirm.timestamp).cancel();

        // Nothing to do here
    }

    public void onApplyConfirmationTimeout(ApplyTimeout _timeout) {
        if(Status.CRASHED == m_curr_status) {
            return;
        }

        debug(String.format("update APPLY timeout for initiator %d", _timeout.initiator_id));
        m_apply_timeouts.remove(_timeout.timestamp);
        m_curr_epoch.active_replicas.remove(_timeout.initiator_id);

        // Manually send confirmation to client
        var result = new AbstractClient.WriteResult(true,
                _timeout.orig_update.getIndex(), _timeout.orig_update.getPosition(), id);
        _timeout.client.tell(result, getSelf());
    }

    //endregion

    //region HEARTBEAT

    /**
     * Coordinator-only periodic tick: sends a HEARTBEAT to every active replica and arms a per-replica timeout to detect silent replica failures.
     */
    public void onRunHeartbeat(RunHeartbeat _beat) {
        if(m_curr_epoch.coordinator_id != id) {
            // Uhm... what?
            throw new IllegalActorStateException("Running heartbeat on non-coordinator replica");
        }

        debug(String.format("running HEARTBEAT from %d", id));

        m_curr_epoch
                .active_replicas
                .forEach((_id, _ref) -> {
                    // Do not send to self or crashed
                    if(Status.CRASHED == m_curr_status || _id == id) {
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

                    // Check if we should crash before sending heartbeat
                    if(m_crash_request.isPresent() && Crash.Type.Heartbeat == m_crash_request.get().crash.type) {
                        var crash_internal = m_crash_request.get();
                        crash_internal.curr_message_count++;
                        if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                            onCrashInEffect();
                        }
                    }
                });
    }

    /**
     * Coordinator-side: a replica failed to answer a heartbeat in time so it is removed from the active replicas set.
     */
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

    /**
     * Coordinator-side: cancels the heartbeat timeout for a replica that just replied, confirms it is alive.
     */
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

    /**
     * Non-coordinator: replies to the coordinator's heartbeat and renews the local coordinator-liveness timeout.
     */
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

    /**
     * Fires when no heartbeat arrived from the coordinator in time; starts a new election
     * (detects a silent coordinator crash).
     */
    public void onHeartbeatReceiveTimeout(HeartbeatReceiveTimeout _timeout) {
        if (Status.CRASHED == m_curr_status) return;
        debug(String.format("replica %d HEARTBEAT timeout", id));
        startElection(m_curr_epoch.coordinator_id);
    }

    //endregion

    //region ELECTION

    public static class ElectionGlobalTimeout implements Serializable {}

    private void removeTimeoutsForElection() {
        m_recv_heartbeat_timeout.ifPresent(Cancellable::cancel);
        m_recv_heartbeat_timeout = Optional.empty();

        m_broadcast_timeout.ifPresent(Cancellable::cancel);
        m_broadcast_timeout = Optional.empty();

        m_writeok_timeout.ifPresent(Cancellable::cancel);
        m_writeok_timeout = Optional.empty();
    }

    /**
     * Begins a ring-based election: builds this replica's candidate entry and forwards it to the next reachable replica.
     */
    private void startElection(int _crashedCoordinatorId) {
        if (m_in_election) {
            debug("already in election, ignoring startElection call");
            return;
        }

        m_curr_status = Status.ELECTION;
        m_in_election = true;
        m_skipped_in_ring = new java.util.HashSet<>();
        m_skipped_in_ring.add(_crashedCoordinatorId);

        removeTimeoutsForElection();

        callbackOnElectionStarted(_crashedCoordinatorId);

        if(1 < m_curr_epoch.active_replicas.size()) {
            var myEntry = buildMyEntry();
            var electionMsg = new ElectionMsg(java.util.List.of(myEntry), _crashedCoordinatorId, m_curr_epoch.id);
            m_last_election_msg = Optional.of(electionMsg);

            var next = computeNextInRing(m_skipped_in_ring);
            tell(electionMsg, next);

            debug(String.format("started ELECTION, forwarding to %s", next.path().name()));

            scheduleElectionAckTimeout(next);
            scheduleElectionGlobalTimeout();
        } else {
            // This should not happen, but a test causes this to happen
            debug(String.format("ELECTION with only one replica: %d", id));
            becomeCoordinator();
        }

    }

    /**
     * Election handler: ignores stale elections, elects the winner once the ring is complete
     * or appends this replica's entry and forwards the message.
     */
    public void onElectionMsg(ElectionMsg _msg){
        if (m_curr_status == Status.CRASHED) {
            return;
        }
        tell(new ElectionAckMsg(id), getSender());

        boolean cond_1 = (m_possible_winner.isPresent() && m_possible_winner.get() != _msg.for_crashed_coordinator);
        boolean cond_2 = (m_possible_winner.isEmpty() && _msg.for_crashed_coordinator != m_curr_epoch.coordinator_id);
        if(cond_1 || cond_2) {
            // Coordinator was already elected,
            // avoid circulating the same election
            // indefinitely

            // The only important thing is that the new coordinator should receive
            // the complete election list. The following replicas have no need
            // to do that, only to sync with the coordinator
            if (cond_1) {
                debug(String.format("replica %d received a stale ELECTION message (new election)",id));
            } else {
                debug(String.format("replica %d received a stale ELECTION message (old election)",id));
            }


            if(m_crash_request.isPresent() && Crash.Type.Election == m_crash_request.get().crash.type) {
                var crash_internal = m_crash_request.get();
                crash_internal.curr_message_count++;
                if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                    onCrashInEffect();
                }
            }

            return;
        }

        if(m_curr_epoch.coordinator_id == id) {
            debug(String.format("coordinator %d received an ELECTION message",id));
            // This should never happen. But now that we are here we cannot simply
            // drop the message, otherwise the previous nodes in the ring will go
            // in timeout.
            becomeCoordinator();
            return;
        }

        boolean hasMyEntry = _msg.candidates.stream().anyMatch(c -> c.replicaId == id);
        if (hasMyEntry) {
            var winner = pickWinner(_msg.candidates);
            m_possible_winner = Optional.of(winner.replicaId);
            debug(String.format("ELECTION complete, winner is %d", winner.replicaId));

            if (winner.replicaId == id){
                becomeCoordinator();
            } else {
                var next = computeNextInRing(m_skipped_in_ring);
                m_last_election_msg = Optional.of(_msg);
                tell(_msg, next);
                scheduleElectionAckTimeout(next);
            }

            if(m_crash_request.isPresent() && Crash.Type.Election == m_crash_request.get().crash.type) {
                var crash_internal = m_crash_request.get();
                crash_internal.curr_message_count++;
                if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                    onCrashInEffect();
                }
            }

            return;
        }

        if (!m_in_election){
            m_in_election = true;
            m_curr_status = Status.ELECTION;
            m_skipped_in_ring = new java.util.HashSet<>();
            m_skipped_in_ring.add(_msg.for_crashed_coordinator);

            removeTimeoutsForElection();

            callbackOnElectionStarted(m_curr_epoch.coordinator_id);
            scheduleElectionGlobalTimeout();
        }

        var updated = _msg.withEntry(buildMyEntry());
        m_last_election_msg = Optional.of(updated);
        var next = computeNextInRing(m_skipped_in_ring);
        tell(updated, next);
        scheduleElectionAckTimeout(next);

        debug(String.format("forwarding ELECTION to %s", next.path().name()));

        if(m_crash_request.isPresent() && Crash.Type.Election == m_crash_request.get().crash.type) {
            var crash_internal = m_crash_request.get();
            crash_internal.curr_message_count++;
            if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                onCrashInEffect();
            }
        }
    }

    /**
     * Cancels the per-hop election ack timeout once the next replica in the ring
     * confirms it received the forwarded election message.
     */
    public void onElectionAckMsg(ElectionAckMsg _msg){
        if (m_curr_status == Status.CRASHED){
            return;
        }
        m_election_ack_timeout.ifPresent(Cancellable::cancel);
        m_election_ack_timeout = Optional.empty();

        if(m_crash_request.isPresent() && Crash.Type.Election == m_crash_request.get().crash.type) {
            var crash_internal = m_crash_request.get();
            crash_internal.curr_message_count++;
            if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                onCrashInEffect();
            }
        }
    }

    /**
     * Promotes this replica to coordinator for a new epoch: recovers any missing update,
     * broadcasts a SYNCHRONIZATION message and resumes processing pending writes.
     * Satisfies the safety property, an update acknowledged by a quorum before the crash
     * is never lost, by re-propagating the in-flight or last-logged update via SYNCHRONIZATION.
     */
    private void becomeCoordinator(){
        callbackOnCoordinatorElected(id);

        m_curr_epoch.id++;
        m_curr_epoch.coordinator_id = id;
        m_next_sn = 0;

        m_possible_winner = Optional.empty();

        UpdateLog.UpdateInfo missingUpdate = null;
        if (m_in_flight_update.isPresent()) { // Check if update in flight
            // That update cannot have been applied, no writeok
            var update = m_in_flight_update.get();
            missingUpdate = new UpdateLog.UpdateInfo(update.value().data, update.key(), update.value().initiator, update.value().client);
            m_updates.addLog(update.value().data, update.key(), update.value().initiator, update.value().client);
            applyUpdate(update.value().data, update.key(), update.value().initiator, update.value().client);
        } else if(m_updates.getLastLogTimestamp().isPresent()) {
            // Else, push the last applied update,
            // since we do not know if everyone
            // has applied it
            var last_stamp = m_updates.getLastLogTimestamp().get();
            var last_update = m_updates.getLog(last_stamp);

            if(last_update.isPresent()) {
                missingUpdate = new UpdateLog.UpdateInfo(last_update.get().data, last_stamp, last_update.get().initiator,
                        last_update.get().client);
            }
        }

        // If any update was in flight, it has been applied
        // in the previous lines
        m_in_flight_update = Optional.empty();

        var syncMsg = new SynchronizationMsg(id, m_curr_epoch.id, missingUpdate);

        // Now send the sync message to all known active replicas
        m_curr_epoch.active_replicas.forEach((_id, _ref) -> {
            if (_id != id) tell(syncMsg, _ref);
        });

        m_election_ack_timeout.ifPresent(Cancellable::cancel);
        m_election_ack_timeout = Optional.empty();
        m_election_global_timeout.ifPresent(Cancellable::cancel);
        m_election_global_timeout = Optional.empty();

        m_in_election = false;
        m_curr_status = Status.IDLE;

        m_pending_heartbeat.ifPresent(Cancellable::cancel);
        m_pending_heartbeat = Optional.of(
                getContext().getSystem().getScheduler().scheduleAtFixedRate(
                        Duration.ofMillis(getCoordinatorBeatInterval()),
                        Duration.ofMillis(getCoordinatorBeatInterval()),
                        getSelf(),
                        new RunHeartbeat(),
                        getContext().getDispatcher(),
                        getSelf()
                )
        );

        debug(String.format("became new coordinator, epoch %d", m_curr_epoch.id));

        // If we had halted updates, enqueue them now
        for(var update : m_requested_updates) {
            onQueuedWrite(update);
        }
        // Here we can clear the queue, since if we crash,
        // those updates would be lost anyway (we are the initiator)
        m_requested_updates.clear();

        tryStartNextBroadcast(); // FIFO grants this arrives after syncMsg
    }

    /**
     * Non-coordinator: catches up on any missing update from the new coordinator,
     * updates epoch/coordinator info, and forwards queued writes to the new coordinator.
     */
    public void onSynchronizationMsg(SynchronizationMsg _msg){
        if (m_curr_status == Status.CRASHED){
            return;
        }

        if (_msg.missingUpdate != null && m_updates.addLogIfAbsent(_msg.missingUpdate.data, _msg.missingUpdate.timestamp,
                _msg.missingUpdate.initiator, _msg.missingUpdate.client)) {
            applyUpdate(_msg.missingUpdate.data, _msg.missingUpdate.timestamp, _msg.missingUpdate.initiator,
                    _msg.missingUpdate.client);
        }

        m_curr_epoch.coordinator_id = _msg.newCoordinatorId;
        m_curr_epoch.id = _msg.newEpoch;

        m_possible_winner = Optional.empty();

        // If any update was in flight, it for sure
        // has been applied through the sync message
        m_in_flight_update = Optional.empty();
        callbackOnCoordinatorElected(_msg.newCoordinatorId);

        m_election_global_timeout.ifPresent(Cancellable::cancel);
        m_election_global_timeout = Optional.empty();
        m_election_ack_timeout.ifPresent(Cancellable::cancel);
        m_election_ack_timeout = Optional.empty();
        m_in_election = false;
        m_curr_status = Status.IDLE;

        m_recv_heartbeat_timeout.ifPresent(Cancellable::cancel);
        m_recv_heartbeat_timeout = Optional.of(
                getContext().getSystem().getScheduler().scheduleOnce(
                        Duration.ofMillis(getCoordinatorBeatInterval() * 2L),
                        getSelf(),
                        new HeartbeatReceiveTimeout(),
                        getContext().getDispatcher(),
                        getSelf()
                )
        );

        debug(String.format("synchronized with new coordinator %d, epoch %d",
                _msg.newCoordinatorId, _msg.newEpoch));

        if(m_crash_request.isPresent() && Crash.Type.Election == m_crash_request.get().crash.type) {
            var crash_internal = m_crash_request.get();
            crash_internal.curr_message_count++;
            if(crash_internal.curr_message_count >= crash_internal.crash.after_n_messages_of_type) {
                onCrashInEffect();
                return;
            }
        }

        // Forward all halted updates to the new coordinator
        var coordinator_ref = m_curr_epoch.active_replicas.get(m_curr_epoch.coordinator_id);
        for(var update : m_requested_updates) {
            coordinator_ref.tell(update, getSelf());
        }
        // Do not clear the queue, new coordinator might crash,
        // losing the requested updates
        // m_requested_updates.clear();
    }

    /**
     * Finds the next reachable replica in the logical ring, skipping any id marked as unreachable.
     */
    private ActorRef computeNextInRing(Set<Integer> _skip) {
        var sortedIds = new ArrayList<>(m_curr_epoch.active_replicas.keySet());
        Collections.sort(sortedIds);

        int myIndex = sortedIds.indexOf(id);
        int size = sortedIds.size();

        for (int i = 1; i < size; i++) {
            int nextId = sortedIds.get((myIndex + i) % size);
            if (!_skip.contains(nextId)) {
                return m_curr_epoch.active_replicas.get(nextId);
            }
        }

        // Should never happen since majority is alive
        // But a test case makes this happen :(
        // throw new IllegalActorStateException("No reachable replica in ring");
        return getSelf();
    }

    /**
     * Selects the election winner: the candidate with the most recent known update, break ties with replica id.
     * Satisfies the guarantee that the elected coordinator will know the most recent update,
     * since a majority (hence at least one correct replica with full knowledge) is always alive.
     */
    private CandidateEntry pickWinner(List<CandidateEntry> _candidates) {
        return _candidates.stream().max(Comparator.comparing((CandidateEntry a) -> a.lastKnownUpdate).thenComparingInt(a -> a.replicaId)).orElseThrow();
    }

    /**
     * Builds this replica's own candidacy entry for an election, based on its most recent in-flight or logged update.
     */
    private CandidateEntry buildMyEntry() {
        // First check if there is an incomplete update and use that
        // as the last known update, else use the last writeoked
        // update
        UpdateTimestamp lastApplied = m_in_flight_update.map(Pair::key).orElse(m_updates.getLastLogTimestamp()
                .orElse(new UpdateTimestamp(0, 0)));
        return new CandidateEntry(id, lastApplied);
    }

    /**
     * Fires when an election fails to complete in time; resets election state and restarts it.
     */
    public void onElectionGlobalTimeout(ElectionGlobalTimeout _timeout) {
        if (Status.CRASHED == m_curr_status || Status.ELECTION != m_curr_status) {
            return;
        }
        debug(String.format("replica %d ELECTION timeout", id));
        var old_coordinator_id = m_possible_winner.orElse(m_curr_epoch.coordinator_id);
        m_in_election = false;
        startElection(old_coordinator_id);
    }

    /**
     * (Re)schedules the timeout for the ack of the election message just forwarded to the given target replica.
     */
    private void scheduleElectionAckTimeout(ActorRef _target) {
        m_election_ack_timeout.ifPresent(Cancellable::cancel);
        // Extract the ID from active_replicas by reverse lookup
        int targetId = m_curr_epoch.active_replicas.entrySet().stream()
                .filter(e -> e.getValue().equals(_target))
                .map(Map.Entry::getKey)
                .findFirst().orElse(-1);
        m_election_ack_timeout = Optional.of(
                getContext().getSystem().getScheduler().scheduleOnce(
                        Duration.ofMillis(getMaxLatencyPlusTolerance()),
                        getSelf(),
                        new ElectionAckTimeout(targetId),
                        getContext().getDispatcher(),
                        getSelf()
                )
        );
    }

    /**
     * (Re)schedules the overall election-completion timeout, scaled by the number of replicas.
     */
    private void scheduleElectionGlobalTimeout() {
        m_election_global_timeout.ifPresent(Cancellable::cancel);
        long delay = (long) getMaxLatencyPlusTolerance() * getSystemNumberOfActors();
        m_election_global_timeout = Optional.of(
                getContext().getSystem().getScheduler().scheduleOnce(
                        Duration.ofMillis(delay),
                        getSelf(),
                        new ElectionGlobalTimeout(),
                        getContext().getDispatcher(),
                        getSelf()
                )
        );
    }

    /**
     * The next replica in the ring failed to ack in time; skips it and retries
     * with the next reachable candidate.
     * Satisfies the election fault tolerance, the ring election completes even with
     * multiple consecutive replica crashes, by skipping unresponsive nodes.
     */
    public void onElectionAckTimeout(ElectionAckTimeout _timeout) {
        if (Status.CRASHED == m_curr_status || Status.ELECTION != m_curr_status) {
            return;
        }
        debug(String.format("ELECTION ACK timeout, skipping replica %d", _timeout.targetReplicaId));
        m_skipped_in_ring.add(_timeout.targetReplicaId);
        m_curr_epoch.active_replicas.remove(_timeout.targetReplicaId);

        var msgToForward = m_last_election_msg.orElseThrow();
        var next = computeNextInRing(m_skipped_in_ring);
        tell(msgToForward, next);
        scheduleElectionAckTimeout(next);
    }

    /**
     * Fires when a forwarded write never got a response from the coordinator; starts an election
     * (coordinator crashed before starting the broadcast).
     * Satisfies the crash detection mid-protocol, coordinator crash before starting the broadcast (Broadcast) or before completing it (WriteOK)
     */
    public void onBroadcastTimeout(BroadcastTimeout _timeout) {
        if (Status.CRASHED == m_curr_status) return;
        debug(String.format("replica %d BROADCAST timeout", id));
        startElection(m_curr_epoch.coordinator_id);
    }

    /**
     * Fires when an ACK-ed UPDATE never got a WRITEOK in time; starts an election
     * (coordinator crashed mid-broadcast).
     * Satisfies the crash detection mid-protocol, coordinator crash before starting the broadcast (Broadcast) or before completing it (WriteOK)
     */
    public void onWriteOKTimeout(WriteOKTimeout _timeout) {
        if (Status.CRASHED == m_curr_status) return;
        debug(String.format("replica %d WRITEOK timeout", id));
        startElection(m_curr_epoch.coordinator_id);
    }

    //endregion

}
