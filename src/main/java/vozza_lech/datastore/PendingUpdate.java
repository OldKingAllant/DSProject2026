package vozza_lech.datastore;

import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class PendingUpdate implements Serializable {

    private final Update m_data;
    private final UpdateTimestamp m_timestamp;
    private final ActorRef m_client;
    private final Set<Integer> m_acks;

    public PendingUpdate(Update _data, UpdateTimestamp _timestamp, ActorRef _client, int _coordinatorId) {
        m_data = _data;
        m_timestamp = _timestamp;
        m_client = _client;
        m_acks = new HashSet<>();
        m_acks.add(_coordinatorId);
    }

    public Update getData() {
        return m_data;
    }

    public UpdateTimestamp getTimestamp() {
        return m_timestamp;
    }

    public ActorRef getClient() {
        return m_client;
    }

    public boolean addAck(int _replicaId) {
        return m_acks.add(_replicaId);
    }

    public boolean hasQuorum(int _quorumSize) {
        return m_acks.size() >= _quorumSize;
    }

    @Override
    public String toString() {
        return "PendingUpdate{" +
                "m_data=" + m_data +
                ", m_timestamp=" + m_timestamp +
                ", m_client=" + m_client +
                ", m_acks=" + m_acks +
                '}';
    }
}