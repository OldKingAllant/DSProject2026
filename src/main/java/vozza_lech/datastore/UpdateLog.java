package vozza_lech.datastore;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Optional;

public class UpdateLog {
    public static class UpdateInfo implements Serializable {
        public final Update data;
        public final UpdateTimestamp timestamp;
        public final ActorRef initiator;

        public UpdateInfo(Update _data, UpdateTimestamp _timestamp, ActorRef _initiator) {
            this.data = _data;
            this.timestamp = _timestamp;
            this.initiator = _initiator;
        }
    }

    private final ArrayList<UpdateInfo> m_log;

    public UpdateLog() {
        m_log = new ArrayList<>();
    }

    public boolean contains(UpdateTimestamp _timestamp) {
        return m_log.stream().anyMatch(p -> p.timestamp.equals(_timestamp));
    }

    public Optional<UpdateInfo> getLog(UpdateTimestamp _timestamp) {
        return m_log.stream()
                .filter(p -> p.timestamp.equals(_timestamp))
                .findFirst();
    }

    /**
     * Appends an update at the end of the log.
     * Assumes strict in-order insertion
     * Returns false if _timestamp does not come after the last logged entry.
     */
    public boolean addLog(Update _data, UpdateTimestamp _timestamp, ActorRef _initiator) {
        if (!m_log.isEmpty()) {
            var last_log = m_log.get(m_log.size() - 1);
            if (last_log.timestamp.compareTo(_timestamp) > 0) {
                return false;
            }
        }

        m_log.add(new UpdateInfo(_data, _timestamp, _initiator));
        return true;
    }

    /**
     * Inserts an update in the correct sorted position if it is not
     * already present. Used during synchronization, where updates may arrive out of order.
     * Returns false if there is already one with the same timestamp.
     */
    public boolean addLogIfAbsent(Update _data, UpdateTimestamp _timestamp, ActorRef _initiator) {
        if (contains(_timestamp)) {
            return false;
        }

        int insertIndex = 0;
        while (insertIndex < m_log.size()
                && m_log.get(insertIndex).timestamp.compareTo(_timestamp) < 0) {
            insertIndex++;
        }

        m_log.add(insertIndex, new UpdateInfo(_data, _timestamp, _initiator));
        return true;
    }

    public Optional<UpdateTimestamp> getLastLogTimestamp() {
        if(m_log.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new UpdateTimestamp(m_log.get(m_log.size() - 1).timestamp));
    }

    public ArrayList<UpdateInfo> getLogsAfter(UpdateTimestamp _timestamp) {
        return new ArrayList<>( m_log.stream()
                .filter((_log) -> _log.timestamp.compareTo(_timestamp) > 0)
                .toList() );
    }
}
