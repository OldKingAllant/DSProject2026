package vozza_lech.datastore;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Optional;

public class UpdateLog {
    public static class UpdatePair implements Serializable {
        public Update data;
        public UpdateTimestamp timestamp;
    }

    private final ArrayList<UpdatePair> m_log;

    public UpdateLog() {
        m_log = new ArrayList<>();
    }

    public boolean addLog(Update _data, UpdateTimestamp _timestamp) {
        if(!m_log.isEmpty()) {
            var last_log = m_log.get(m_log.size() - 1);
            if(last_log.timestamp.compareTo(_timestamp) > 0) {
                return false;
            }
        }

        var update = new UpdatePair();
        update.timestamp = _timestamp;
        update.data = _data;
        m_log.add(update);
        return true;
    }

    public Optional<UpdateTimestamp> getLastLogTimestamp() {
        if(m_log.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new UpdateTimestamp(m_log.get(m_log.size() - 1).timestamp));
    }

    public ArrayList<UpdatePair> getLogsAfter(UpdateTimestamp _timestamp) {
        return new ArrayList<>( m_log.stream()
                .filter((_log) -> _log.timestamp.compareTo(_timestamp) > 0)
                .toList() );
    }
}
