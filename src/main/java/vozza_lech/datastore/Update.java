package vozza_lech.datastore;

import java.io.Serializable;

public class Update implements Serializable {
    private final int m_index;
    private final int m_position;

    public Update(int _index, int _position) {
        m_index = _index;
        m_position = _position;
    }

    public Update(Update _other) {
        m_index = _other.m_index;
        m_position = _other.m_position;
    }

    public int getIndex() {
        return m_index;
    }

    public int getPosition() {
        return m_position;
    }
}
