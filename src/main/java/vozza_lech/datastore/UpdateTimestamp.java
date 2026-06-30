package vozza_lech.datastore;

import java.io.Serializable;

public class UpdateTimestamp implements Serializable, Comparable<UpdateTimestamp> {
    private final int m_epoch;
    private final int m_sn;

    public UpdateTimestamp() {
        m_epoch = 0;
        m_sn = 0;
    }

    public UpdateTimestamp(int _epoch, int _sn) {
        m_epoch = _epoch;
        m_sn = _sn;
    }

    public UpdateTimestamp(UpdateTimestamp _other) {
        m_epoch = _other.m_epoch;;
        m_sn = _other.m_sn;
    }

    public int getEpoch() {
        return this.m_epoch;
    }

    public int getSn() {
        return this.m_sn;
    }

    @Override
    public int hashCode() {
        return (m_epoch + " " + m_sn).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof UpdateTimestamp)) {
            return false;
        }
        UpdateTimestamp other = (UpdateTimestamp) obj;
        return this.m_epoch == other.m_epoch && this.m_sn == other.m_sn;
    }

    @Override
    public int compareTo(UpdateTimestamp o) {
        if(this.m_epoch == o.m_epoch) {
            return Integer.compare(this.m_sn, o.m_sn);
        }
        return Integer.compare(this.m_epoch, o.m_epoch);
    }
}
