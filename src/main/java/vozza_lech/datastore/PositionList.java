package vozza_lech.datastore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Optional;

public class PositionList {
    private final ArrayList<PersonOfInterest> m_people;

    public PositionList() {
        m_people = new ArrayList<>();
    }

    public Optional<PersonOfInterest> getPerson(int _index) {
        if(m_people.isEmpty() || m_people.size() < _index) {
            return Optional.empty();
        }

        return Optional.of(new PersonOfInterest(m_people.get(_index)));
    }

    /// Called at the beginning, no need for
    /// a version parameter
    public void addPerson(int _init_pos) {
        m_people.add(new PersonOfInterest(new UpdateTimestamp(), _init_pos));
    }

    public Optional<UpdateTimestamp> getLastUpdate() {
        if(m_people.isEmpty()) {
            return Optional.empty();
        }

        return m_people.stream()
                .map((_person) -> _person.last_update)
                .max(Comparator.naturalOrder());
    }
}
