package vozza_lech.datastore;

public class PersonOfInterest {
    public UpdateTimestamp last_update;
    public int position;

    public PersonOfInterest(UpdateTimestamp _last_update, int _position) {
        last_update = new UpdateTimestamp(_last_update);
        position = _position;
    }

    public PersonOfInterest(PersonOfInterest _other) {
        last_update = _other.last_update;
        position = _other.position;
    }
}
