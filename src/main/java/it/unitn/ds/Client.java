package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.IllegalActorStateException;
import akka.actor.Props;
import scala.Int;
import scala.concurrent.duration.Duration;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class Client extends AbstractClient {

    private final int READ_TIMEOUT_MS = 1000;
    private final int WRITE_TIMEOUT_MS = 1000 * 5;

    /**
    State enum used to track if the test framework
     decides to attempt another request before the previous one
     completes
    */
    /*private enum State {
        IDLE,
        READ,
        WRITE
    }*/

    private record Pair<K, V>(K key, V value) {}

    // private State                 m_state;
    private List<ReadRequest>     m_read_requests;
    private List<WriteRequest>    m_write_requests;
    private List<Cancellable>     m_read_timeout_events;
    private List<Cancellable>     m_write_timeout_events;
    private long                  m_read_timeout;
    private long                  m_write_timeout;

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);

        m_read_timeout = readTimeoutDelay;
        m_write_timeout = writeTimeoutDelay;

        m_read_requests = new LinkedList<>();
        m_write_requests = new LinkedList<>();

        m_read_timeout_events = new LinkedList<>();
        m_write_timeout_events = new LinkedList<>();
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    /// In case a timeout happens: no need to retry,
    /// the project specification does not say anything about it

    private boolean removeRead(int _rindex) {
        var index_req = IntStream.range(0, m_read_requests.size())
                .mapToObj((_index) -> new Pair<>(_index, m_read_requests.get(_index)))
                .filter((_index_req) -> _index_req.value().index == _rindex)
                .findFirst();

        if(index_req.isPresent()) {
            var index = index_req.get().key();
            m_read_timeout_events.remove(index.intValue()).cancel();
            m_read_requests.remove(index.intValue());
            return true;
        }

        return false;
    }

    private boolean removeWrite(int _windex, int _value) {
        var index_req = IntStream.range(0, m_write_requests.size())
                .mapToObj((_index) -> new Pair<>(_index, m_write_requests.get(_index)))
                .filter((_index_req) ->
                        _index_req.value().index == _windex && _index_req.value().value == _value)
                .findFirst();

        if(index_req.isPresent()) {
            var index = index_req.get().key();
            m_write_timeout_events.remove(index.intValue()).cancel();
            m_write_requests.remove(index.intValue());
            return true;
        }

        return false;
    }

    public void readTimeout(ReadTimeout _timeout) {
        callbackOnReadTimeout(_timeout);
        if(!removeRead(_timeout.index)) {
            debug("client received unexpected READ timeout");
        }
    }

    public void writeTimeout(WriteTimeout _timeout) {
        callbackOnWriteTimeout(_timeout);
        if(!removeWrite(_timeout.index, _timeout.value)) {
            debug("client received unexpected WRITE timeout");
        }
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        log(String.format("requesting READ (%d) to %s", index, replica.path().name()));
        var read_req = new ReadRequest(index);
        replica.tell(read_req, getSelf());

        m_read_requests.add(read_req);
        m_read_timeout_events.add(
                getContext().getSystem()
                .getScheduler().scheduleOnce(
                        Duration.create(m_read_timeout, TimeUnit.MILLISECONDS),
                        getSelf(),
                        new ReadTimeout(getSelf(), replica, index),
                        getContext().getDispatcher(),
                        getSelf()
                ));
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        log(String.format("requesting WRITE (%d, %d) to %s", index, value, replica.path().name()));
        var write_req = new WriteRequest(index, value);
        replica.tell(write_req, getSelf());

        m_write_requests.add(write_req);
        m_write_timeout_events.add(
                getContext().getSystem()
                .getScheduler().scheduleOnce(
                        Duration.create(m_write_timeout, TimeUnit.MILLISECONDS),
                        getSelf(),
                        new WriteTimeout(getSelf(), replica, index, value),
                        getContext().getDispatcher(),
                        getSelf()
                ));
    }

    public void receiveReadResult(ReadResult _result) {
        callbackOnReadResult(_result);
        if(!removeRead(_result.index)) {
            debug("client received unexpected READ result");
        }
    }

    public void receiveWriteResult(WriteResult _result) {
        callbackOnWriteResult(_result);
        if(!removeWrite(_result.index, _result.value)) {
            debug("client received unexpected WRITE result");
        }
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ReadResult.class, this::receiveReadResult)
                .match(WriteResult.class, this::receiveWriteResult)
                .match(ReadTimeout.class, this::readTimeout)
                .match(WriteTimeout.class, this::writeTimeout)
                .build();
    }

}
