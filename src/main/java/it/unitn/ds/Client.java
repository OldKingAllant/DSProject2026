package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.IllegalActorStateException;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class Client extends AbstractClient {

    private final int READ_TIMEOUT_MS = 1000;
    private final int WRITE_TIMEOUT_MS = 1000 * 5;

    /**
    State enum used to track if the test framework
     decides to attempt another request before the previous one
     completes
    */
    private enum State {
        IDLE,
        READ,
        WRITE
    }

    private State                 m_state;
    private Optional<Cancellable> m_cancellable_handle;

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
        m_state = State.IDLE;
        m_cancellable_handle = Optional.empty();
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

    public void readTimeout(ReadTimeout _timeout) {
        callbackOnReadTimeout(_timeout);
        m_cancellable_handle = Optional.empty();
        m_state = State.IDLE;
    }

    public void writeTimeout(WriteTimeout _timeout) {
        callbackOnWriteTimeout(_timeout);
        m_cancellable_handle = Optional.empty();
        m_state = State.IDLE;
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        if(State.IDLE != m_state) {
            throw new IllegalActorStateException("Requesting READ operation while not idle");
        }
        log(String.format("requesting READ (%d) to %s", index, replica.path().name()));
        var read_req = new ReadRequest(index);
        replica.tell(read_req, getSelf());

        m_cancellable_handle = Optional.of( getContext().getSystem()
                .getScheduler().scheduleOnce(
                        Duration.create(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                        getSelf(),
                        new ReadTimeout(getSelf(), replica, index),
                        getContext().getDispatcher(),
                        getSelf()
                ) );
        m_state = State.READ;
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        if(State.IDLE != m_state) {
            throw new IllegalActorStateException("Requesting WRITE operation while not idle");
        }
        log(String.format("requesting WRITE (%d, %d) to %s", index, value, replica.path().name()));
        var write_req = new WriteRequest(index, value);
        replica.tell(write_req, getSelf());

        m_cancellable_handle = Optional.of( getContext().getSystem()
                .getScheduler().scheduleOnce(
                        Duration.create(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                        getSelf(),
                        new WriteTimeout(getSelf(), replica, index, value),
                        getContext().getDispatcher(),
                        getSelf()
                ) );
        m_state = State.WRITE;
    }

    public void receiveReadResult(ReadResult _result) {
        callbackOnReadResult(_result);
        m_cancellable_handle.ifPresent(Cancellable::cancel);
        m_cancellable_handle = Optional.empty();
        m_state = State.IDLE;
    }

    public void receiveWriteResult(WriteResult _result) {
        callbackOnWriteResult(_result);
        m_cancellable_handle.ifPresent(Cancellable::cancel);
        m_cancellable_handle = Optional.empty();
        m_state = State.IDLE;
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
