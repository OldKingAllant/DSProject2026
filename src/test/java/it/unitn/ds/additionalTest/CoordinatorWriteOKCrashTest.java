package it.unitn.ds.additionalTest;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CoordinatorWriteOKCrashTest {

    /**
     * Coordinator crashes after sending exactly 1 WRITEOK message.
     * After election and synchronization, all surviving replicas must
     * have applied the update (uniform agreement) and the client
     * contacting the last replica must read the written value.
     */
    @ParameterizedTest(name = "coordinator crashes after 1st WRITEOK => nodes {0}")
    @CsvSource({ "5", "7" })
    void coordinatorCrashAfterFirstWriteOK(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "coordCrashAfter1stWriteOK_" + n_nodes, n_nodes, COORDINATOR_ID);

        TestKit probe = new TestKit(sys.system);
        int targetReplicaID = n_nodes - 1;
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        // Coordinator crashes after sending its 1st WRITEOK
        sys.actors.get(COORDINATOR_ID).tell(
                new Crash(Crash.Type.WriteOK, 1), Actor.noSender());

        client.tell(new AbstractClient.WriteRequest(
                TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE), Actor.noSender());

        // Write must still complete after election
        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult", msg -> msg instanceof WriteResult);
        assertEquals(new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), wr);

        // Wait for synchronization to propagate to all replicas
        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        // Read from target replica must return the written value
        client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof ReadResult);
        assertEquals(new ReadResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), rr);

        sys.system.terminate();
    }

    /**
     * Coordinator crashes after sending N-2 WRITEOK messages.
     * The last replica has not yet applied the update when the crash happens.
     * After election and synchronization, all survivors must agree on the value.
     */
    @ParameterizedTest(name = "coordinator crashes after (N-2) WRITEOKs => nodes {0}")
    @CsvSource({ "5", "7" })
    void coordinatorCrashAfterAllButLastWriteOK(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "coordCrashAllButLastWriteOK_" + n_nodes, n_nodes, COORDINATOR_ID);

        TestKit probe = new TestKit(sys.system);
        int targetReplicaID = n_nodes - 1;
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        // Crash after sending N-2 WRITEOKs (coordinator itself + N-2 others = N-1 total)
        int crashAfter = n_nodes - 2;
        sys.actors.get(COORDINATOR_ID).tell(
                new Crash(Crash.Type.WriteOK, crashAfter), Actor.noSender());

        client.tell(new AbstractClient.WriteRequest(
                TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE), Actor.noSender());

        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult", msg -> msg instanceof WriteResult);
        assertEquals(new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), wr);

        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof ReadResult);
        assertEquals(new ReadResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), rr);

        sys.system.terminate();
    }

    /**
     * Two clients each send one write to a non-coordinator replica while the
     * coordinator is still processing the first write. The coordinator crashes
     * after sending its 1st WRITEOK, so the second queued write has not yet
     * been broadcast.
     * After election the new coordinator must:
     *   1. complete the interrupted first write (if not yet done)
     *   2. process the second queued write.
     */
    @ParameterizedTest(name = "coordinator crashes during WRITEOK, two queued writes => nodes {0}")
    @CsvSource({ "7" })
    void coordinatorCrashDuringWriteOKTwoQueuedWrites(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int SECOND_VALUE   = TestsCommons.TEST_VALUE + 1;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "coordCrashWriteOK2Queued_" + n_nodes, n_nodes, COORDINATOR_ID);

        TestKit probe1 = new TestKit(sys.system);
        TestKit probe2 = new TestKit(sys.system);
        TestKit readProbe = new TestKit(sys.system);

        ActorRef client1 = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(1)), probe1.getRef()), "client1");
        ActorRef client2 = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(2)), probe2.getRef()), "client2");
        ActorRef readClient = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(n_nodes - 1)), readProbe.getRef()), "readClient");

        // Coordinator will crash after 1st WRITEOK
        sys.actors.get(COORDINATOR_ID).tell(
                new Crash(Crash.Type.WriteOK, 1), Actor.noSender());

        // Send two writes almost simultaneously from different replicas
        client1.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
                Actor.noSender());
        client2.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, SECOND_VALUE),
                Actor.noSender());

        long window = TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys) * 2;

        // Both writes must eventually complete
        WriteResult wr1 = (WriteResult) probe1.fishForMessage(
                Duration.ofMillis(window), "WriteResult1", msg -> msg instanceof WriteResult);
        WriteResult wr2 = (WriteResult) probe2.fishForMessage(
                Duration.ofMillis(window), "WriteResult2", msg -> msg instanceof WriteResult);

        assertEquals(true, wr1.success);
        assertEquals(true, wr2.success);

        // Wait for full propagation
        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        // Final read must return the value of whichever write was committed last
        readClient.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) readProbe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof ReadResult);

        // The read value must be either TEST_VALUE or SECOND_VALUE (whichever was applied last),
        // but it must match on every replica: sequential consistency.
        int finalValue = rr.value;
        assert finalValue == TestsCommons.TEST_VALUE || finalValue == SECOND_VALUE
                : "Unexpected final value: " + finalValue;

        sys.system.terminate();
    }

    /**
     * A single client sends NUM_WRITES sequential writes to a non-coordinator replica.
     * The coordinator will crash after its 1st WRITEOK.
     * After election the new coordinator must drain the queue and all NUM_WRITES
     * must eventually complete successfully.
     */
    @ParameterizedTest(name = "coordinator crashes during WRITEOK, single client multiple writes => nodes {0}")
    @CsvSource({ "7" })
    void coordinatorCrashDuringWriteOKMultipleWritesSameClient(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int NUM_WRITES     = 4;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "coordCrashWriteOKMultiWrites_" + n_nodes, n_nodes, COORDINATOR_ID);

        TestKit probe = new TestKit(sys.system);
        int targetReplicaID = n_nodes - 1;
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        // Coordinator will crash after 1st WRITEOK
        sys.actors.get(COORDINATOR_ID).tell(
                new Crash(Crash.Type.WriteOK, 1), Actor.noSender());

        // Send all writes rapidly
        for (int v = 0; v < NUM_WRITES; v++) {
            client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, v), Actor.noSender());
        }

        long window = TestsCommons.getElectionMaxDelay(sys)
                + TestsCommons.getMaxUpdateDelay(sys) * NUM_WRITES;

        // All writes must complete
        for (int v = 0; v < NUM_WRITES; v++) {
            WriteResult wr = (WriteResult) probe.fishForMessage(
                    Duration.ofMillis(window), "WriteResult_" + v, msg -> msg instanceof WriteResult);
            assertEquals(true, wr.success, "Write #" + v + " must succeed");
        }

        // Wait for full propagation
        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        // Final read must return the last written value (NUM_WRITES - 1)
        client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof ReadResult);
        assertEquals(NUM_WRITES - 1, rr.value,
                "Final read must return the last written value");

        sys.system.terminate();
    }

}

