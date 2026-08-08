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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coordinator crash during BROADCAST (Crash.Type.Update),
 * Crash before the quorum is reached, no WRITEOK has been sent.
 */

class CoordinatorBroadcastCrashTest {

    /**
     * Coordinator crashes right after starting the broadcast of the only write.
     * No ACK was ever processed, so no quorum/WRITEOK was ever sent.
     */
    @ParameterizedTest(name = "coordinator crashes right after 1st UPDATE broadcast => nodes {0}")
    @CsvSource({ "5", "7" })
    void coordinatorCrashAfterFirstUpdateBroadcast(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "coordCrashAfter1stBroadcast_" + n_nodes, n_nodes, COORDINATOR_ID);

        TestKit probe = new TestKit(sys.system);
        int targetReplicaID = n_nodes - 1;
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        // Crash right after processing the 1st write request (broadcast just started).
        sys.actors.get(COORDINATOR_ID).tell(
                new Crash(Crash.Type.Update, 1), Actor.noSender());

        client.tell(new AbstractClient.WriteRequest(
                TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE), Actor.noSender());

        // The write must still complete after an election.
        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult", msg -> msg instanceof WriteResult);
        assertEquals(new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), wr);

        // Wait for synchronization to propagate to all replicas.
        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof ReadResult);
        assertEquals(new ReadResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), rr);

        sys.system.terminate();
    }

    /**
     * Coordinator processes the 1st write fine, then crashes right as it accepts
     * the 2nd request (still queued or with its broadcast just started).
     */
    @ParameterizedTest(name = "coordinator crashes right after 2nd UPDATE broadcast => nodes {0}")
    @CsvSource({ "5", "7" })
    void coordinatorCrashAfterSecondUpdateBroadcast(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int INDEX_1 = 0;
        final int VALUE_1 = TestsCommons.TEST_VALUE;
        final int INDEX_2 = 1;
        final int VALUE_2 = TestsCommons.TEST_VALUE + 10;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "coordCrashAfter2ndBroadcast_" + n_nodes, n_nodes, COORDINATOR_ID);

        TestKit probe = new TestKit(sys.system);
        int targetReplicaID = n_nodes - 1;
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        sys.actors.get(COORDINATOR_ID).tell(
                new Crash(Crash.Type.Update, 2), Actor.noSender());

        client.tell(new AbstractClient.WriteRequest(INDEX_1, VALUE_1), Actor.noSender());
        client.tell(new AbstractClient.WriteRequest(INDEX_2, VALUE_2), Actor.noSender());

        long window = TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys) * 2;

        WriteResult wr1 = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(window), "WriteResult1", msg -> msg instanceof WriteResult);
        WriteResult wr2 = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(window), "WriteResult2", msg -> msg instanceof WriteResult);
        assertTrue(wr1.success);
        assertTrue(wr2.success);

        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        client.tell(new AbstractClient.ReadRequest(INDEX_1), Actor.noSender());
        ReadResult rr1 = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult1", msg -> msg instanceof ReadResult);
        assertEquals(new ReadResult(true, INDEX_1, VALUE_1, targetReplicaID), rr1);

        client.tell(new AbstractClient.ReadRequest(INDEX_2), Actor.noSender());
        ReadResult rr2 = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult2", msg -> msg instanceof ReadResult);
        assertEquals(new ReadResult(true, INDEX_2, VALUE_2, targetReplicaID), rr2);

        sys.system.terminate();
    }

    /**
     * Two clients write to the same index almost simultaneously via different
     * replicas; coordinator crashes as soon as it starts the first broadcast.
     */
    @ParameterizedTest(name = "coordinator crashes during broadcast, two queued writes => nodes {0}")
    @CsvSource({ "7" })
    void coordinatorCrashDuringBroadcastTwoQueuedWrites(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int SECOND_VALUE = TestsCommons.TEST_VALUE + 1;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "coordCrashBroadcast2Queued_" + n_nodes, n_nodes, COORDINATOR_ID);

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

        // Crash as soon as the first broadcast starts, before any ACK.
        sys.actors.get(COORDINATOR_ID).tell(
                new Crash(Crash.Type.Update, 1), Actor.noSender());

        client1.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
                Actor.noSender());
        client2.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, SECOND_VALUE),
                Actor.noSender());

        long window = TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys) * 2;

        WriteResult wr1 = (WriteResult) probe1.fishForMessage(
                Duration.ofMillis(window), "WriteResult1", msg -> msg instanceof WriteResult);
        WriteResult wr2 = (WriteResult) probe2.fishForMessage(
                Duration.ofMillis(window), "WriteResult2", msg -> msg instanceof WriteResult);
        assertTrue(wr1.success);
        assertTrue(wr2.success);

        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        // Same index for both: whichever applied last wins, but must be
        // consistent across every replica (sequential consistency).
        readClient.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) readProbe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof ReadResult);

        int finalValue = rr.value;
        assertTrue(finalValue == TestsCommons.TEST_VALUE || finalValue == SECOND_VALUE,
                "Unexpected final value: " + finalValue);

        sys.system.terminate();
    }

    /**
     * A single client fires NUM_WRITES writes in quick succession; coordinator
     * crashes as soon as the first broadcast starts.
     */
    @ParameterizedTest(name = "coordinator crashes during broadcast, single client multiple writes => nodes {0}")
    @CsvSource({ "7" })
    void coordinatorCrashDuringBroadcastMultipleWritesSameClient(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int NUM_WRITES = 4;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "coordCrashBroadcastMultiWrites_" + n_nodes, n_nodes, COORDINATOR_ID);

        TestKit probe = new TestKit(sys.system);
        int targetReplicaID = n_nodes - 1;
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        sys.actors.get(COORDINATOR_ID).tell(
                new Crash(Crash.Type.Update, 1), Actor.noSender());

        for (int v = 0; v < NUM_WRITES; v++) {
            client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, v), Actor.noSender());
        }

        long window = TestsCommons.getElectionMaxDelay(sys)
                + TestsCommons.getMaxUpdateDelay(sys) * NUM_WRITES;

        for (int v = 0; v < NUM_WRITES; v++) {
            WriteResult wr = (WriteResult) probe.fishForMessage(
                    Duration.ofMillis(window), "WriteResult_" + v, msg -> msg instanceof WriteResult);
            assertTrue(wr.success, "Write #" + v + " must succeed");
        }

        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof ReadResult);
        assertEquals(NUM_WRITES - 1, rr.value,
                "Final read must return the last written value");

        sys.system.terminate();
    }

    /**
     * Mirror of the above from a non-coordinator replica's perspective: it ACKs
     * the UPDATE then crashes before ever applying it. Coordinator stays alive.
     */
    @ParameterizedTest(name = "non-coordinator replica crashes right after ACKing UPDATE => nodes {0}")
    @CsvSource({ "5", "7" })
    void nonCoordinatorReplicaCrashAfterAckingUpdate(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int CRASHING_REPLICA_ID = 1;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "nonCoordCrashAfterAck_" + n_nodes, n_nodes, COORDINATOR_ID);

        TestKit probe = new TestKit(sys.system);
        int targetReplicaID = n_nodes - 1;
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        // Non-coordinator replica crashes right after receiving/ACKing its 1st UPDATE.
        sys.actors.get(CRASHING_REPLICA_ID).tell(
                new Crash(Crash.Type.Update, 1), Actor.noSender());

        client.tell(new AbstractClient.WriteRequest(
                TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE), Actor.noSender());

        // No election needed: coordinator is alive, quorum still forms.
        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult", msg -> msg instanceof WriteResult);
        assertEquals(new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), wr);

        Thread.sleep(TestsCommons.getBaseMaxUpdateDelay(sys));

        client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof ReadResult);
        assertEquals(new ReadResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), rr);

        sys.system.terminate();
    }
}
