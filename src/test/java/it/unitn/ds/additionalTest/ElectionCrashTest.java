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
 * Crashes happening during the ELECTION protocol itself.
 */
class ElectionCrashTest {

    /**
     * A non-coordinator replica crashes right after forwarding its own candidate entry.
     * The ring must skip it (ElectionAckTimeout) and still complete.
     */
    @ParameterizedTest(name = "non-coordinator replica crashes mid-ring during election => nodes {0}")
    @CsvSource({ "5", "7" })
    void replicaCrashMidRingDuringElection(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int CRASHING_REPLICA_ID = 1;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "replicaCrashMidRing_" + n_nodes, n_nodes, COORDINATOR_ID);

        // Crash right after this replica forwards its 1st election message.
        sys.actors.get(CRASHING_REPLICA_ID).tell(
                new Crash(Crash.Type.Election, 1), Actor.noSender());

        // Immediate coordinator death triggers an organic election.
        sys.actors.get(COORDINATOR_ID).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());

        int targetReplicaID = n_nodes - 1;
        TestKit probe = new TestKit(sys.system);
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        client.tell(new AbstractClient.WriteRequest(
                TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE), Actor.noSender());

        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult", msg -> msg instanceof WriteResult);
        assertTrue(wr.success);

        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof ReadResult);
        assertEquals(TestsCommons.TEST_VALUE, rr.value);

        sys.system.terminate();
    }

    /**
     * The winner (highest id) crashes right after appending its own id in the election.
     * The candidate list still names it winner on the first lap, so
     * the ring spins without resolving until ElectionGlobalTimeout resets the election.
     */
    @ParameterizedTest(name = "would-be winner crashes before declaring itself elected => nodes {0}")
    @CsvSource({ "5", "7" })
    void wouldBeWinnerCrashesDuringElection(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int EXPECTED_WINNER_ID = n_nodes - 1; // highest id, natural tie-break winner

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "wouldBeWinnerCrash_" + n_nodes, n_nodes, COORDINATOR_ID);

        // Crash right after forwarding its own entry, i.e. before it can
        // ever see the ring complete and elect itself.
        sys.actors.get(EXPECTED_WINNER_ID).tell(
                new Crash(Crash.Type.Election, 1), Actor.noSender());

        sys.actors.get(COORDINATOR_ID).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());

        int targetReplicaID = n_nodes - 2; // still alive after both crashes
        TestKit probe = new TestKit(sys.system);
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        client.tell(new AbstractClient.WriteRequest(
                TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE), Actor.noSender());

        long window = TestsCommons.getElectionMaxDelay(sys) * 2 + TestsCommons.getMaxUpdateDelay(sys);
        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(window), "WriteResult", msg -> msg instanceof WriteResult);
        assertTrue(wr.success);

        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof ReadResult);
        assertEquals(TestsCommons.TEST_VALUE, rr.value);

        sys.system.terminate();
    }

    /**
     * The newly elected coordinator crashes on its very first heartbeat, right after taking over.
     * A second election must follow and hand off to the next surviving candidate.
     */
    @ParameterizedTest(name = "new coordinator crashes on its 1st heartbeat, forcing a 2nd election => nodes {0}")
    @CsvSource({ "5", "7" })
    void newCoordinatorCrashesShortlyAfterElection(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int FIRST_NEW_COORDINATOR_ID = n_nodes - 1; // highest id wins the 1st election

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "doubleElection_" + n_nodes, n_nodes, COORDINATOR_ID);

        // Crash as soon as it sends its very first heartbeat as coordinator.
        sys.actors.get(FIRST_NEW_COORDINATOR_ID).tell(
                new Crash(Crash.Type.Heartbeat, 1), Actor.noSender());

        sys.actors.get(COORDINATOR_ID).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());

        int targetReplicaID = n_nodes - 2; // still alive after both crashes
        TestKit probe = new TestKit(sys.system);
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        client.tell(new AbstractClient.WriteRequest(
                TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE), Actor.noSender());

        Thread.sleep(TestsCommons.getElectionMaxDelay(sys));

        long window = TestsCommons.getElectionMaxDelay(sys) * 2
                + TestsCommons.TEST_COORDINATOR_BEAT_INTERVAL
                + TestsCommons.getMaxUpdateDelay(sys);
        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(window), "WriteResult", msg -> msg instanceof WriteResult);
        assertTrue(wr.success);

        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof ReadResult);
        assertEquals(TestsCommons.TEST_VALUE, rr.value);

        sys.system.terminate();
    }

    /**
     * Two non-coordinator replicas crash at once during the same election. Tests consecutive ring skips.
     */
    @ParameterizedTest(name = "two non-coordinator replicas crash simultaneously during election => nodes {0}")
    @CsvSource({ "7" })
    void multipleReplicasCrashDuringElection(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final int CRASHING_REPLICA_ID_1 = 1;
        final int CRASHING_REPLICA_ID_2 = 2;

        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "multiReplicaCrash_" + n_nodes, n_nodes, COORDINATOR_ID);

        sys.actors.get(CRASHING_REPLICA_ID_1).tell(
                new Crash(Crash.Type.Now, 1), Actor.noSender());
        sys.actors.get(CRASHING_REPLICA_ID_2).tell(
                new Crash(Crash.Type.Now, 1), Actor.noSender());

        sys.actors.get(COORDINATOR_ID).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());

        int targetReplicaID = n_nodes - 1;
        TestKit probe = new TestKit(sys.system);
        ActorRef client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID)), probe.getRef()),
                "client");

        client.tell(new AbstractClient.WriteRequest(
                TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE), Actor.noSender());

        WriteResult wr = (WriteResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys) * 2 + TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult", msg -> msg instanceof WriteResult);
        assertTrue(wr.success);

        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
        ReadResult rr = (ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof ReadResult);
        assertEquals(TestsCommons.TEST_VALUE, rr.value);

        sys.system.terminate();
    }
}