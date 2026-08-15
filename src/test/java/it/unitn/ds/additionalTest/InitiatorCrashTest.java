package it.unitn.ds.additionalTest;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InitiatorCrashTest {
    @ParameterizedTest(name = "Update initiator crashes after receiveing broadcasted update => nodes {0}")
    @CsvSource({"5"})
    void initiatorCrashAfterRecvBroadcast(int n_nodes) throws InterruptedException {
        final int COORDINATOR_ID = 0;
        final TestsSystemWrapper sys = TestsCommons.createTestSystem(
                "initiatorCrash_" + n_nodes, n_nodes, COORDINATOR_ID);

        TestKit probe = new TestKit(sys.system);
        int targetReplicaID = 1;
        ActorRef write_client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID)), probe.getRef()),
                "write_client");
        ActorRef read_client = sys.system.actorOf(
                Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
                        Optional.of(sys.actors.get(targetReplicaID + 1)), probe.getRef()),
                "read_client");

        sys.actors.get(targetReplicaID)
                        .tell(new Crash(Crash.Type.Update, 2), Actor.noSender());

        write_client.tell(new AbstractClient.WriteRequest(
                TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE), Actor.noSender());


        AbstractClient.WriteResult wr = (AbstractClient.WriteResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys)),
                "WriteResult", msg -> msg instanceof AbstractClient.WriteResult);
        assertEquals(new AbstractClient.WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, COORDINATOR_ID), wr);

        Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

        read_client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());

        AbstractClient.ReadResult rr = (AbstractClient.ReadResult) probe.fishForMessage(
                Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
                "ReadResult", msg -> msg instanceof AbstractClient.ReadResult);
        assertEquals(new AbstractClient.ReadResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID + 1), rr);


        sys.system.terminate();
    }
}
