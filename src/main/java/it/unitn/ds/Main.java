package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        final int N_REPLICAS = 3;
        final int COORDINATOR_ID = 0;
        final ActorSystem system = ActorSystem.create("TestMain");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                system.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i
                )
            );
        }

        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }

        // TODO: Create your clients
        
        // TODO: Implement your main logic

        try {
            // --- Create clients ---
            ActorRef client0 = system.actorOf(
                    Client.props(1000, 5000, Optional.of(replicas.get(0))),
                    "Client_0"
            );
            ActorRef client1 = system.actorOf(
                    Client.props(1000, 5000, Optional.of(replicas.get(1))),
                    "Client_1"
            );
            ActorRef client2 = system.actorOf(
                    Client.props(1000, 5000, Optional.of(replicas.get(2))),
                    "Client_2"
            );

            // --- Scenario ---

            // WRITE via coordinator: set position[0] = 42
            Logger.log("[Main] Client0 -> WRITE index=0, value=42 (via replica 0, coordinator)");
            client0.tell(new AbstractClient.WriteRequest(0, 42), ActorRef.noSender());
            Thread.sleep(500);

            // READ from replica 0: should return 42
            Logger.log("[Main] Client0 -> READ index=0 (via replica 0)");
            client0.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
            Thread.sleep(300);

            // WRITE via non-coordinator: set position[1] = 99
            Logger.log("[Main] Client1 -> WRITE index=1, value=99 (via replica 1, forwarded to coordinator)");
            client1.tell(new AbstractClient.WriteRequest(1, 99), ActorRef.noSender());
            Thread.sleep(500);

            // READ from replica 1: should return 99
            Logger.log("[Main] Client1 -> READ index=1 (via replica 1)");
            client1.tell(new AbstractClient.ReadRequest(1), ActorRef.noSender());
            Thread.sleep(300);

            // Two concurrent WRITEs to index=2 from different clients: tests total order
            Logger.log("[Main] Client0 -> WRITE index=2, value=10");
            client0.tell(new AbstractClient.WriteRequest(2, 10), ActorRef.noSender());
            Logger.log("[Main] Client1 -> WRITE index=2, value=20");
            client1.tell(new AbstractClient.WriteRequest(2, 20), ActorRef.noSender());
            Thread.sleep(1000);

            // READ from replica 2: value must be consistent (either 10 or 20, same on all replicas)
            Logger.log("[Main] Client2 -> READ index=2 (via replica 2, checking total order)");
            client2.tell(new AbstractClient.ReadRequest(2), ActorRef.noSender());
            Thread.sleep(500);

        } catch (InterruptedException ignored) {}

        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }


}
