package com.mio.swarch;

import MIO.*;
import com.zeroc.Ice.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Exception;

/**
 * Servidor que maneja el sistema de streaming en tiempo real.
 * Incluye:
 * - StreamBroker: Pub/Sub de datagramas
 */
public class StreamingServer {
    private static final Logger logger = LoggerFactory.getLogger(StreamingServer.class);

    public static void main(String[] args) {
        String masterHost = System.getenv("MASTER_HOST");
        if (masterHost == null || masterHost.isEmpty()) {
            masterHost = "192.168.131.101";
        }

        try (Communicator communicator = Util.initialize(args)) {

            // Crear adapter en puerto 11000
            ObjectAdapter adapter = communicator.createObjectAdapterWithEndpoints(
                    "StreamingAdapter", "tcp -h " + masterHost + " -p 11000"
            );

            // Crear broker
            StreamBrokerService broker = new StreamBrokerService();

            // Registrar broker
            adapter.add(broker, Util.stringToIdentity("StreamBroker"));

            adapter.activate();

            logger.info("════════════════════════════════════════════════════════");
            logger.info("Streaming Server iniciado en tcp://{}:11000", masterHost);
            logger.info("════════════════════════════════════════════════════════");
            logger.info("Servicios disponibles:");
            logger.info("  - StreamBroker (Pub/Sub)");
            logger.info("");
            logger.info("Servidor listo para recibir conexiones.");
            logger.info("════════════════════════════════════════════════════════");

            // Shutdown hook para limpieza
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Deteniendo servidor de streaming...");
                broker.shutdown();
            }));

            communicator.waitForShutdown();

        } catch (Exception e) {
            logger.error("Error en Streaming Server", e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}