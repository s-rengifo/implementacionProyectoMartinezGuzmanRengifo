package com.mio.swarch;

import MIO.*;
import com.zeroc.Ice.Current;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Broker de mensajes para streaming de datagramas en tiempo real.
 * Implementa patrón Publish/Subscribe.
 *
 * Funcionamiento:
 * 1. Publishers (buses simulados) publican datagramas al broker
 * 2. Subscribers (procesadores) reciben datagramas automáticamente
 * 3. Broker distribuye mensajes a todos los subscribers activos
 */
public class StreamBrokerService implements StreamBroker {
    private static final Logger logger = LoggerFactory.getLogger(StreamBrokerService.class);

    // Subscribers activos (thread-safe)
    private final Set<StreamSubscriberPrx> subscribers = ConcurrentHashMap.newKeySet();

    // Estadísticas
    private volatile long totalDatagramsPublished = 0;
    private volatile long totalDatagramsDelivered = 0;
    private volatile boolean isActive = true;

    // Buffer para evitar sobrecarga (opcional)
    private final BlockingQueue<Datagram> messageQueue = new LinkedBlockingQueue<>(10000);
    private final ExecutorService distributionExecutor = Executors.newFixedThreadPool(4);

    public StreamBrokerService() {
        // Iniciar thread de distribución
        startDistributionThread();
    }

    /**
     * Publica un datagrama a todos los subscribers.
     * Llamado por buses simulados.
     */
    @Override
    public void publish(Datagram datagram, Current current) {
        if (!isActive) {
            logger.warn("Broker inactivo, mensaje rechazado");
            return;
        }

        try {
            // Agregar a cola de distribución
            boolean added = messageQueue.offer(datagram, 100, TimeUnit.MILLISECONDS);

            if (added) {
                totalDatagramsPublished++;

                if (totalDatagramsPublished % 1000 == 0) {
                    logger.info("Broker: {} datagramas publicados, {} subscribers activos",
                            totalDatagramsPublished, subscribers.size());
                }
            } else {
                logger.warn("Cola de mensajes llena, datagrama descartado");
            }

        } catch (InterruptedException e) {
            logger.error("Error publicando datagrama", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Publica múltiples datagramas en lote (más eficiente).
     */
    @Override
    public void publishBatch(Datagram[] datagrams, Current current) {
        if (!isActive) return;

        for (Datagram datagram : datagrams) {
            try {
                messageQueue.offer(datagram, 10, TimeUnit.MILLISECONDS);
                totalDatagramsPublished++;
            } catch (InterruptedException e) {
                logger.error("Error en batch publish", e);
                Thread.currentThread().interrupt();
                break;
            }
        }

        logger.debug("Batch publicado: {} datagramas", datagrams.length);
    }

    /**
     * Registra un subscriber para recibir datagramas.
     */
    @Override
    public void subscribe(StreamSubscriberPrx subscriber, Current current) {
        if (subscriber == null) {
            logger.warn("Intento de subscribe con proxy null");
            return;
        }

        boolean added = subscribers.add(subscriber);

        if (added) {
            logger.info("✓ Nuevo subscriber registrado. Total: {}", subscribers.size());
        } else {
            logger.debug("Subscriber ya estaba registrado");
        }
    }

    /**
     * Desregistra un subscriber.
     */
    @Override
    public void unsubscribe(StreamSubscriberPrx subscriber, Current current) {
        boolean removed = subscribers.remove(subscriber);

        if (removed) {
            logger.info("✓ Subscriber removido. Total: {}", subscribers.size());
        }
    }

    /**
     * Obtiene estadísticas del broker.
     */
    @Override
    public String getStats(Current current) {
        return String.format(
                "╔════════════════════════════════════════╗\n" +
                        "║   Estadísticas del Broker Streaming   ║\n" +
                        "╠════════════════════════════════════════╣\n" +
                        "║ Subscribers activos:     %6d       ║\n" +
                        "║ Datagramas publicados:   %,10d   ║\n" +
                        "║ Datagramas entregados:   %,10d   ║\n" +
                        "║ Cola pendiente:          %6d       ║\n" +
                        "║ Estado:                  %-10s ║\n" +
                        "╚════════════════════════════════════════╝",
                subscribers.size(),
                totalDatagramsPublished,
                totalDatagramsDelivered,
                messageQueue.size(),
                isActive ? "ACTIVO" : "INACTIVO"
        );
    }

    /**
     * Thread que distribuye mensajes de la cola a los subscribers.
     */
    private void startDistributionThread() {
        Thread distributionThread = new Thread(() -> {
            logger.info("Thread de distribución iniciado");

            while (isActive) {
                try {
                    // Obtener mensaje de la cola (bloqueante)
                    Datagram datagram = messageQueue.poll(1, TimeUnit.SECONDS);

                    if (datagram != null) {
                        distributeToSubscribers(datagram);
                    }

                } catch (InterruptedException e) {
                    logger.warn("Thread de distribución interrumpido");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error en distribución", e);
                }
            }

            logger.info("Thread de distribución detenido");
        }, "BrokerDistribution");

        distributionThread.setDaemon(true);
        distributionThread.start();
    }

    /**
     * Distribuye un datagrama a todos los subscribers activos.
     */
    private void distributeToSubscribers(Datagram datagram) {
        if (subscribers.isEmpty()) {
            return;
        }

        // Crear lista de subscribers para iterar (evitar ConcurrentModificationException)
        List<StreamSubscriberPrx> activeSubscribers = new ArrayList<>(subscribers);

        // Enviar a cada subscriber en paralelo
        for (StreamSubscriberPrx subscriber : activeSubscribers) {
            distributionExecutor.submit(() -> {
                try {
                    subscriber.onDatagramReceived(datagram);
                    totalDatagramsDelivered++;

                } catch (Exception e) {
                    // Subscriber inaccesible, remover
                    logger.warn("Subscriber no responde, removiendo: {}", e.getMessage());
                    subscribers.remove(subscriber);
                }
            });
        }
    }

    /**
     * Detiene el broker.
     */
    public void shutdown() {
        logger.info("Deteniendo broker...");
        isActive = false;
        distributionExecutor.shutdown();

        try {
            distributionExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Timeout esperando shutdown del executor");
            distributionExecutor.shutdownNow();
        }

        logger.info("✓ Broker detenido");
    }
}