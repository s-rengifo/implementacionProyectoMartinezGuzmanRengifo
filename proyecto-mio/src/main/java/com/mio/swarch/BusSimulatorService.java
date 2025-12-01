package com.mio.swarch;

import MIO.*;
import com.zeroc.Ice.Current;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simula buses del SITM-MIO publicando datagramas en tiempo real.
 * Lee datos de archivo y los envía al broker con delays configurables.
 */
public class BusSimulatorService implements BusSimulator {
    private static final Logger logger = LoggerFactory.getLogger(BusSimulatorService.class);

    private final String simulatorId;
    private final StreamBrokerPrx broker;

    private volatile boolean isStreaming = false;
    private ExecutorService simulationExecutor;

    private AtomicLong totalDatagramsSent = new AtomicLong(0);
    private volatile long simulationStartTime = 0;

    public BusSimulatorService(String simulatorId, StreamBrokerPrx broker) {
        this.simulatorId = simulatorId;
        this.broker = broker;
        logger.info("✓ BusSimulator {} creado", simulatorId);
    }

    /**
     * Inicia la simulación de streaming desde un archivo.
     *
     * @param dataFilePath Archivo CSV con datagramas
     * @param delayMs Delay entre envíos (0 = máxima velocidad)
     * @param batchSize Número de datagramas por batch (1 = individual)
     */
    @Override
    public void startStreaming(String dataFilePath, int delayMs, int batchSize, Current current)
            throws ProcessingException {

        if (isStreaming) {
            throw new ProcessingException("Simulación ya está corriendo");
        }

        File file = new File(dataFilePath);
        if (!file.exists()) {
            throw new ProcessingException("Archivo no encontrado: " + dataFilePath);
        }

        isStreaming = true;
        simulationStartTime = System.currentTimeMillis();
        totalDatagramsSent.set(0);

        // Crear executor con thread único para mantener orden
        simulationExecutor = Executors.newSingleThreadExecutor();

        logger.info("Iniciando simulación desde: {}", dataFilePath);
        logger.info("Delay: {}ms, Batch: {}", delayMs, batchSize);

        // Ejecutar simulación en thread separado
        simulationExecutor.submit(() -> runSimulation(dataFilePath, delayMs, batchSize));
    }

    /**
     * Detiene la simulación.
     */
    @Override
    public void stopStreaming(Current current) {
        if (!isStreaming) {
            logger.warn("Simulación no está corriendo");
            return;
        }

        logger.info("Deteniendo simulación...");
        isStreaming = false;

        if (simulationExecutor != null) {
            simulationExecutor.shutdownNow();
        }

        long duration = (System.currentTimeMillis() - simulationStartTime) / 1000;
        double rate = totalDatagramsSent.get() / Math.max(1.0, duration);

        logger.info("✓ Simulación detenida:");
        logger.info("   Datagramas enviados: {}", totalDatagramsSent.get());
        logger.info("   Duración: {}s", duration);
        logger.info("   Tasa: {:.1f} datagramas/seg", rate);
    }

    /**
     * Verifica si la simulación está corriendo.
     */
    @Override
    public boolean isStreaming(Current current) {
        return isStreaming;
    }

    /**
     * Obtiene estadísticas de la simulación.
     */
    @Override
    public String getStats(Current current) {
        long duration = isStreaming ?
                (System.currentTimeMillis() - simulationStartTime) / 1000 : 0;
        double rate = totalDatagramsSent.get() / Math.max(1.0, duration);

        return String.format(
                "╔════════════════════════════════════════╗\n" +
                        "║  Simulador de Bus: %-18s ║\n" +
                        "╠════════════════════════════════════════╣\n" +
                        "║ Estado:                  %-10s ║\n" +
                        "║ Datagramas enviados:     %,10d   ║\n" +
                        "║ Duración:                %6ds      ║\n" +
                        "║ Tasa:                    %6.1f/s    ║\n" +
                        "╚════════════════════════════════════════╝",
                simulatorId,
                isStreaming ? "ACTIVO" : "INACTIVO",
                totalDatagramsSent.get(),
                duration,
                rate
        );
    }

    /**
     * Ejecuta la simulación leyendo el archivo y publicando al broker.
     */
    private void runSimulation(String filePath, int delayMs, int batchSize) {
        logger.info("Leyendo archivo: {}", filePath);

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath), 65536)) {
            // Saltar header
            String header = reader.readLine();
            if (header == null) {
                logger.error("Archivo vacío");
                return;
            }

            List<Datagram> batch = new ArrayList<>(batchSize);
            String line;
            int lineCount = 0;

            while (isStreaming && (line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                try {
                    // Parsear datagrama
                    Datagram datagram = parseDatagram(line);
                    batch.add(datagram);
                    lineCount++;

                    // Si batch está lleno, publicar
                    if (batch.size() >= batchSize) {
                        publishBatch(batch);
                        batch.clear();

                        // Delay entre batches
                        if (delayMs > 0) {
                            Thread.sleep(delayMs);
                        }
                    }

                    // Log de progreso
                    if (lineCount % 100000 == 0) {
                        long duration = (System.currentTimeMillis() - simulationStartTime) / 1000;
                        double rate = totalDatagramsSent.get() / Math.max(1.0, duration);

                        logger.info("Simulación: {} datagramas enviados ({:.1f}/s)",
                                totalDatagramsSent.get(), rate);
                    }

                } catch (Exception e) {
                    logger.debug("Error parseando línea {}: {}", lineCount, e.getMessage());
                }
            }

            // Publicar batch final
            if (!batch.isEmpty()) {
                publishBatch(batch);
            }

            logger.info("✓ Archivo completo procesado: {} datagramas enviados",
                    totalDatagramsSent.get());

        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                logger.info("Simulación interrumpida por usuario");
                Thread.currentThread().interrupt();
            } else {
                logger.error("Error en simulación", e);
            }
        } finally {
            isStreaming = false;
        }
    }

    /**
     * Publica un batch de datagramas al broker.
     */
    private void publishBatch(List<Datagram> batch) {
        try {
            if (batch.size() == 1) {
                // Publicar individual
                broker.publish(batch.get(0));
            } else {
                // Publicar en batch
                broker.publishBatch(batch.toArray(new Datagram[0]));
            }

            totalDatagramsSent.addAndGet(batch.size());

        } catch (Exception e) {
            logger.error("Error publicando al broker: {}", e.getMessage());
        }
    }

    /**
     * Parsea una línea CSV a Datagram.
     */
    private Datagram parseDatagram(String line) {
        String[] parts = line.split(",");
        Datagram d = new Datagram();

        d.eventType = parseInt(parts[0]);
        d.registerDate = parts[1];
        d.stopId = parseLong(parts[2]);
        d.odometer = parseInt(parts[3]);
        d.latitude = parseDouble(parts[4]) / 1_000_000.0;
        d.longitude = parseDouble(parts[5]) / 1_000_000.0;
        d.taskId = parseInt(parts[6]);
        d.lineId = parseInt(parts[7]);
        d.tripId = parseInt(parts[8]);
        d.unknown1 = parseLong(parts[9]);
        d.datagramDate = parts[10];
        d.busId = parseInt(parts[11]);

        return d;
    }

    private int parseInt(String s) {
        try {
            return Integer.parseInt(s.trim());
        } catch (Exception e) {
            return -1;
        }
    }

    private long parseLong(String s) {
        try {
            return Long.parseLong(s.trim());
        } catch (Exception e) {
            return -1L;
        }
    }

    private double parseDouble(String s) {
        try {
            return Double.parseDouble(s.trim());
        } catch (Exception e) {
            return 0.0;
        }
    }
}