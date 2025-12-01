package com.mio.swarch;

import MIO.*;
import com.zeroc.Ice.Current;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Subscriber que recibe datagramas del broker y los procesa en tiempo real.
 * Calcula velocidades de arcos y actualiza resultados continuamente.
 */
public class StreamSubscriberService implements StreamSubscriber {
    private static final Logger logger = LoggerFactory.getLogger(StreamSubscriberService.class);

    private final String subscriberId;
    private final GraphInfo graphInfo;
    private final ResultAggregatorService resultAggregator;
    private final String jobId;

    // Workers disponibles para procesamiento
    private final BlockingQueue<WorkerPrx> workerPool;

    // Buffer de datagramas para procesamiento en batch
    private final BlockingQueue<Datagram> datagramBuffer = new LinkedBlockingQueue<>(5000);
    private final ExecutorService processingExecutor = Executors.newFixedThreadPool(4);

    // Estadísticas
    private volatile long datagramsReceived = 0;
    private volatile long datagramsProcessed = 0;
    private volatile boolean isActive = true;

    // Tamaño de batch para enviar a workers
    private static final int BATCH_SIZE = 1000;

    public StreamSubscriberService(String subscriberId, GraphInfo graphInfo,
                                   ResultAggregatorService resultAggregator,
                                   String jobId, List<WorkerPrx> workers) {
        this.subscriberId = subscriberId;
        this.graphInfo = graphInfo;
        this.resultAggregator = resultAggregator;
        this.jobId = jobId;

        if (workers != null && !workers.isEmpty()) {
            this.workerPool = new LinkedBlockingQueue<>(workers);
        } else {
            this.workerPool = new LinkedBlockingQueue<>();
        }

        // Iniciar threads de procesamiento
        startProcessingThreads();

        logger.info("✓ StreamSubscriber iniciado: {} con {} workers",
                subscriberId, workerPool.size());
    }

    /**
     * Callback llamado por el broker cuando llega un nuevo datagrama.
     */
    @Override
    public void onDatagramReceived(Datagram datagram, Current current) {
        if (!isActive) return;

        try {
            // Agregar a buffer
            boolean added = datagramBuffer.offer(datagram, 50, TimeUnit.MILLISECONDS);

            if (added) {
                datagramsReceived++;

                if (datagramsReceived % 5000 == 0) {
                    logger.info("Subscriber {}: {} datagramas recibidos, {} en buffer",
                            subscriberId, datagramsReceived, datagramBuffer.size());
                }
            } else {
                logger.warn("Buffer lleno, datagrama descartado");
            }

        } catch (InterruptedException e) {
            logger.error("Error recibiendo datagrama", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Inicia threads que procesan datagramas del buffer.
     */
    private void startProcessingThreads() {
        // Thread principal que agrupa datagramas en batches
        Thread batchProcessor = new Thread(() -> {
            logger.info("Thread de procesamiento por batches iniciado");

            List<Datagram> currentBatch = new ArrayList<>(BATCH_SIZE);

            while (isActive) {
                try {
                    // Obtener datagramas del buffer
                    Datagram datagram = datagramBuffer.poll(1, TimeUnit.SECONDS);

                    if (datagram != null) {
                        currentBatch.add(datagram);

                        // Si batch está lleno, procesar
                        if (currentBatch.size() >= BATCH_SIZE) {
                            processBatch(new ArrayList<>(currentBatch));
                            currentBatch.clear();
                        }
                    } else if (!currentBatch.isEmpty()) {
                        // Timeout: procesar batch parcial
                        processBatch(new ArrayList<>(currentBatch));
                        currentBatch.clear();
                    }

                } catch (InterruptedException e) {
                    logger.warn("Thread de batch interrumpido");
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error procesando batch", e);
                }
            }

            // Procesar batch final
            if (!currentBatch.isEmpty()) {
                processBatch(currentBatch);
            }

            logger.info("Thread de procesamiento detenido");
        }, "StreamBatchProcessor");

        batchProcessor.setDaemon(true);
        batchProcessor.start();
    }

    /**
     * Procesa un batch de datagramas usando un worker disponible.
     */
    private void processBatch(List<Datagram> batch) {
        processingExecutor.submit(() -> {
            WorkerPrx worker = null;

            try {
                // Obtener worker disponible
                worker = workerPool.poll(5, TimeUnit.SECONDS);

                if (worker == null) {
                    logger.warn("No hay workers disponibles, batch descartado");
                    return;
                }

                // Convertir datagramas a líneas CSV
                String[] datagramLines = new String[batch.size()];
                for (int i = 0; i < batch.size(); i++) {
                    datagramLines[i] = datagramToCSV(batch.get(i));
                }

                // Crear tarea
                ProcessingTask task = new ProcessingTask();
                task.taskId = (int) (System.currentTimeMillis() % Integer.MAX_VALUE);
                task.datagramLines = datagramLines;
                task.filePath = "";
                task.startOffset = 0;
                task.endOffset = 0;

                // Procesar en worker
                ProcessingResult result = worker.processTask(task, graphInfo);

                // Devolver worker a la pool
                workerPool.offer(worker);

                if (result.success) {
                    // Agregar resultado parcial
                    resultAggregator.addPartialResult(jobId, result, null);

                    datagramsProcessed += batch.size();

                    if (datagramsProcessed % 10000 == 0) {
                        logger.info("✓ Subscriber {}: {} datagramas procesados",
                                subscriberId, datagramsProcessed);
                    }
                } else {
                    logger.error("✗ Error procesando batch: {}", result.errorMessage);
                }

            } catch (InterruptedException e) {
                logger.warn("Procesamiento interrumpido");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("Error procesando batch con worker", e);

                // Devolver worker a la pool si se obtuvo
                if (worker != null) {
                    workerPool.offer(worker);
                }
            }
        });
    }

    /**
     * Convierte un Datagram a formato CSV para el worker.
     */
    private String datagramToCSV(Datagram d) {
        return String.format("%d,%s,%d,%d,%.0f,%.0f,%d,%d,%d,%d,%s,%d",
                d.eventType,
                d.registerDate,
                d.stopId,
                d.odometer,
                d.latitude * 1_000_000,  // Convertir a formato original
                d.longitude * 1_000_000,
                d.taskId,
                d.lineId,
                d.tripId,
                d.unknown1,
                d.datagramDate,
                d.busId
        );
    }

    /**
     * Obtiene estadísticas del subscriber.
     */
    public String getStats() {
        return String.format(
                "╔════════════════════════════════════════╗\n" +
                        "║  Estadísticas Subscriber: %-12s ║\n" +
                        "╠════════════════════════════════════════╣\n" +
                        "║ Datagramas recibidos:    %,10d   ║\n" +
                        "║ Datagramas procesados:   %,10d   ║\n" +
                        "║ En buffer:               %6d       ║\n" +
                        "║ Workers disponibles:     %6d       ║\n" +
                        "║ Estado:                  %-10s ║\n" +
                        "╚════════════════════════════════════════╝",
                subscriberId,
                datagramsReceived,
                datagramsProcessed,
                datagramBuffer.size(),
                workerPool.size(),
                isActive ? "ACTIVO" : "INACTIVO"
        );
    }

    /**
     * Detiene el subscriber.
     */
    public void shutdown() {
        logger.info("Deteniendo subscriber {}...", subscriberId);
        isActive = false;
        processingExecutor.shutdown();

        try {
            processingExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Timeout esperando shutdown");
            processingExecutor.shutdownNow();
        }

        logger.info("✓ Subscriber {} detenido", subscriberId);
    }
}