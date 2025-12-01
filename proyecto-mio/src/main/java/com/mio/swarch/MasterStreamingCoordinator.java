package com.mio.swarch;

import MIO.*;
import com.zeroc.Ice.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Exception;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Coordinador extendido que maneja procesamiento en streaming.
 * Se integra con el broker para procesamiento en tiempo real.
 */
public class MasterStreamingCoordinator implements StreamCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(MasterStreamingCoordinator.class);

    private final GraphBuilderService graphBuilder;
    private final ResultAggregatorService resultAggregator;
    private final List<WorkerInfo> workers;
    private final QueryServiceImpl queryService;
    private final Communicator communicator;

    // Jobs de streaming activos
    private final Map<String, StreamingJob> streamingJobs = new ConcurrentHashMap<>();

    public MasterStreamingCoordinator(GraphBuilderService graphBuilder,
                                      ResultAggregatorService resultAggregator,
                                      List<WorkerInfo> workers,
                                      QueryServiceImpl queryService,
                                      Communicator communicator) {
        this.graphBuilder = graphBuilder;
        this.resultAggregator = resultAggregator;
        this.workers = workers;
        this.queryService = queryService;
        this.communicator = communicator;

        logger.info("✓ StreamCoordinator inicializado con {} workers compartidos",
                workers != null ? workers.size() : 0);
    }
    @Override
    public String startStreamingProcessing(String dataFilePath, int numWorkers, Current current)
            throws ProcessingException {

        if (workers == null || workers.isEmpty()) {
            throw new ProcessingException("No hay workers disponibles para procesamiento");
        }

        try {
            String jobId = "streaming-" + UUID.randomUUID().toString();
            logger.info("Iniciando procesamiento streaming. JobId: {}, Workers solicitados: {}",
                    jobId, numWorkers);

            // Obtener grafo
            GraphInfo graphInfo = graphBuilder.getGraphInfo(current);
            if (graphInfo == null || graphInfo.stops.length == 0) {
                throw new ProcessingException("Grafo no inicializado. Ejecute buildGraph primero.");
            }

            // Seleccionar workers disponibles (máximo los disponibles)
            int availableWorkers = Math.min(numWorkers, workers.size());
            List<WorkerPrx> selectedWorkers = workers.stream()
                    .limit(availableWorkers)
                    .map(w -> w.proxy)
                    .collect(Collectors.toList());

            if (selectedWorkers.isEmpty()) {
                throw new ProcessingException("No se pudieron asignar workers");
            }

            logger.info("Asignados {} workers de {} solicitados",
                    selectedWorkers.size(), numWorkers);

            // Crear subscriber con workers
            StreamSubscriberService subscriber = new StreamSubscriberService(
                    "master-subscriber-" + jobId,
                    graphInfo,
                    resultAggregator,
                    jobId,
                    selectedWorkers
            );

            // Crear adapter para el subscriber
            ObjectAdapter subscriberAdapter = communicator.createObjectAdapterWithEndpoints(
                    "SubscriberAdapter-" + jobId, "tcp -h " + getMasterHost() + " -p 0"
            );

            StreamSubscriberPrx subscriberProxy = StreamSubscriberPrx.uncheckedCast(
                    subscriberAdapter.add(subscriber, Util.stringToIdentity("Subscriber-" + jobId))
            );

            subscriberAdapter.activate();

            // Conectar al broker y registrar el subscriber
            try {
                ObjectPrx brokerBase = communicator.stringToProxy(
                        "StreamBroker:tcp -h " + getMasterHost() + " -p 11000"
                );
                StreamBrokerPrx broker = StreamBrokerPrx.checkedCast(brokerBase);

                if (broker == null) {
                    throw new ProcessingException("No se pudo conectar al StreamBroker en " +
                            getMasterHost() + ":11000. Asegúrese de ejecutar StreamingServer.");
                }

                // Registrar subscriber con el broker
                broker.subscribe(subscriberProxy);
                logger.info("✓ Subscriber registrado con el broker en {}:11000", getMasterHost());

            } catch (Exception e) {
                subscriberAdapter.destroy();
                throw new ProcessingException("Error conectando al broker: " + e.getMessage());
            }

            // Crear job de streaming
            StreamingJob job = new StreamingJob(jobId, subscriber, subscriberAdapter,
                    selectedWorkers.size());
            streamingJobs.put(jobId, job);

            // Configurar QueryService con este jobId
            queryService.setCurrentJobId(jobId);

            logger.info("✓ Procesamiento streaming iniciado exitosamente: {}", jobId);
            logger.info("  - Job ID: {}", jobId);
            logger.info("  - Workers asignados: {}", selectedWorkers.size());
            logger.info("  - Subscriber: {}", "master-subscriber-" + jobId);

            return jobId;

        } catch (ProcessingException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Error iniciando streaming", e);
            throw new ProcessingException("Error: " + e.getMessage());
        }
    }

    @Override
    public void stopStreamingProcessing(String jobId, Current current) {
        StreamingJob job = streamingJobs.get(jobId);

        if (job != null) {
            logger.info("Deteniendo procesamiento streaming: {}", jobId);

            // Detener subscriber
            job.subscriber.shutdown();

            // Destruir adapter
            try {
                job.subscriberAdapter.destroy();
            } catch (Exception e) {
                logger.warn("Error destruyendo adapter: {}", e.getMessage());
            }

            job.active = false;
            job.endTime = System.currentTimeMillis();

            // Remover del mapa
            streamingJobs.remove(jobId);

            logger.info("✓ Procesamiento streaming detenido: {}", jobId);
        } else {
            logger.warn("Job de streaming no encontrado: {}", jobId);
        }
    }

    @Override
    public ArcSpeed[] getStreamingResults(String jobId, Current current)
            throws DataNotFoundException {

        StreamingJob job = streamingJobs.get(jobId);

        if (job == null) {
            throw new DataNotFoundException("Job de streaming no encontrado: " + jobId);
        }

        if (!job.active) {
            throw new DataNotFoundException("Job de streaming ya fue detenido: " + jobId);
        }

        try {
            // Obtener resultados actuales del aggregator
            ArcSpeed[] results = resultAggregator.getFinalResults(jobId, current);
            logger.debug("Streaming job {}: {} resultados obtenidos", jobId, results.length);
            return results;
        } catch (DataNotFoundException e) {
            logger.warn("Aún no hay resultados para job {}", jobId);
            return new ArcSpeed[0];
        }
    }

    @Override
    public String getStreamingStats(String jobId, Current current) {
        StreamingJob job = streamingJobs.get(jobId);

        if (job == null) {
            return "Job de streaming no encontrado: " + jobId;
        }

        long duration = job.active ?
                (System.currentTimeMillis() - job.startTime) / 1000 :
                (job.endTime - job.startTime) / 1000;

        String subscriberStats = job.subscriber.getStats();

        // Obtener estadísticas del result aggregator
        String resultsStats;
        try {
            ArcSpeed[] results = getStreamingResults(jobId, current);
            resultsStats = String.format("Arcos calculados: %,d", results.length);
        } catch (Exception e) {
            resultsStats = "Arcos calculados: 0";
        }

        return String.format(
                "╔════════════════════════════════════════════════════════╗\n" +
                        "║         Estadísticas de Streaming: %-18s ║\n" +
                        "╠════════════════════════════════════════════════════════╣\n" +
                        "║ Estado:                 %-32s ║\n" +
                        "║ Workers asignados:      %6d                            ║\n" +
                        "║ Duración:               %6d segundos                   ║\n" +
                        "║ %-45s ║\n" +
                        "╚════════════════════════════════════════════════════════╝\n\n" +
                        "%s",
                jobId.substring(0, Math.min(18, jobId.length())),
                job.active ? "ACTIVO" : "DETENIDO",
                job.numWorkers,
                duration,
                resultsStats,
                subscriberStats
        );
    }

    private String getMasterHost() {
        String masterHost = System.getenv("MASTER_HOST");
        if (masterHost == null || masterHost.isEmpty()) {
            masterHost = "192.168.131.101";
        }
        return masterHost;
    }

    /**
     * Obtiene el subscriber de un job para registro en broker.
     */
    public StreamSubscriberService getSubscriber(String jobId) {
        StreamingJob job = streamingJobs.get(jobId);
        return job != null ? job.subscriber : null;
    }

    /**
     * Lista todos los jobs de streaming activos.
     */
    public Set<String> getActiveStreamingJobs() {
        return streamingJobs.entrySet().stream()
                .filter(e -> e.getValue().active)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }
}

/**
 * Representa un job de procesamiento en streaming.
 */
class StreamingJob {
    final String jobId;
    final StreamSubscriberService subscriber;
    final ObjectAdapter subscriberAdapter;
    final int numWorkers;
    final long startTime;

    volatile boolean active;
    volatile long endTime;

    StreamingJob(String jobId, StreamSubscriberService subscriber,
                 ObjectAdapter subscriberAdapter, int numWorkers) {
        this.jobId = jobId;
        this.subscriber = subscriber;
        this.subscriberAdapter = subscriberAdapter;
        this.numWorkers = numWorkers;
        this.startTime = System.currentTimeMillis();
        this.active = true;
        this.endTime = 0;
    }
}