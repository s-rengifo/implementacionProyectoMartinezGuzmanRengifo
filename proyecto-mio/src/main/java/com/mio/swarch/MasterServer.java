package com.mio.swarch;

import MIO.*;
import com.zeroc.Ice.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.Exception;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MasterServer {
    private static final Logger logger = LoggerFactory.getLogger(MasterServer.class);

    // Lista compartida de workers entre MasterCoordinator y StreamCoordinator
    private static final List<WorkerInfo> sharedWorkers = new CopyOnWriteArrayList<>();

    public static void main(String[] args) {
        List<String> extraArgs = new ArrayList<>();

        String masterHost = System.getenv("MASTER_HOST");
        if (masterHost == null || masterHost.isEmpty()) {
            masterHost = "192.168.131.101";
        }

        try (Communicator communicator = Util.initialize(args, extraArgs)) {
            ObjectAdapter adapter = communicator.createObjectAdapterWithEndpoints(
                    "MasterAdapter", "tcp -h " + masterHost + " -p 10000"
            );

            GraphBuilderService graphBuilder = new GraphBuilderService();
            ResultAggregatorService resultAggregator = new ResultAggregatorService();
            QueryServiceImpl queryService = new QueryServiceImpl(resultAggregator);

            // Inicializar MasterCoordinatorImpl con workers compartidos
            MasterCoordinatorImpl coordinator = new MasterCoordinatorImpl(
                    graphBuilder, resultAggregator, queryService, sharedWorkers);

            // Inicializar MasterStreamingCoordinator con workers compartidos
            MasterStreamingCoordinator streamCoordinator = new MasterStreamingCoordinator(
                    graphBuilder, resultAggregator, sharedWorkers, queryService, communicator);

            adapter.add(coordinator, Util.stringToIdentity("MasterCoordinator"));
            adapter.add(streamCoordinator, Util.stringToIdentity("StreamCoordinator"));
            adapter.add(graphBuilder, Util.stringToIdentity("GraphBuilder"));
            adapter.add(resultAggregator, Util.stringToIdentity("ResultAggregator"));
            adapter.add(queryService, Util.stringToIdentity("QueryService"));

            adapter.activate();

            logger.info("════════════════════════════════════════════════════════");
            logger.info("Master Server iniciado en tcp://{}:10000", masterHost);
            logger.info("════════════════════════════════════════════════════════");
            logger.info("Servicios disponibles:");
            logger.info("  - MasterCoordinator");
            logger.info("  - GraphBuilder");
            logger.info("  - ResultAggregator");
            logger.info("  - QueryService");
            logger.info("");
            logger.info("Escuchando conexiones de Workers...");
            logger.info("Servidor listo. Presione Ctrl+C para detener.");
            logger.info("════════════════════════════════════════════════════════");

            communicator.waitForShutdown();
        } catch (Exception e) {
            logger.error("Error en Master Server", e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}

class MasterCoordinatorImpl implements MasterCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(MasterCoordinatorImpl.class);

    // CAMBIO 1: Aumentar tamaño de chunk para reducir número de tasks
    private static final int MAX_CHUNK_SIZE = 200000; // 200k líneas (antes 50k)

    // CAMBIO 2: Límite de tasks en cola simultáneas
    private static final int MAX_CONCURRENT_TASKS = 20;

    private final GraphBuilderService graphBuilder;
    private final ResultAggregatorService resultAggregator;
    private final QueryServiceImpl queryService;
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private List<WorkerInfo> workers = new CopyOnWriteArrayList<>();

    // CAMBIO 3: Pool de hilos ajustado dinámicamente según workers disponibles
    private ExecutorService executorService;

    public MasterCoordinatorImpl(GraphBuilderService graphBuilder,
                                 ResultAggregatorService resultAggregator,
                                 QueryServiceImpl queryService,
                                 List<WorkerInfo> sharedWorkers) {
        this.graphBuilder = graphBuilder;
        this.resultAggregator = resultAggregator;
        this.queryService = queryService;
        this.workers = sharedWorkers;  // Usar lista compartida
        this.executorService = Executors.newFixedThreadPool(4);
    }

    @Override
    public String startProcessing(String dataFilePath, int numTasks, Current current)
            throws ProcessingException {
        try {
            String jobId = UUID.randomUUID().toString();
            logger.info("Iniciando procesamiento STREAMING. JobId: {}, Archivo: {}, Tasks: {}",
                    jobId, dataFilePath, numTasks);

            // Validar archivo
            File file = new File(dataFilePath);
            if (!file.isAbsolute()) {
                file = new File(System.getProperty("user.dir"), dataFilePath);
                logger.info("Ruta relativa detectada. Usando ruta absoluta: {}", file.getAbsolutePath());
            }

            if (!file.exists() || !file.canRead()) {
                String errorMsg = String.format("Archivo no encontrado o no legible: %s", file.getAbsolutePath());
                logger.error(errorMsg);
                throw new ProcessingException(errorMsg);
            }

            long fileSize = file.length();
            logger.info("Archivo validado: {} ({} MB)", file.getAbsolutePath(), fileSize / (1024 * 1024));

            // Obtener información del grafo
            GraphInfo graphInfo = graphBuilder.getGraphInfo(current);
            if (graphInfo == null || graphInfo.stops.length == 0) {
                String errorMsg = "Grafo no inicializado. Ejecute buildGraph primero.";
                logger.error(errorMsg);
                throw new ProcessingException(errorMsg);
            }

            logger.info("Grafo disponible: {} rutas, {} paradas", graphInfo.totalRoutes, graphInfo.totalStops);

            // CAMBIO 4: Ajustar pool según workers disponibles
            int availableWorkers = workers.size();
            if (availableWorkers == 0) {
                throw new ProcessingException("No hay workers disponibles");
            }

            // Recrear executor con tamaño apropiado
            if (executorService.isShutdown() || executorService.isTerminated()) {
                executorService = Executors.newFixedThreadPool(Math.min(availableWorkers * 2, MAX_CONCURRENT_TASKS));
            }

            // Crear Job
            Job job = new Job(jobId, numTasks, graphInfo);
            jobs.put(jobId, job);

            final String finalFilePath = file.getAbsolutePath();

            // Iniciar procesamiento en paralelo con streaming
            executorService.submit(() -> processFileWithStreaming(job, finalFilePath, numTasks));

            logger.info("Procesamiento iniciado en modo STREAMING con {} workers", availableWorkers);

            return jobId;

        } catch (ProcessingException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Error inesperado al iniciar procesamiento", e);
            throw new ProcessingException("Error: " + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }
    }

    /**
     * VERSIÓN CORREGIDA: Procesa el archivo con control de flujo
     */
    private void processFileWithStreaming(Job job, String filePath, int numWorkers) {
        logger.info("Iniciando procesamiento STREAMING para job {}", job.jobId);

        if (workers.isEmpty()) {
            logger.error("No hay workers disponibles");
            job.status = JobStatus.FAILED;
            return;
        }

        job.status = JobStatus.RUNNING;
        job.startTime = System.currentTimeMillis();

        // CAMBIO 5: Usar BlockingQueue con capacidad limitada para control de flujo
        BlockingQueue<WorkerInfo> workerQueue = new LinkedBlockingQueue<>(workers);

        // Contadores
        AtomicInteger taskIdCounter = new AtomicInteger(0);
        AtomicInteger tasksInProgress = new AtomicInteger(0);
        AtomicInteger completedTasks = new AtomicInteger(0);
        AtomicInteger failedTasks = new AtomicInteger(0);

        // CAMBIO 6: Semáforo para limitar tasks concurrentes
        Semaphore taskLimiter = new Semaphore(MAX_CONCURRENT_TASKS);

        // Pool para procesar tareas
        ExecutorService taskExecutor = Executors.newFixedThreadPool(
                Math.min(workers.size() * 2, MAX_CONCURRENT_TASKS)
        );

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath), 65536)) {
            // Saltar header
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("Archivo vacío");
            }

            logger.info("Iniciando lectura por chunks de {} líneas", MAX_CHUNK_SIZE);
            logger.info("Límite de tasks concurrentes: {}", MAX_CONCURRENT_TASKS);

            List<String> currentChunk = new ArrayList<>(MAX_CHUNK_SIZE);
            String line;
            int totalLinesRead = 0;

            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                currentChunk.add(line);
                totalLinesRead++;

                // Cuando el chunk está lleno, enviarlo
                if (currentChunk.size() >= MAX_CHUNK_SIZE) {
                    final int taskId = taskIdCounter.getAndIncrement();
                    final List<String> chunkToSend = new ArrayList<>(currentChunk);

                    // CAMBIO 7: Esperar si hay muchas tasks en progreso (backpressure)
                    try {
                        taskLimiter.acquire();
                    } catch (InterruptedException e) {
                        logger.warn("Interrupción esperando semáforo");
                        Thread.currentThread().interrupt();
                        break;
                    }

                    tasksInProgress.incrementAndGet();

                    // Enviar chunk a worker
                    taskExecutor.submit(() -> {
                        try {
                            WorkerInfo worker = workerQueue.poll(5, TimeUnit.SECONDS);
                            if (worker == null) {
                                logger.error("Timeout esperando worker para task {}", taskId);
                                failedTasks.incrementAndGet();
                                return;
                            }

                            // Crear tarea
                            ProcessingTask task = new ProcessingTask();
                            task.taskId = taskId;
                            task.datagramLines = chunkToSend.toArray(new String[0]);
                            task.filePath = "";
                            task.startOffset = 0;
                            task.endOffset = 0;

                            logger.debug("Enviando task {} a worker {} ({} líneas)",
                                    taskId, worker.workerId, task.datagramLines.length);

                            // Procesar en worker
                            ProcessingResult result = worker.proxy.processTask(task, job.graphInfo);

                            // Devolver worker a la cola
                            workerQueue.offer(worker);

                            if (result.success) {
                                resultAggregator.addPartialResult(job.jobId, result, null);
                                completedTasks.incrementAndGet();

                                if (taskId % 10 == 0) {
                                    logger.info("Progreso: Task {} completada. Total: {}/{}, En proceso: {}",
                                            taskId, completedTasks.get(), taskIdCounter.get(), tasksInProgress.get());
                                }
                            } else {
                                logger.error("Task {} falló: {}", taskId, result.errorMessage);
                                failedTasks.incrementAndGet();
                            }

                        } catch (Exception e) {
                            logger.error("Error procesando task " + taskId, e);
                            failedTasks.incrementAndGet();
                        } finally {
                            tasksInProgress.decrementAndGet();
                            taskLimiter.release();
                        }
                    });

                    // Limpiar chunk
                    currentChunk.clear();

                    // CAMBIO 8: Log mejorado de progreso
                    if (totalLinesRead % 1000000 == 0) {
                        double progress = (double) completedTasks.get() / taskIdCounter.get() * 100;
                        logger.info("═══ Progreso: {} M líneas leídas, {} tasks creadas, {} completadas ({:.1f}%), {} en proceso ═══",
                                totalLinesRead / 1000000, taskIdCounter.get(), completedTasks.get(), progress, tasksInProgress.get());
                    }
                }
            }

            // Enviar último chunk
            if (!currentChunk.isEmpty()) {
                final int taskId = taskIdCounter.getAndIncrement();
                final List<String> chunkToSend = new ArrayList<>(currentChunk);

                try {
                    taskLimiter.acquire();
                } catch (InterruptedException e) {
                    logger.warn("Interrupción esperando semáforo para última task");
                }

                tasksInProgress.incrementAndGet();

                taskExecutor.submit(() -> {
                    try {
                        WorkerInfo worker = workerQueue.poll(5, TimeUnit.SECONDS);
                        if (worker != null) {
                            ProcessingTask task = new ProcessingTask();
                            task.taskId = taskId;
                            task.datagramLines = chunkToSend.toArray(new String[0]);
                            task.filePath = "";
                            task.startOffset = 0;
                            task.endOffset = 0;

                            logger.info("Enviando ÚLTIMA task {} a worker {} ({} líneas)",
                                    taskId, worker.workerId, task.datagramLines.length);

                            ProcessingResult result = worker.proxy.processTask(task, job.graphInfo);
                            workerQueue.offer(worker);

                            if (result.success) {
                                resultAggregator.addPartialResult(job.jobId, result, null);
                                completedTasks.incrementAndGet();
                            } else {
                                failedTasks.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Error procesando última task", e);
                        failedTasks.incrementAndGet();
                    } finally {
                        tasksInProgress.decrementAndGet();
                        taskLimiter.release();
                    }
                });
            }

            logger.info("✓ Archivo leído completamente: {} líneas, {} tareas creadas",
                    totalLinesRead, taskIdCounter.get());

        } catch (Exception e) {
            logger.error("Error leyendo archivo en modo streaming", e);
            job.status = JobStatus.FAILED;
            taskExecutor.shutdownNow();
            return;
        }

        // Esperar finalización
        taskExecutor.shutdown();
        try {
            logger.info("Esperando finalización de todas las tareas...");

            boolean finished = taskExecutor.awaitTermination(2, TimeUnit.HOURS);

            if (!finished) {
                logger.warn("Timeout esperando tareas. Forzando shutdown...");
                taskExecutor.shutdownNow();
            }

            // Actualizar job
            job.endTime = System.currentTimeMillis();
            job.completedTasks.set(completedTasks.get());
            job.failedTasks.set(failedTasks.get());
            job.totalTasks = taskIdCounter.get();

            job.status = failedTasks.get() == 0 ? JobStatus.COMPLETED : JobStatus.PARTIAL;

            double totalTime = (job.endTime - job.startTime) / 1000.0;

            logger.info("════════════════════════════════════════════════════════");
            logger.info("Job {} FINALIZADO", job.jobId);
            logger.info("Estado: {}", job.status);
            logger.info("Tareas completadas: {}/{}", completedTasks.get(), taskIdCounter.get());
            logger.info("Tareas fallidas: {}", failedTasks.get());
            logger.info("Tiempo total: {:.2f} segundos", totalTime);
            logger.info("════════════════════════════════════════════════════════");

            // Actualizar QueryService
            if (job.status == JobStatus.COMPLETED || job.status == JobStatus.PARTIAL) {
                queryService.setCurrentJobId(job.jobId);
            }

        } catch (InterruptedException e) {
            logger.error("Interrupción esperando tareas", e);
            job.status = JobStatus.FAILED;
        }
    }

    @Override
    public void registerWorker(WorkerPrx workerProxy, String workerId, Current current) {
        WorkerInfo info = new WorkerInfo(workerId, workerProxy);
        workers.add(info);
        logger.info("Worker registrado: {} (Total workers: {})", workerId, workers.size());
    }

    @Override
    public String getProcessingStatus(String jobId, Current current) {
        Job job = jobs.get(jobId);
        if (job == null) {
            return "Job no encontrado: " + jobId;
        }

        double elapsedTime = job.endTime > 0 ?
                (job.endTime - job.startTime) / 1000.0 :
                (System.currentTimeMillis() - job.startTime) / 1000.0;

        return String.format(
                "JobId: %s\nEstado: %s\nTareas completadas: %d/%d\nTareas fallidas: %d\nTiempo: %.2fs",
                job.jobId,
                job.status,
                job.completedTasks.get(),
                job.totalTasks,
                job.failedTasks.get(),
                elapsedTime
        );
    }

    @Override
    public ArcSpeed[] getResults(String jobId, Current current) throws DataNotFoundException {
        Job job = jobs.get(jobId);
        if (job == null) {
            throw new DataNotFoundException("Job no encontrado: " + jobId);
        }

        if (job.status != JobStatus.COMPLETED && job.status != JobStatus.PARTIAL) {
            throw new DataNotFoundException("Job aún en proceso: " + jobId);
        }

        return resultAggregator.getFinalResults(jobId, current);
    }

    @Override
    public void cancelProcessing(String jobId, Current current) {
        Job job = jobs.get(jobId);
        if (job != null) {
            job.status = JobStatus.CANCELLED;
            logger.info("Job {} cancelado", jobId);
        }
    }
}

class Job {
    String jobId;
    int totalTasks;
    GraphInfo graphInfo;
    JobStatus status = JobStatus.PENDING;
    AtomicInteger completedTasks = new AtomicInteger(0);
    AtomicInteger failedTasks = new AtomicInteger(0);
    long startTime;
    long endTime;

    Job(String jobId, int estimatedTasks, GraphInfo graphInfo) {
        this.jobId = jobId;
        this.totalTasks = estimatedTasks;
        this.graphInfo = graphInfo;
    }
}

enum JobStatus {
    PENDING, RUNNING, COMPLETED, PARTIAL, FAILED, CANCELLED
}

class WorkerInfo {
    String workerId;
    WorkerPrx proxy;

    WorkerInfo(String workerId, WorkerPrx proxy) {
        this.workerId = workerId;
        this.proxy = proxy;
    }
}