package com.mio.swarch;

import MIO.*;
import com.zeroc.Ice.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.Exception;
import java.util.Scanner;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ClientApp {
    private static final Logger logger = LoggerFactory.getLogger(ClientApp.class);

    // Configuración del Master - puede ser sobreescrita por variable de entorno
    private static String getMasterHost() {
        String masterHost = System.getenv("MASTER_HOST");
        if (masterHost == null || masterHost.isEmpty()) {
            masterHost = "192.168.131.101"; // IP por defecto del master (104m01)
        }
        return masterHost;
    }

    private static final String MASTER_PROXY_TEMPLATE = "MasterCoordinator:tcp -h %s -p 10000";
    private static final String GRAPH_BUILDER_PROXY_TEMPLATE = "GraphBuilder:tcp -h %s -p 10000";
    private static final String QUERY_SERVICE_PROXY_TEMPLATE = "QueryService:tcp -h %s -p 10000";
    private static final String RESULT_AGGREGATOR_PROXY_TEMPLATE = "ResultAggregator:tcp -h %s -p 10000";
    private static final String STREAM_BROKER_PROXY_TEMPLATE = "StreamBroker:tcp -h %s -p 11000";
    private static final String STREAM_COORDINATOR_PROXY_TEMPLATE = "StreamCoordinator:tcp -h %s -p 10000";

    public static void main(String[] args) {
        String masterHost = getMasterHost();

        try (Communicator communicator = Util.initialize(args)) {

            logger.info("Conectando al Master en {}:10000...", masterHost);

            MasterCoordinatorPrx master = getMasterProxy(communicator, masterHost);
            GraphBuilderPrx graphBuilder = getGraphBuilderProxy(communicator, masterHost);
            QueryServicePrx queryService = getQueryServiceProxy(communicator, masterHost);
            ResultAggregatorPrx resultAggregator = getResultAggregatorProxy(communicator, masterHost);
            StreamCoordinatorPrx streamCoordinator = getStreamCoordinatorProxy(communicator, masterHost);

            // Conectar al broker de streaming
            logger.info("Conectando al Streaming Server en {}:11000...", masterHost);
            StreamBrokerPrx streamBroker = getStreamBrokerProxy(communicator, masterHost);

            logger.info("✓ Conexiones establecidas");

            Scanner scanner = new Scanner(System.in);
            boolean running = true;

            printMenu(masterHost);

            while (running) {
                System.out.print("\nOpción: ");
                String option = scanner.nextLine().trim();

                switch (option) {
                    case "1":
                        buildGraph(graphBuilder, scanner);
                        break;
                    case "2":
                        startProcessing(master, scanner);
                        break;
                    case "3":
                        checkStatus(master, scanner);
                        break;
                    case "4":
                        getResults(master, scanner);
                        break;
                    case "5":
                        saveResults(resultAggregator, scanner);
                        break;
                    case "6":
                        queryArcSpeed(queryService, scanner);
                        break;
                    case "7":
                        queryRouteSpeed(queryService, scanner);
                        break;
                    case "8":
                        showStatistics(queryService);
                        break;
                    case "9":
                        runStreamingMode(communicator, graphBuilder, streamCoordinator,
                                resultAggregator, queryService, scanner, masterHost);
                        break;
                    case "10":
                        runCompleteTest(graphBuilder, master, resultAggregator, scanner);
                        break;
                    case "0":
                        running = false;
                        break;
                    case "h":
                        printMenu(masterHost);
                        break;
                    default:
                        System.out.println("Opción inválida");
                }
            }

            System.out.println("Cliente finalizado");

        } catch (Exception e) {
            logger.error("Error en cliente", e);
            e.printStackTrace();
        }
    }

    private static void printMenu(String masterHost) {
        System.out.println("\n════════════════════════════════════════════════════════");
        System.out.println("       SISTEMA MIO - Cliente de Prueba");
        System.out.println("       Conectado a Master: " + masterHost + ":10000");
        System.out.println("════════════════════════════════════════════════════════");
        System.out.println("1. Construir grafo de rutas");
        System.out.println("2. Iniciar procesamiento de datagramas");
        System.out.println("3. Consultar estado de procesamiento");
        System.out.println("4. Obtener resultados");
        System.out.println("5. Guardar resultados en archivo");
        System.out.println("6. Consultar velocidad de un arco");
        System.out.println("7. Consultar velocidades de una ruta");
        System.out.println("8. Ver estadísticas generales");
        System.out.println("9. MODO STREAMING (Tiempo Real)");
        System.out.println("0. Salir");
        System.out.println("h. Mostrar menú");
        System.out.println("════════════════════════════════════════════════════════");
    }

    /**
     * MODO STREAMING - Con Procesamiento Real en Tiempo Real
     */
    private static void runStreamingMode(Communicator communicator,
                                         GraphBuilderPrx graphBuilder,
                                         StreamCoordinatorPrx streamCoordinator,
                                         ResultAggregatorPrx resultAggregator,
                                         QueryServicePrx queryService,
                                         Scanner scanner,
                                         String masterHost) {
        try {
            System.out.println("\n╔════════════════════════════════════════════════════════╗");
            System.out.println("║             PROCESAMIENTO EN TIEMPO REAL               ║");
            System.out.println("║       (Calculando velocidades mientras transmite)      ║");
            System.out.println("╚════════════════════════════════════════════════════════╝");

            // 1. Verificar grafo
            System.out.println("\n[1/5] Verificando grafo...");
            GraphInfo graphInfo = graphBuilder.getGraphInfo();
            if (graphInfo == null || graphInfo.totalStops == 0) {
                System.out.println("Grafo no construido. Construyendo...");
                graphInfo = graphBuilder.buildGraph(
                        "/home/swarch/proyecto-mio/MIO/lines-241.csv",
                        "/home/swarch/proyecto-mio/MIO/stops-241.csv",
                        "/home/swarch/proyecto-mio/MIO/linestops-241.csv"
                );
            }
            System.out.printf("✓ Grafo: %d rutas, %d paradas\n",
                    graphInfo.totalRoutes, graphInfo.totalStops);

            // 2. Configurar parámetros
            System.out.println("\n[2/5] Configuración de streaming:");
            System.out.print("Archivo de datagramas (default: data/datagrams_1M.csv): ");
            String dataFile = scanner.nextLine().trim();
            if (dataFile.isEmpty()) {
                dataFile = "data/datagrams_1M.csv";
            }

            File file = new File(dataFile);
            if (!file.exists()) {
                file = new File(System.getProperty("user.dir"), dataFile);
            }

            if (!file.exists()) {
                System.err.println("✗ Archivo no encontrado: " + dataFile);
                return;
            }

            System.out.printf("✓ Archivo: %s (%.2f MB)\n",
                    file.getAbsolutePath(), file.length() / (1024.0 * 1024.0));

            System.out.print("Número de workers para procesamiento (default: 3): ");
            String workersStr = scanner.nextLine().trim();
            int numWorkers = workersStr.isEmpty() ? 3 : Integer.parseInt(workersStr);

            System.out.print("Delay entre datagramas en ms (0=máxima velocidad, default: 10): ");
            String delayStr = scanner.nextLine().trim();
            int delayMs = delayStr.isEmpty() ? 10 : Integer.parseInt(delayStr);

            System.out.print("Tamaño de batch (default: 100): ");
            String batchStr = scanner.nextLine().trim();
            int batchSize = batchStr.isEmpty() ? 100 : Integer.parseInt(batchStr);

            // 3. Iniciar procesamiento en el master
            System.out.println("\n[3/5] Iniciando procesamiento en tiempo real...");
            String jobId = streamCoordinator.startStreamingProcessing(dataFile, numWorkers);
            System.out.printf("✓ Job de streaming creado: %s\n", jobId);
            System.out.printf("✓ Asignados %d workers para procesamiento\n", numWorkers);

            // 4. Conectar al broker
            System.out.println("\n[4/5] Conectando al Streaming Broker...");
            StreamBrokerPrx broker;
            try {
                broker = getStreamBrokerProxy(communicator, masterHost);
                System.out.println("✓ Conectado al broker en " + masterHost + ":11000");
            } catch (Exception e) {
                System.err.println("✗ Error: No se pudo conectar al broker.");
                System.err.println("  Asegúrese de que StreamingServer esté corriendo:");
                System.err.println("  ./gradlew runStreamingServer");
                streamCoordinator.stopStreamingProcessing(jobId);
                return;
            }

            // 5. Crear simulador y comenzar streaming
            System.out.println("\n[5/5] Iniciando simulación de buses...");

            BusSimulatorService busSimulator = new BusSimulatorService("bus-sim-1", broker);

            ObjectAdapter simAdapter = communicator.createObjectAdapterWithEndpoints(
                    "SimulatorAdapter", "tcp"
            );

            BusSimulatorPrx simPrx = BusSimulatorPrx.uncheckedCast(
                    simAdapter.add(busSimulator, Util.stringToIdentity("BusSimulator"))
            );

            simAdapter.activate();

            // Iniciar streaming
            simPrx.startStreaming(file.getAbsolutePath(), delayMs, batchSize);

            System.out.println("✓ Simulación iniciada");
            System.out.println("\n╔════════════════════════════════════════════════════════╗");
            System.out.println("║       STREAMING ACTIVO - PROCESANDO EN TIEMPO REAL     ║");
            System.out.println("║       Presione ENTER para detener y ver resultados    ║");
            System.out.println("╚════════════════════════════════════════════════════════╝\n");

            // Monitorear con scheduler mostrando VELOCIDADES REALES
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            AtomicLong lastCheck = new AtomicLong(System.currentTimeMillis());
            AtomicInteger updateCounter = new AtomicInteger(0);

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    int updates = updateCounter.incrementAndGet();
                    System.out.println("\n" + new Date());
                    System.out.println("═══════════════════════════════════════════════════════");

                    // Stats del broker
                    try {
                        System.out.println(broker.getStats());
                    } catch (Exception e) {
                        System.out.println("Broker no responde");
                    }

                    // Stats del simulador
                    try {
                        System.out.println(simPrx.getStats());
                    } catch (Exception e) {
                        System.out.println("Simulador no responde");
                    }

                    // Stats del procesamiento streaming
                    try {
                        String streamStats = streamCoordinator.getStreamingStats(jobId);
                        System.out.println(streamStats);
                    } catch (Exception e) {
                        System.out.println("Stats de procesamiento no disponibles aún");
                    }

                    // MOSTRAR RESULTADOS PARCIALES CALCULADOS
                    if (updates % 2 == 0) { // Cada 10 segundos
                        try {
                            ArcSpeed[] results = streamCoordinator.getStreamingResults(jobId);

                            if (results.length > 0) {
                                System.out.println("\nVELOCIDADES CALCULADAS EN TIEMPO REAL:");
                                System.out.println("─────────────────────────────────────────────────────");
                                System.out.println("Total de arcos con velocidad: " + results.length);

                                // Mostrar algunos arcos de ejemplo
                                System.out.println("\nÚltimos 10 arcos calculados:");
                                int start = Math.max(0, results.length - 10);
                                for (int i = start; i < results.length; i++) {
                                    ArcSpeed arc = results[i];
                                    System.out.printf("  Ruta %d: Parada %d → %d | %.2f km/h | %d muestras\n",
                                            arc.lineId, arc.originStopId, arc.destinationStopId,
                                            arc.averageSpeed, arc.sampleCount);
                                }

                                // Estadística general
                                double avgSpeed = 0;
                                int totalSamples = 0;
                                for (ArcSpeed arc : results) {
                                    avgSpeed += arc.averageSpeed;
                                    totalSamples += arc.sampleCount;
                                }
                                avgSpeed /= results.length;

                                System.out.println("\nEstadísticas globales:");
                                System.out.printf("  Velocidad promedio del sistema: %.2f km/h\n", avgSpeed);
                                System.out.printf("  Total de muestras procesadas: %,d\n", totalSamples);
                            } else {
                                System.out.println("\nCalculando velocidades... (esperando datos)");
                            }
                        } catch (DataNotFoundException e) {
                            System.out.println("\nAún no hay resultados calculados...");
                        } catch (Exception e) {
                            logger.debug("Error obteniendo resultados: {}", e.getMessage());
                        }
                    }

                    System.out.println("═══════════════════════════════════════════════════════");

                } catch (Exception e) {
                    logger.error("Error mostrando stats", e);
                }
            }, 5, 5, TimeUnit.SECONDS);

            // Esperar que usuario detenga
            scanner.nextLine();

            // Detener todo
            System.out.println("\nDeteniendo streaming y procesamiento...");
            scheduler.shutdown();

            try {
                simPrx.stopStreaming();
            } catch (Exception e) {
                logger.warn("Error deteniendo simulador: {}", e.getMessage());
            }

            try {
                streamCoordinator.stopStreamingProcessing(jobId);
            } catch (Exception e) {
                logger.warn("Error deteniendo procesamiento: {}", e.getMessage());
            }

            // Esperar a que termine el scheduler
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Resumen final
            System.out.println("\n╔════════════════════════════════════════════════════════╗");
            System.out.println("║               RESUMEN FINAL DE STREAMING               ║");
            System.out.println("╚════════════════════════════════════════════════════════╝\n");

            try {
                System.out.println(broker.getStats());
            } catch (Exception e) {
                System.out.println("Broker stats no disponibles");
            }

            try {
                System.out.println(simPrx.getStats());
            } catch (Exception e) {
                System.out.println("Simulador stats no disponibles");
            }

            try {
                System.out.println(streamCoordinator.getStreamingStats(jobId));
            } catch (Exception e) {
                System.out.println("Stats de procesamiento no disponibles");
            }

            // Mostrar resultados finales
            try {
                ArcSpeed[] finalResults = streamCoordinator.getStreamingResults(jobId);
                System.out.println("\n╔════════════════════════════════════════════════════════╗");
                System.out.println("║                 RESULTADOS FINALES                     ║");
                System.out.println("╚════════════════════════════════════════════════════════╝");
                System.out.printf("\nTotal de arcos calculados: %,d\n", finalResults.length);

                if (finalResults.length > 0) {
                    // Top 10 rutas más rápidas
                    Arrays.sort(finalResults, (a, b) -> Double.compare(b.averageSpeed, a.averageSpeed));

                    System.out.println("\n Top 10 arcos más rápidos:");
                    for (int i = 0; i < Math.min(10, finalResults.length); i++) {
                        ArcSpeed arc = finalResults[i];
                        System.out.printf("%2d. Ruta %d: %d → %d | %.2f km/h (%d muestras)\n",
                                i + 1, arc.lineId, arc.originStopId, arc.destinationStopId,
                                arc.averageSpeed, arc.sampleCount);
                    }

                    // Top 10 rutas más lentas
                    System.out.println("\n Top 10 arcos más lentos:");
                    for (int i = Math.max(0, finalResults.length - 10); i < finalResults.length; i++) {
                        ArcSpeed arc = finalResults[i];
                        System.out.printf("%2d. Ruta %d: %d → %d | %.2f km/h (%d muestras)\n",
                                finalResults.length - i, arc.lineId, arc.originStopId, arc.destinationStopId,
                                arc.averageSpeed, arc.sampleCount);
                    }
                }

                // Guardar resultados
                System.out.print("\n¿Guardar resultados en archivo CSV? (s/n): ");
                String save = scanner.nextLine().trim().toLowerCase();
                if (save.equals("s") || save.equals("si")) {
                    String outputPath = "streaming_results_" + System.currentTimeMillis() + ".csv";
                    resultAggregator.persistResults(jobId, outputPath);
                    System.out.printf("✓ Resultados guardados en: %s\n", outputPath);
                }

            } catch (Exception e) {
                System.err.println("Error obteniendo resultados finales: " + e.getMessage());
            }

            System.out.println("\n✓ Modo streaming finalizado");

        } catch (Exception e) {
            System.err.println("✗ Error en modo streaming: " + e.getMessage());
            logger.error("Error en streaming", e);
            e.printStackTrace();
        }
    }

    private static void buildGraph(GraphBuilderPrx graphBuilder, Scanner scanner) {
        try {
            System.out.print("Ruta archivo lines (ej: /home/swarch/proyecto-mio/MIO/lines-241.csv): ");
            String linesFile = scanner.nextLine().trim();

            System.out.print("Ruta archivo stops (ej: /home/swarch/proyecto-mio/MIO/stops-241.csv): ");
            String stopsFile = scanner.nextLine().trim();

            System.out.print("Ruta archivo linestops (ej: /home/swarch/proyecto-mio/MIO/linestops-241.csv): ");
            String lineStopsFile = scanner.nextLine().trim();

            System.out.println("Construyendo grafo...");
            GraphInfo graphInfo = graphBuilder.buildGraph(linesFile, stopsFile, lineStopsFile);

            System.out.println("✓ Grafo construido exitosamente:");
            System.out.printf("  - Rutas: %d\n", graphInfo.totalRoutes);
            System.out.printf("  - Paradas: %d\n", graphInfo.totalStops);
            System.out.printf("  - Arcos: %d\n", graphInfo.totalArcs);

        } catch (Exception e) {
            System.err.println("✗ Error: " + e.getMessage());
            logger.error("Error construyendo grafo", e);
        }
    }

    private static void startProcessing(MasterCoordinatorPrx master, Scanner scanner) {
        try {
            System.out.print("Ruta archivo datagramas (ej: data/datagrams_1M.csv): ");
            String dataFile = scanner.nextLine().trim();

            // Validar que el archivo existe localmente primero
            File localFile = new File(dataFile);
            if (!localFile.exists()) {
                // Intentar ruta absoluta
                localFile = new File(System.getProperty("user.dir"), dataFile);
                if (!localFile.exists()) {
                    System.err.println("✗ Advertencia: Archivo no encontrado localmente: " + dataFile);
                    System.err.println("  Ruta buscada: " + localFile.getAbsolutePath());
                    System.out.print("¿Desea continuar de todas formas? (s/n): ");
                    String continuar = scanner.nextLine().trim().toLowerCase();
                    if (!continuar.equals("s") && !continuar.equals("si")) {
                        System.out.println("Operación cancelada");
                        return;
                    }
                } else {
                    dataFile = localFile.getAbsolutePath();
                    System.out.println("  Usando ruta absoluta: " + dataFile);
                }
            } else {
                dataFile = localFile.getAbsolutePath();
                System.out.println("  Archivo encontrado: " + dataFile + " (" + (localFile.length() / (1024 * 1024)) + " MB)");
            }

            System.out.print("Número de tareas/chunks (ej: 4): ");
            int numTasks = Integer.parseInt(scanner.nextLine().trim());

            if (numTasks < 1 || numTasks > 100) {
                System.err.println("✗ Número de tareas debe estar entre 1 y 100");
                return;
            }

            System.out.println("Iniciando procesamiento...");
            long startTime = System.currentTimeMillis();

            String jobId = master.startProcessing(dataFile, numTasks);

            System.out.printf("✓ Procesamiento iniciado. JobId: %s\n", jobId);
            System.out.println("Use opción 3 para consultar el estado.");

        } catch (ProcessingException e) {
            System.err.println("✗ Error de procesamiento: " + e.reason);
            logger.error("Error iniciando procesamiento", e);
        } catch (Exception e) {
            System.err.println("✗ Error: " + e.getMessage());
            logger.error("Error iniciando procesamiento", e);
            e.printStackTrace();
        }
    }

    private static void checkStatus(MasterCoordinatorPrx master, Scanner scanner) {
        try {
            System.out.print("JobId: ");
            String jobId = scanner.nextLine().trim();

            String status = master.getProcessingStatus(jobId);
            System.out.println("\n" + status);

        } catch (Exception e) {
            System.err.println("✗ Error: " + e.getMessage());
        }
    }

    private static void getResults(MasterCoordinatorPrx master, Scanner scanner) {
        try {
            System.out.print("JobId: ");
            String jobId = scanner.nextLine().trim();

            System.out.println("Obteniendo resultados...");
            ArcSpeed[] results = master.getResults(jobId);

            System.out.printf("\n✓ Resultados obtenidos: %d arcos\n", results.length);

            if (results.length > 0) {
                System.out.println("\nPrimeros 10 arcos:");
                System.out.println("LineId | Origin | Dest | Orient | Vel(km/h) | Dist(km) | Tiempo(s) | Muestras");
                System.out.println("--------------------------------------------------------------------------------");

                for (int i = 0; i < Math.min(10, results.length); i++) {
                    ArcSpeed arc = results[i];
                    System.out.printf("%6d | %6d | %4d | %6d | %9.2f | %8.3f | %9.1f | %8d\n",
                            arc.lineId, arc.originStopId, arc.destinationStopId, arc.orientation,
                            arc.averageSpeed, arc.distance, arc.averageTime, arc.sampleCount);
                }
            }

        } catch (Exception e) {
            System.err.println("✗ Error: " + e.getMessage());
        }
    }

    private static void saveResults(ResultAggregatorPrx resultAggregator, Scanner scanner) {
        try {
            System.out.print("JobId: ");
            String jobId = scanner.nextLine().trim();

            System.out.print("Ruta archivo salida (ej: results.csv): ");
            String outputPath = scanner.nextLine().trim();

            System.out.println("Guardando resultados...");
            resultAggregator.persistResults(jobId, outputPath);

            System.out.printf("✓ Resultados guardados en: %s\n", outputPath);

        } catch (Exception e) {
            System.err.println("✗ Error: " + e.getMessage());
        }
    }

    private static void queryArcSpeed(QueryServicePrx queryService, Scanner scanner) {
        try {
            System.out.print("Line ID: ");
            int lineId = Integer.parseInt(scanner.nextLine().trim());

            System.out.print("Origin Stop ID: ");
            long originStop = Long.parseLong(scanner.nextLine().trim());

            System.out.print("Destination Stop ID: ");
            long destStop = Long.parseLong(scanner.nextLine().trim());

            System.out.print("Orientation (0 o 1): ");
            int orientation = Integer.parseInt(scanner.nextLine().trim());

            ArcSpeed arc = queryService.getArcSpeed(lineId, originStop, destStop, orientation);

            System.out.println("\n✓ Velocidad del arco:");
            System.out.printf("  Velocidad promedio: %.2f km/h\n", arc.averageSpeed);
            System.out.printf("  Distancia: %.3f km\n", arc.distance);
            System.out.printf("  Tiempo promedio: %.1f segundos\n", arc.averageTime);
            System.out.printf("  Número de muestras: %d\n", arc.sampleCount);

        } catch (Exception e) {
            System.err.println("✗ Error: " + e.getMessage());
        }
    }

    private static void queryRouteSpeed(QueryServicePrx queryService, Scanner scanner) {
        try {
            System.out.print("Line ID: ");
            int lineId = Integer.parseInt(scanner.nextLine().trim());

            System.out.print("Orientation (0 o 1): ");
            int orientation = Integer.parseInt(scanner.nextLine().trim());

            ArcSpeed[] arcs = queryService.getRouteSpeed(lineId, orientation);

            System.out.printf("\n✓ Velocidades de la ruta %d (orientación %d): %d arcos\n",
                    lineId, orientation, arcs.length);

            if (arcs.length > 0) {
                double avgSpeed = 0;
                for (ArcSpeed arc : arcs) {
                    avgSpeed += arc.averageSpeed;
                }
                avgSpeed /= arcs.length;

                System.out.printf("  Velocidad promedio de la ruta: %.2f km/h\n", avgSpeed);
                System.out.println("\nArcos:");

                for (int i = 0; i < Math.min(15, arcs.length); i++) {
                    ArcSpeed arc = arcs[i];
                    System.out.printf("  %d -> %d: %.2f km/h\n",
                            arc.originStopId, arc.destinationStopId, arc.averageSpeed);
                }

                if (arcs.length > 15) {
                    System.out.printf("  ... y %d arcos más\n", arcs.length - 15);
                }
            }

        } catch (Exception e) {
            System.err.println("✗ Error: " + e.getMessage());
        }
    }

    private static void showStatistics(QueryServicePrx queryService) {
        try {
            String stats = queryService.getSystemStatistics();
            System.out.println("\n" + stats);

        } catch (Exception e) {
            System.err.println("✗ Error: " + e.getMessage());
        }
    }

    private static void runCompleteTest(GraphBuilderPrx graphBuilder, MasterCoordinatorPrx master,
                                        ResultAggregatorPrx resultAggregator, Scanner scanner) {
        try {
            System.out.println("\n========== Prueba Completa del Sistema ==========\n");

            // Paso 1: Construir grafo
            System.out.println("Paso 1: Construyendo grafo...");
            GraphInfo graphInfo = graphBuilder.buildGraph(
                    "/home/swarch/proyecto-mio/MIO/lines-241.csv",
                    "/home/swarch/proyecto-mio/MIO/stops-241.csv",
                    "/home/swarch/proyecto-mio/MIO/linestops-241.csv"
            );
            System.out.printf("✓ Grafo: %d rutas, %d paradas, %d arcos\n\n",
                    graphInfo.totalRoutes, graphInfo.totalStops, graphInfo.totalArcs);

            // Paso 2: Iniciar procesamiento
            System.out.print("Número de workers/tareas (recomendado: 10): ");
            int numTasks = Integer.parseInt(scanner.nextLine().trim());

            System.out.println("\nPaso 2: Iniciando procesamiento...");
            long startTime = System.currentTimeMillis();

            String jobId = master.startProcessing(
                    "/home/swarch/proyecto-mio/MIO/datagrams4history.csv",
                    numTasks
            );
            System.out.printf("✓ JobId: %s\n\n", jobId);

            // Paso 3: Monitorear progreso
            System.out.println("Paso 3: Monitoreando progreso...");
            boolean completed = false;

            while (!completed) {
                Thread.sleep(5000); // Consultar cada 5 segundos

                String status = master.getProcessingStatus(jobId);
                System.out.println(status);
                System.out.println("---");

                if (status.contains("COMPLETED") || status.contains("PARTIAL") ||
                        status.contains("FAILED")) {
                    completed = true;
                }
            }

            long endTime = System.currentTimeMillis();
            double totalTime = (endTime - startTime) / 1000.0;

            // Paso 4: Obtener resultados
            System.out.println("\nPaso 4: Obteniendo resultados...");
            ArcSpeed[] results = master.getResults(jobId);
            System.out.printf("✓ %d arcos calculados\n\n", results.length);

            // Paso 5: Guardar resultados
            String outputPath = "results_" + System.currentTimeMillis() + ".csv";
            System.out.println("Paso 5: Guardando resultados en " + outputPath);
            resultAggregator.persistResults(jobId, outputPath);
            System.out.println("✓ Resultados guardados\n");

            // Resumen final
            System.out.println("========== Resumen ==========");
            System.out.printf("Tiempo total: %.2f segundos\n", totalTime);
            System.out.printf("Arcos calculados: %d\n", results.length);
            System.out.printf("Throughput: %.2f arcos/segundo\n", results.length / totalTime);
            System.out.printf("Archivo resultados: %s\n", outputPath);
            System.out.println("==============================\n");

        } catch (Exception e) {
            System.err.println("✗ Error en prueba completa: " + e.getMessage());
            logger.error("Error en prueba completa", e);
        }
    }

    private static MasterCoordinatorPrx getMasterProxy(Communicator communicator, String masterHost) {
        ObjectPrx base = communicator.stringToProxy(String.format(MASTER_PROXY_TEMPLATE, masterHost));
        return MasterCoordinatorPrx.checkedCast(base);
    }

    private static GraphBuilderPrx getGraphBuilderProxy(Communicator communicator, String masterHost) {
        ObjectPrx base = communicator.stringToProxy(String.format(GRAPH_BUILDER_PROXY_TEMPLATE, masterHost));
        return GraphBuilderPrx.checkedCast(base);
    }

    private static QueryServicePrx getQueryServiceProxy(Communicator communicator, String masterHost) {
        ObjectPrx base = communicator.stringToProxy(String.format(QUERY_SERVICE_PROXY_TEMPLATE, masterHost));
        return QueryServicePrx.checkedCast(base);
    }

    private static ResultAggregatorPrx getResultAggregatorProxy(Communicator communicator, String masterHost) {
        ObjectPrx base = communicator.stringToProxy(String.format(RESULT_AGGREGATOR_PROXY_TEMPLATE, masterHost));
        return ResultAggregatorPrx.checkedCast(base);
    }

    private static StreamBrokerPrx getStreamBrokerProxy(Communicator communicator, String masterHost) {
        ObjectPrx base = communicator.stringToProxy(String.format(STREAM_BROKER_PROXY_TEMPLATE, masterHost));
        return StreamBrokerPrx.checkedCast(base);
    }

    private static StreamCoordinatorPrx getStreamCoordinatorProxy(Communicator communicator, String masterHost) {
        ObjectPrx base = communicator.stringToProxy(String.format(STREAM_COORDINATOR_PROXY_TEMPLATE, masterHost));
        return StreamCoordinatorPrx.checkedCast(base);
    }
}