package com.mio.swarch;

import MIO.*;
import com.zeroc.Ice.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.lang.Exception;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerNode {
    private static final Logger logger = LoggerFactory.getLogger(WorkerNode.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Uso: WorkerNode <worker-id> <puerto>");
            System.exit(1);
        }

        String workerId = args[0];
        int port = Integer.parseInt(args[1]);

        // Obtener IP local desde variable de entorno o detectar automáticamente
        String workerHost = System.getenv("WORKER_HOST");
        if (workerHost == null || workerHost.isEmpty()) {
            workerHost = "0.0.0.0"; // Escuchar en todas las interfaces
        }

        try (Communicator communicator = Util.initialize(args, "config/worker.properties")) {
            ObjectAdapter adapter = communicator.createObjectAdapterWithEndpoints(
                    "WorkerAdapter", "tcp -h " + workerHost + " -p " + port
            );

            WorkerImpl worker = new WorkerImpl(workerId);
            ObjectPrx proxy = adapter.add(worker, Util.stringToIdentity("Worker" + workerId));

            adapter.activate();

            logger.info("════════════════════════════════════════════════════════");
            logger.info("Worker {} iniciado en puerto {}", workerId, port);
            logger.info("Escuchando en: {}:{}", workerHost, port);
            logger.info("════════════════════════════════════════════════════════");

            // Registrarse con el Master
            registerWithMaster(communicator, workerId, proxy);

            communicator.waitForShutdown();

        } catch (Exception e) {
            logger.error("Error en Worker " + workerId, e);
            System.exit(1);
        }
    }

    private static void registerWithMaster(Communicator communicator, String workerId, ObjectPrx proxy) {
        try {
            // Obtener IP del Master desde variable de entorno o usar por defecto
            String masterHost = System.getenv("MASTER_HOST");
            if (masterHost == null || masterHost.isEmpty()) {
                masterHost = "192.168.131.101"; // IP del master (104m01)
            }

            logger.info("Conectando al Master en {}:10000...", masterHost);

            ObjectPrx base = communicator.stringToProxy("MasterCoordinator:tcp -h " + masterHost + " -p 10000");
            MasterCoordinatorPrx master = MasterCoordinatorPrx.checkedCast(base);

            if (master == null) {
                throw new RuntimeException("Proxy inválido para MasterCoordinator");
            }

            WorkerPrx workerProxy = WorkerPrx.uncheckedCast(proxy);
            master.registerWorker(workerProxy, workerId);

            logger.info("✓ Worker {} registrado exitosamente con el Master", workerId);

        } catch (Exception e) {
            logger.error("✗ Error registrando worker con Master: {}", e.getMessage());
            logger.error("  Verifique que el Master esté corriendo en {}:10000",
                    System.getenv("MASTER_HOST") != null ? System.getenv("MASTER_HOST") : "192.168.131.101");
        }
    }
}

class WorkerImpl implements Worker {
    private static final Logger logger = LoggerFactory.getLogger(WorkerImpl.class);

    private final String workerId;
    private volatile boolean available = true;

    // Constantes para cálculo de velocidad
    private static final double EARTH_RADIUS_KM = 6371.0;
    private static final int MIN_TIME_DIFF_SECONDS = 10; // Mínimo tiempo entre puntos
    private static final double MAX_SPEED_KMH = 120.0; // Velocidad máxima razonable

    public WorkerImpl(String workerId) {
        this.workerId = workerId;
    }

    @Override
    public ProcessingResult processTask(ProcessingTask task, GraphInfo graphInfo, Current current)
            throws ProcessingException {
        logger.info("Worker {} procesando task {}", workerId, task.taskId);

        ProcessingResult result = new ProcessingResult();
        result.taskId = task.taskId;
        result.success = false;

        long startTime = System.currentTimeMillis();

        try {
            available = false;

            // Construir mapa de paradas para búsqueda rápida
            Map<Long, Stop> stopMap = buildStopMap(graphInfo.stops);

            // Construir mapa de arcos válidos
            Map<String, ArcInfo> arcMap = buildArcMap(graphInfo.lineStops);

            // Procesar datagramas
            Map<String, ArcSpeedAccumulator> speedAccumulators = new ConcurrentHashMap<>();
            int processedRecords = 0;

            Map<String, DatagramTracker> busTrackers = new HashMap<>();

            // Verificar si recibimos datos remotos o debemos leer archivo local
            if (task.datagramLines != null && task.datagramLines.length > 0) {
                // Modo REMOTO: Procesar datos enviados por el Master
                logger.info("Modo REMOTO: Procesando {} líneas enviadas por el Master", task.datagramLines.length);

                for (String line : task.datagramLines) {
                    if (line == null || line.trim().isEmpty()) continue;

                    try {
                        Datagram datagram = parseDatagram(line);
                        processedRecords++;

                        // Filtrar datagramas inválidos
                        if (datagram.stopId <= 0 || datagram.lineId <= 0) {
                            continue;
                        }

                        // Clave del bus
                        String busKey = datagram.busId + "-" + datagram.lineId;
                        DatagramTracker tracker = busTrackers.get(busKey);

                        if (tracker == null) {
                            tracker = new DatagramTracker(datagram);
                            busTrackers.put(busKey, tracker);
                        } else {
                            // Calcular velocidad entre paradas
                            calculateArcSpeed(tracker.lastDatagram, datagram, stopMap,
                                    arcMap, speedAccumulators);
                            tracker.lastDatagram = datagram;
                        }

                    } catch (Exception e) {
                        // Log y continuar con siguiente registro
                        if (processedRecords % 100000 == 0) {
                            logger.debug("Error en línea {}: {}", processedRecords, e.getMessage());
                        }
                    }
                }

            } else if (task.filePath != null && !task.filePath.isEmpty()) {
                // Modo LOCAL: Leer desde archivo (compatibilidad hacia atrás)
                logger.info("Modo LOCAL: Leyendo desde archivo {}", task.filePath);

                try (RandomAccessFile raf = new RandomAccessFile(task.filePath, "r")) {
                    raf.seek(task.startOffset);

                    String line;
                    while (raf.getFilePointer() < task.endOffset && (line = raf.readLine()) != null) {
                        try {
                            Datagram datagram = parseDatagram(line);
                            processedRecords++;

                            if (datagram.stopId <= 0 || datagram.lineId <= 0) {
                                continue;
                            }

                            String busKey = datagram.busId + "-" + datagram.lineId;
                            DatagramTracker tracker = busTrackers.get(busKey);

                            if (tracker == null) {
                                tracker = new DatagramTracker(datagram);
                                busTrackers.put(busKey, tracker);
                            } else {
                                calculateArcSpeed(tracker.lastDatagram, datagram, stopMap,
                                        arcMap, speedAccumulators);
                                tracker.lastDatagram = datagram;
                            }

                        } catch (Exception e) {
                            if (processedRecords % 100000 == 0) {
                                logger.debug("Error en línea {}: {}", processedRecords, e.getMessage());
                            }
                        }
                    }
                }
            } else {
                throw new ProcessingException("Task sin datos: ni datagramLines ni filePath especificados");
            }

            // Convertir acumuladores a resultados finales
            result.partialResults = new HashMap<>();
            for (Map.Entry<String, ArcSpeedAccumulator> entry : speedAccumulators.entrySet()) {
                ArcSpeedAccumulator acc = entry.getValue();

                if (acc.count > 0) {
                    ArcSpeed arcSpeed = new ArcSpeed();
                    arcSpeed.originStopId = acc.originStopId;
                    arcSpeed.destinationStopId = acc.destinationStopId;
                    arcSpeed.lineId = acc.lineId;
                    arcSpeed.orientation = acc.orientation;
                    arcSpeed.averageSpeed = acc.totalSpeed / acc.count;
                    arcSpeed.averageTime = acc.totalTime / acc.count;
                    arcSpeed.distance = acc.totalDistance / acc.count;
                    arcSpeed.sampleCount = acc.count;

                    result.partialResults.put(entry.getKey(), arcSpeed);
                }
            }

            result.processedRecords = processedRecords;
            result.processingTime = (System.currentTimeMillis() - startTime) / 1000.0;
            result.success = true;

            logger.info("Task {} completada: {} registros, {} arcos calculados, {} segundos",
                    task.taskId, processedRecords, result.partialResults.size(),
                    String.format("%.2f", result.processingTime));

        } catch (Exception e) {
            result.errorMessage = e.getMessage();
            logger.error("Error procesando task " + task.taskId, e);
            throw new ProcessingException("Error: " + e.getMessage());

        } finally {
            available = true;
        }

        return result;
    }

    private Datagram parseDatagram(String line) {
        String[] parts = line.split(",");
        Datagram d = new Datagram();

        d.eventType = parseInt(parts[0]);
        d.registerDate = parts[1];
        d.stopId = parseLong(parts[2]);
        d.odometer = parseInt(parts[3]);
        d.latitude = parseDouble(parts[4]) / 1_000_000.0; // Convertir a decimal
        d.longitude = parseDouble(parts[5]) / 1_000_000.0;
        d.taskId = parseInt(parts[6]);
        d.lineId = parseInt(parts[7]);
        d.tripId = parseInt(parts[8]);
        d.unknown1 = parseLong(parts[9]);
        d.datagramDate = parts[10];
        d.busId = parseInt(parts[11]);

        return d;
    }

    private void calculateArcSpeed(Datagram prev, Datagram current, Map<Long, Stop> stopMap,
                                   Map<String, ArcInfo> arcMap,
                                   Map<String, ArcSpeedAccumulator> speedAccumulators) {

        // Verificar que son paradas diferentes
        if (prev.stopId == current.stopId) {
            return;
        }

        // Verificar que el arco existe en el grafo
        String arcKey = buildArcKey(current.lineId, prev.stopId, current.stopId);
        ArcInfo arcInfo = arcMap.get(arcKey);

        if (arcInfo == null) {
            return; // Arco no válido en la ruta
        }

        // Calcular distancia entre puntos GPS
        double distance = haversineDistance(
                prev.latitude, prev.longitude,
                current.latitude, current.longitude
        );

        // Calcular tiempo transcurrido
        long timeDiff = calculateTimeDiff(prev.datagramDate, current.datagramDate);

        // Validar mediciones
        if (timeDiff < MIN_TIME_DIFF_SECONDS || distance <= 0) {
            return;
        }

        // Calcular velocidad (km/h)
        double speed = (distance / timeDiff) * 3600.0;

        // Filtrar velocidades irreales
        if (speed > MAX_SPEED_KMH || speed < 0) {
            return;
        }

        // Acumular resultado
        // ANTES de agregar a la lista se verifica si ya esta o no esta en la lista ese registro mediante
        // funcion lambda
        String key = buildArcKey(current.lineId, prev.stopId, current.stopId, arcInfo.orientation);
        ArcSpeedAccumulator acc = speedAccumulators.computeIfAbsent(key,
                k -> new ArcSpeedAccumulator(prev.stopId, current.stopId, current.lineId, arcInfo.orientation));

        acc.addMeasurement(speed, distance, timeDiff);
    }

    private double haversineDistance(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS_KM * c;
    }

    private long calculateTimeDiff(String date1, String date2) {
        try {
            // Formato: "2019-05-27 20:14:43"
            java.time.LocalDateTime dt1 = java.time.LocalDateTime.parse(date1.replace(" ", "T"));
            java.time.LocalDateTime dt2 = java.time.LocalDateTime.parse(date2.replace(" ", "T"));
            return Math.abs(java.time.Duration.between(dt1, dt2).getSeconds());
        } catch (Exception e) {
            return 0;
        }
    }

    private Map<Long, Stop> buildStopMap(Stop[] stops) {
        Map<Long, Stop> map = new HashMap<>();
        for (Stop stop : stops) {
            map.put(stop.stopId, stop);
        }
        return map;
    }

    private Map<String, ArcInfo> buildArcMap(LineStop[] lineStops) {
        Map<String, List<LineStop>> routeMap = new HashMap<>();

        // Agrupar por lineId y orientation
        for (LineStop ls : lineStops) {
            String key = ls.lineId + "-" + ls.orientation;
            routeMap.computeIfAbsent(key, k -> new ArrayList<>()).add(ls);
        }

        Map<String, ArcInfo> arcMap = new HashMap<>();

        // Para cada ruta, ordenar por secuencia y crear arcos
        for (Map.Entry<String, List<LineStop>> entry : routeMap.entrySet()) {
            List<LineStop> stops = entry.getValue();
            stops.sort(Comparator.comparingInt(ls -> ls.stopSequence));

            for (int i = 0; i < stops.size() - 1; i++) {
                LineStop from = stops.get(i);
                LineStop to = stops.get(i + 1);

                String arcKey = buildArcKey(from.lineId, from.stopId, to.stopId);
                arcMap.put(arcKey, new ArcInfo(from.lineId, from.orientation));
            }
        }

        return arcMap;
    }

    private String buildArcKey(int lineId, long originId, long destId) {
        return lineId + "-" + originId + "-" + destId;
    }

    private String buildArcKey(int lineId, long originId, long destId, int orientation) {
        return lineId + "-" + originId + "-" + destId + "-" + orientation;
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

    @Override
    public boolean isAvailable(Current current) {
        return available;
    }

    @Override
    public String getWorkerId(Current current) {
        return workerId;
    }

    @Override
    public void shutdown(Current current) {
        logger.info("Worker {} recibió señal de apagado", workerId);
        current.adapter.getCommunicator().shutdown();
    }
}

class DatagramTracker {
    Datagram lastDatagram;

    DatagramTracker(Datagram initial) {
        this.lastDatagram = initial;
    }
}

class ArcInfo {
    int lineId;
    int orientation;

    ArcInfo(int lineId, int orientation) {
        this.lineId = lineId;
        this.orientation = orientation;
    }
}

class ArcSpeedAccumulator {
    long originStopId;
    long destinationStopId;
    int lineId;
    int orientation;
    double totalSpeed = 0;
    double totalDistance = 0;
    double totalTime = 0;
    int count = 0;

    ArcSpeedAccumulator(long originStopId, long destinationStopId, int lineId, int orientation) {
        this.originStopId = originStopId;
        this.destinationStopId = destinationStopId;
        this.lineId = lineId;
        this.orientation = orientation;
    }

    synchronized void addMeasurement(double speed, double distance, double time) {
        totalSpeed += speed;
        totalDistance += distance;
        totalTime += time;
        count++;
    }
}