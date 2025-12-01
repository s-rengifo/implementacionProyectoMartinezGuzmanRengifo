package com.mio.swarch;

import MIO.*;
import com.zeroc.Ice.Current;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Servicio de agregación de resultados parciales de workers.
 * Implementa el patrón Reduce del Map-Reduce.
 *
 * Responsabilidades:
 * - Recibir resultados parciales de workers
 * - Agregar velocidades usando promedios ponderados
 * - Calcular estadísticas globales
 * - Persistir resultados en formato CSV
 */
public class ResultAggregatorService implements ResultAggregator {
    private static final Logger logger = LoggerFactory.getLogger(ResultAggregatorService.class);

    // Estructura principal: jobId -> Map<arcKey, ArcSpeedAggregator>
    // Permite múltiples jobs en paralelo
    private final Map<String, JobResults> jobResultsMap = new ConcurrentHashMap<>();

    /**
     * Agrega un resultado parcial de un worker al job especificado.
     * Thread-safe: múltiples workers pueden reportar simultáneamente.
     */
    @Override
    public void addPartialResult(String jobId, ProcessingResult result, Current current) {
        if (jobId == null || result == null) {
            logger.warn("Resultado parcial inválido: jobId={}, result={}", jobId, result);
            return;
        }

        logger.debug("Agregando resultado parcial de task {} para job {} ({} arcos)",
                result.taskId, jobId, result.partialResults.size());

        // Obtener o crear JobResults para este job
        JobResults jobResults = jobResultsMap.computeIfAbsent(jobId, k -> new JobResults(jobId));

        // Timestamp de última actualización
        jobResults.updateTimestamp();

        // Agregar cada arco del resultado parcial
        int arcosAgregados = 0;
        for (Map.Entry<String, ArcSpeed> entry : result.partialResults.entrySet()) {
            String arcKey = entry.getKey();
            ArcSpeed partialSpeed = entry.getValue();

            // Validar datos
            if (partialSpeed == null || partialSpeed.sampleCount <= 0) {
                logger.debug("Arco inválido ignorado: {}", arcKey);
                continue;
            }

            // Obtener o crear agregador para este arco
            ArcSpeedAggregator aggregator = jobResults.arcAggregators.computeIfAbsent(
                    arcKey,
                    k -> new ArcSpeedAggregator(
                            partialSpeed.originStopId,
                            partialSpeed.destinationStopId,
                            partialSpeed.lineId,
                            partialSpeed.orientation
                    )
            );

            // Agregar resultado parcial usando promedio ponderado
            aggregator.addPartialResult(partialSpeed);
            arcosAgregados++;
        }

        // Actualizar estadísticas del job
        jobResults.totalTasksProcessed++;
        jobResults.totalRecordsProcessed += result.processedRecords;
        jobResults.totalProcessingTime += result.processingTime;

        logger.info("Job {}: Task {} agregada. Total arcos: {}, Registros: {}, Tiempo: {} seg",
                jobId, result.taskId, jobResults.arcAggregators.size(),
                jobResults.totalRecordsProcessed, String.format("%.2f", result.processingTime));
    }

    /**
     * Obtiene los resultados finales calculados para un job.
     * Calcula promedios ponderados de todos los resultados parciales.
     */
    @Override
    public ArcSpeed[] getFinalResults(String jobId, Current current) throws DataNotFoundException {
        JobResults jobResults = jobResultsMap.get(jobId);

        if (jobResults == null) {
            String msg = "No hay resultados disponibles para job: " + jobId;
            logger.error(msg);
            throw new DataNotFoundException(msg);
        }

        if (jobResults.arcAggregators.isEmpty()) {
            String msg = "Job " + jobId + " no tiene arcos calculados";
            logger.warn(msg);
            throw new DataNotFoundException(msg);
        }

        logger.info("Calculando resultados finales para job {}: {} arcos",
                jobId, jobResults.arcAggregators.size());

        // Convertir agregadores a resultados finales
        List<ArcSpeed> results = new ArrayList<>(jobResults.arcAggregators.size());

        for (ArcSpeedAggregator aggregator : jobResults.arcAggregators.values()) {
            ArcSpeed arcSpeed = aggregator.computeFinalSpeed();
            results.add(arcSpeed);
        }

        // Ordenar resultados por lineId, luego por originStopId, luego por destStopId
        results.sort(Comparator
                .comparingInt((ArcSpeed a) -> a.lineId)
                .thenComparingLong(a -> a.originStopId)
                .thenComparingLong(a -> a.destinationStopId)
                .thenComparingInt(a -> a.orientation)
        );

        logger.info("Job {}: Retornando {} resultados finales ordenados", jobId, results.size());

        return results.toArray(new ArcSpeed[0]);
    }

    /**
     * Persiste los resultados de un job en un archivo CSV.
     */
    @Override
    public void persistResults(String jobId, String outputPath, Current current)
            throws ProcessingException {
        try {
            logger.info("Persistiendo resultados del job {} en: {}", jobId, outputPath);

            ArcSpeed[] results = getFinalResults(jobId, current);

            try (PrintWriter writer = new PrintWriter(new FileWriter(outputPath))) {
                // Escribir header CSV
                writer.println("lineId,originStopId,destinationStopId,orientation," +
                        "averageSpeed,distance,averageTime,sampleCount");

                // Escribir cada arco
                for (ArcSpeed arc : results) {
                    writer.printf("%d,%d,%d,%d,%.4f,%.4f,%.2f,%d%n",
                            arc.lineId,
                            arc.originStopId,
                            arc.destinationStopId,
                            arc.orientation,
                            arc.averageSpeed,
                            arc.distance,
                            arc.averageTime,
                            arc.sampleCount
                    );
                }
            }

            logger.info("✓ Resultados del job {} persistidos exitosamente: {} arcos en {}",
                    jobId, results.length, outputPath);

        } catch (DataNotFoundException e) {
            logger.error("Job no encontrado: {}", jobId);
            throw new ProcessingException("Job no encontrado: " + jobId);
        } catch (Exception e) {
            logger.error("Error persistiendo resultados del job " + jobId, e);
            throw new ProcessingException("Error: " + e.getMessage());
        }
    }

    // ========== Métodos auxiliares para consultas ==========

    /**
     * Obtiene la velocidad de un arco específico.
     */
    public ArcSpeed getArcSpeed(String jobId, int lineId, long originStop, long destStop, int orientation)
            throws DataNotFoundException {
        JobResults jobResults = jobResultsMap.get(jobId);

        if (jobResults == null) {
            throw new DataNotFoundException("Job no encontrado: " + jobId);
        }

        String key = buildArcKey(lineId, originStop, destStop, orientation);
        ArcSpeedAggregator aggregator = jobResults.arcAggregators.get(key);

        if (aggregator == null) {
            throw new DataNotFoundException("Arco no encontrado: " + key);
        }

        return aggregator.computeFinalSpeed();
    }

    /**
     * Obtiene todas las velocidades de una ruta específica.
     */
    public List<ArcSpeed> getRouteSpeed(String jobId, int lineId, int orientation) {
        JobResults jobResults = jobResultsMap.get(jobId);

        if (jobResults == null) {
            return Collections.emptyList();
        }

        List<ArcSpeed> routeSpeeds = jobResults.arcAggregators.values().stream()
                .filter(agg -> agg.lineId == lineId && agg.orientation == orientation)
                .map(ArcSpeedAggregator::computeFinalSpeed)
                .sorted(Comparator.comparingLong(a -> a.originStopId))
                .collect(Collectors.toList());

        logger.debug("Ruta {}-{}: {} arcos encontrados", lineId, orientation, routeSpeeds.size());

        return routeSpeeds;
    }

    /**
     * Obtiene velocidades de arcos que incluyen paradas específicas (zona).
     */
    public List<ArcSpeed> getZoneSpeeds(String jobId, long[] stopIds) {
        JobResults jobResults = jobResultsMap.get(jobId);

        if (jobResults == null) {
            return Collections.emptyList();
        }

        Set<Long> stopIdSet = new HashSet<>();
        for (long stopId : stopIds) {
            stopIdSet.add(stopId);
        }

        List<ArcSpeed> zoneSpeeds = jobResults.arcAggregators.values().stream()
                .filter(agg -> stopIdSet.contains(agg.originStopId) || stopIdSet.contains(agg.destinationStopId))
                .map(ArcSpeedAggregator::computeFinalSpeed)
                .sorted(Comparator
                        .comparingInt((ArcSpeed a) -> a.lineId)
                        .thenComparingLong(a -> a.originStopId))
                .collect(Collectors.toList());

        logger.debug("Zona con {} paradas: {} arcos encontrados", stopIds.length, zoneSpeeds.size());

        return zoneSpeeds;
    }

    /**
     * Genera estadísticas generales de un job.
     */
    public String getStatistics(String jobId) {
        JobResults jobResults = jobResultsMap.get(jobId);

        if (jobResults == null) {
            return "Job no encontrado: " + jobId;
        }

        if (jobResults.arcAggregators.isEmpty()) {
            return "Job " + jobId + " no tiene resultados aún.";
        }

        // Calcular estadísticas
        int totalArcs = jobResults.arcAggregators.size();
        int totalSamples = 0;
        double totalAvgSpeed = 0;
        double minSpeed = Double.MAX_VALUE;
        double maxSpeed = Double.MIN_VALUE;
        double totalDistance = 0;
        double totalTime = 0;

        Map<Integer, Integer> arcsByLine = new HashMap<>();

        for (ArcSpeedAggregator aggregator : jobResults.arcAggregators.values()) {
            ArcSpeed arc = aggregator.computeFinalSpeed();

            totalSamples += arc.sampleCount;
            totalAvgSpeed += arc.averageSpeed;
            totalDistance += arc.distance;
            totalTime += arc.averageTime;

            minSpeed = Math.min(minSpeed, arc.averageSpeed);
            maxSpeed = Math.max(maxSpeed, arc.averageSpeed);

            arcsByLine.put(arc.lineId, arcsByLine.getOrDefault(arc.lineId, 0) + 1);
        }

        double overallAvgSpeed = totalArcs > 0 ? totalAvgSpeed / totalArcs : 0;
        double avgDistance = totalArcs > 0 ? totalDistance / totalArcs : 0;
        double avgTime = totalArcs > 0 ? totalTime / totalArcs : 0;

        // Top 5 rutas con más arcos
        List<Map.Entry<Integer, Integer>> topRoutes = arcsByLine.entrySet().stream()
                .sorted(Map.Entry.<Integer, Integer>comparingByValue().reversed())
                .limit(5)
                .collect(Collectors.toList());

        StringBuilder stats = new StringBuilder();
        stats.append("╔═══════════════════════════════════════════════════════════╗\n");
        stats.append(String.format("║  Estadísticas del Job: %-34s ║\n", jobId.substring(0, Math.min(34, jobId.length()))));
        stats.append("╠═══════════════════════════════════════════════════════════╣\n");
        stats.append(String.format("║  Total de arcos calculados:      %,10d              ║\n", totalArcs));
        stats.append(String.format("║  Total de muestras:              %,10d              ║\n", totalSamples));
        stats.append(String.format("║  Tareas procesadas:              %,10d              ║\n", jobResults.totalTasksProcessed));
        stats.append(String.format("║  Registros procesados:           %,10d              ║\n", jobResults.totalRecordsProcessed));
        stats.append("╠═══════════════════════════════════════════════════════════╣\n");
        stats.append(String.format("║  Velocidad promedio general:     %8.2f km/h         ║\n", overallAvgSpeed));
        stats.append(String.format("║  Velocidad mínima:               %8.2f km/h         ║\n", minSpeed));
        stats.append(String.format("║  Velocidad máxima:               %8.2f km/h         ║\n", maxSpeed));
        stats.append(String.format("║  Distancia promedio:             %8.3f km           ║\n", avgDistance));
        stats.append(String.format("║  Tiempo promedio:                %8.1f seg          ║\n", avgTime));
        stats.append("╠═══════════════════════════════════════════════════════════╣\n");
        stats.append("║  Top 5 Rutas con más arcos:                              ║\n");

        for (int i = 0; i < topRoutes.size(); i++) {
            Map.Entry<Integer, Integer> entry = topRoutes.get(i);
            stats.append(String.format("║    %d. Ruta %-5d: %,6d arcos                         ║\n",
                    i + 1, entry.getKey(), entry.getValue()));
        }

        stats.append("╠═══════════════════════════════════════════════════════════╣\n");
        stats.append(String.format("║  Tiempo total procesamiento:     %8.2f seg          ║\n",
                jobResults.totalProcessingTime));
        stats.append(String.format("║  Throughput:                     %,10.0f rec/seg     ║\n",
                jobResults.totalRecordsProcessed / Math.max(1, jobResults.totalProcessingTime)));
        stats.append(String.format("║  Última actualización:           %-22s ║\n",
                new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(jobResults.lastUpdateTime)));
        stats.append("╚═══════════════════════════════════════════════════════════╝\n");

        return stats.toString();
    }

    /**
     * Limpia los resultados de un job (útil para liberar memoria).
     */
    public void clearJobResults(String jobId) {
        JobResults removed = jobResultsMap.remove(jobId);
        if (removed != null) {
            logger.info("Resultados del job {} eliminados de memoria", jobId);
        }
    }

    /**
     * Obtiene lista de todos los jobs activos.
     */
    public Set<String> getActiveJobs() {
        return new HashSet<>(jobResultsMap.keySet());
    }

    /**
     * Construye clave única para un arco.
     */
    private String buildArcKey(int lineId, long originId, long destId, int orientation) {
        return lineId + "-" + originId + "-" + destId + "-" + orientation;
    }
}

/**
 * Contenedor de resultados para un job específico.
 * Mantiene todos los agregadores de arcos y metadatos del job.
 */
class JobResults {
    final String jobId;
    final Map<String, ArcSpeedAggregator> arcAggregators;

    // Metadatos y estadísticas
    int totalTasksProcessed = 0;
    long totalRecordsProcessed = 0;
    double totalProcessingTime = 0.0;
    long lastUpdateTime;
    final long creationTime;

    JobResults(String jobId) {
        this.jobId = jobId;
        this.arcAggregators = new ConcurrentHashMap<>();
        this.creationTime = System.currentTimeMillis();
        this.lastUpdateTime = creationTime;
    }

    void updateTimestamp() {
        this.lastUpdateTime = System.currentTimeMillis();
    }
}

/**
 * Agregador de velocidades para un arco específico.
 * Implementa agregación thread-safe con promedios ponderados.
 *
 * Fórmula de agregación:
 * velocidad_final = Σ(velocidad_i × muestras_i) / Σ(muestras_i)
 */
class ArcSpeedAggregator {
    final long originStopId;
    final long destinationStopId;
    final int lineId;
    final int orientation;

    // Acumuladores ponderados (synchronized)
    private double totalWeightedSpeed = 0;
    private double totalWeightedDistance = 0;
    private double totalWeightedTime = 0;
    private int totalSamples = 0;

    ArcSpeedAggregator(long originStopId, long destinationStopId, int lineId, int orientation) {
        this.originStopId = originStopId;
        this.destinationStopId = destinationStopId;
        this.lineId = lineId;
        this.orientation = orientation;
    }

    /**
     * Agrega un resultado parcial usando promedio ponderado.
     * Thread-safe: permite múltiples workers reportando simultáneamente.
     */
    // Se marca como synchronized para cuando un hilo ejecute el metodo los demas
    // hilos que intenten ejececutarlo , tendran que esperar a que el primero termine,
    // asegurando consistencia de los datos
    synchronized void addPartialResult(ArcSpeed partialSpeed) {
        if (partialSpeed == null || partialSpeed.sampleCount <= 0) {
            return;
        }

        int weight = partialSpeed.sampleCount;

        // Acumular valores ponderados
        totalWeightedSpeed += partialSpeed.averageSpeed * weight;
        totalWeightedDistance += partialSpeed.distance * weight;
        totalWeightedTime += partialSpeed.averageTime * weight;
        totalSamples += weight;
    }

    /**
     * Calcula la velocidad final del arco basada en todos los resultados parciales.
     */
    synchronized ArcSpeed computeFinalSpeed() {
        ArcSpeed arcSpeed = new ArcSpeed();
        arcSpeed.originStopId = originStopId;
        arcSpeed.destinationStopId = destinationStopId;
        arcSpeed.lineId = lineId;
        arcSpeed.orientation = orientation;
        arcSpeed.sampleCount = totalSamples;

        if (totalSamples > 0) {
            // Calcular promedios ponderados
            arcSpeed.averageSpeed = totalWeightedSpeed / totalSamples;
            arcSpeed.distance = totalWeightedDistance / totalSamples;
            arcSpeed.averageTime = totalWeightedTime / totalSamples;
        } else {
            arcSpeed.averageSpeed = 0;
            arcSpeed.distance = 0;
            arcSpeed.averageTime = 0;
        }

        return arcSpeed;
    }

    /**
     * Obtiene el número total de muestras agregadas.
     */
    synchronized int getSampleCount() {
        return totalSamples;
    }
}