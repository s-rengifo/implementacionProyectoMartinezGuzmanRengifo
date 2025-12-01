package com.mio.swarch;

import MIO.*;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import com.zeroc.Ice.Current;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class GraphBuilderService implements GraphBuilder {
    private static final Logger logger = LoggerFactory.getLogger(GraphBuilderService.class);

    private GraphInfo cachedGraphInfo;

    @Override
    public GraphInfo buildGraph(String linesFile, String stopsFile, String lineStopsFile, Current current)
            throws InvalidDataException {
        logger.info("Construyendo grafo desde archivos CSV...");

        try {
            long startTime = System.currentTimeMillis();

            // Leer paradas
            List<Stop> stops = loadStops(stopsFile);
            logger.info("Cargadas {} paradas", stops.size());

            // Leer line-stops (relación paradas-rutas)
            List<LineStop> lineStops = loadLineStops(lineStopsFile);
            logger.info("Cargadas {} relaciones parada-ruta", lineStops.size());

            // Leer líneas (para validación)
            Set<Integer> validLines = loadLines(linesFile);
            logger.info("Cargadas {} líneas/rutas", validLines.size());

            // Calcular arcos
            int totalArcs = calculateArcs(lineStops);

            // Crear GraphInfo
            GraphInfo graphInfo = new GraphInfo();
            graphInfo.totalRoutes = validLines.size();
            graphInfo.totalStops = stops.size();
            graphInfo.totalArcs = totalArcs;
            graphInfo.stops = stops.toArray(new Stop[0]);
            graphInfo.lineStops = lineStops.toArray(new LineStop[0]);

            cachedGraphInfo = graphInfo;

            long elapsedTime = System.currentTimeMillis() - startTime;
            logger.info("Grafo construido exitosamente en {}ms: {} rutas, {} paradas, {} arcos",
                    elapsedTime, graphInfo.totalRoutes, graphInfo.totalStops, graphInfo.totalArcs);

            return graphInfo;

        } catch (Exception e) {
            logger.error("Error construyendo grafo", e);
            throw new InvalidDataException("Error: " + e.getMessage());
        }
    }

    @Override
    public GraphInfo getGraphInfo(Current current) {
        return cachedGraphInfo;
    }

    private List<Stop> loadStops(String filePath) throws IOException, CsvException {
        List<Stop> stops = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> rows = reader.readAll();

            // Saltar header
            for (int i = 1; i < rows.size(); i++) {
                String[] row = rows.get(i);

                try {
                    Stop stop = new Stop();
                    stop.stopId = Long.parseLong(cleanValue(row[0]));
                    stop.shortName = cleanValue(row[2]);
                    stop.longName = cleanValue(row[3]);
                    stop.longitude = Double.parseDouble(cleanValue(row[6]));
                    stop.latitude = Double.parseDouble(cleanValue(row[7]));

                    stops.add(stop);

                } catch (Exception e) {
                    logger.warn("Error parseando parada en fila {}: {}", i, e.getMessage());
                }
            }
        }

        return stops;
    }

    private List<LineStop> loadLineStops(String filePath) throws IOException, CsvException {
        List<LineStop> lineStops = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> rows = reader.readAll();

            // Saltar header
            for (int i = 1; i < rows.size(); i++) {
                String[] row = rows.get(i);

                try {
                    LineStop lineStop = new LineStop();
                    lineStop.lineStopId = Long.parseLong(cleanValue(row[0]));
                    lineStop.stopSequence = Integer.parseInt(cleanValue(row[1]));
                    lineStop.orientation = Integer.parseInt(cleanValue(row[2]));
                    lineStop.lineId = Integer.parseInt(cleanValue(row[3]));
                    lineStop.stopId = Long.parseLong(cleanValue(row[4]));

                    lineStops.add(lineStop);

                } catch (Exception e) {
                    logger.warn("Error parseando line-stop en fila {}: {}", i, e.getMessage());
                }
            }
        }

        return lineStops;
    }

    private Set<Integer> loadLines(String filePath) throws IOException, CsvException {
        Set<Integer> lines = new HashSet<>();

        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            List<String[]> rows = reader.readAll();

            // Saltar header
            for (int i = 1; i < rows.size(); i++) {
                String[] row = rows.get(i);

                try {
                    int lineId = Integer.parseInt(cleanValue(row[0]));
                    lines.add(lineId);
                } catch (Exception e) {
                    logger.warn("Error parseando línea en fila {}: {}", i, e.getMessage());
                }
            }
        }

        return lines;
    }

    private int calculateArcs(List<LineStop> lineStops) {
        // Agrupar por lineId y orientation
        Map<String, List<LineStop>> routeMap = new HashMap<>();

        for (LineStop ls : lineStops) {
            String key = ls.lineId + "-" + ls.orientation;
            routeMap.computeIfAbsent(key, k -> new ArrayList<>()).add(ls);
        }

        int totalArcs = 0;

        // Para cada ruta, contar arcos (n-1 arcos para n paradas)
        for (List<LineStop> stops : routeMap.values()) {
            if (stops.size() > 1) {
                totalArcs += stops.size() - 1;
            }
        }

        return totalArcs;
    }

    private String cleanValue(String value) {
        if (value == null) return "";
        return value.replace("\"", "").trim();
    }
}