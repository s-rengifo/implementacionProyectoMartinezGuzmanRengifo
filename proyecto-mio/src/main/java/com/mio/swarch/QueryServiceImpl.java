package com.mio.swarch;

import MIO.*;
import com.zeroc.Ice.Current;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class QueryServiceImpl implements QueryService {
    private static final Logger logger = LoggerFactory.getLogger(QueryServiceImpl.class);

    private final ResultAggregatorService resultAggregator;
    private String currentJobId; // ID del último job completado

    public QueryServiceImpl(ResultAggregatorService resultAggregator) {
        this.resultAggregator = resultAggregator;
    }

    public void setCurrentJobId(String jobId) {
        this.currentJobId = jobId;
        logger.info("JobId actual establecido: {}", jobId);
    }

    @Override
    public ArcSpeed getArcSpeed(int lineId, long originStop, long destStop, int orientation, Current current)
            throws DataNotFoundException {

        if (currentJobId == null) {
            throw new DataNotFoundException("No hay resultados disponibles. Ejecute un procesamiento primero.");
        }

        logger.debug("Consultando velocidad de arco: lineId={}, origin={}, dest={}, orientation={}",
                lineId, originStop, destStop, orientation);

        return resultAggregator.getArcSpeed(currentJobId, lineId, originStop, destStop, orientation);
    }

    @Override
    public ArcSpeed[] getRouteSpeed(int lineId, int orientation, Current current) {
        if (currentJobId == null) {
            return new ArcSpeed[0];
        }

        logger.debug("Consultando velocidades de ruta: lineId={}, orientation={}", lineId, orientation);

        List<ArcSpeed> speeds = resultAggregator.getRouteSpeed(currentJobId, lineId, orientation);
        return speeds.toArray(new ArcSpeed[0]);
    }

    @Override
    public ArcSpeed[] getZoneSpeeds(long[] stopIds, Current current) {
        if (currentJobId == null) {
            return new ArcSpeed[0];
        }

        logger.debug("Consultando velocidades de zona con {} paradas", stopIds.length);

        try {
            ArcSpeed[] allSpeeds = resultAggregator.getFinalResults(currentJobId, current);

            // Filtrar arcos que incluyan las paradas de la zona
            return java.util.Arrays.stream(allSpeeds)
                    .filter(arc -> containsStop(stopIds, arc.originStopId) ||
                            containsStop(stopIds, arc.destinationStopId))
                    .toArray(ArcSpeed[]::new);

        } catch (DataNotFoundException e) {
            logger.error("Error obteniendo velocidades de zona", e);
            return new ArcSpeed[0];
        }
    }

    @Override
    public String getSystemStatistics(Current current) {
        if (currentJobId == null) {
            return "No hay estadísticas disponibles. Ejecute un procesamiento primero.";
        }

        return resultAggregator.getStatistics(currentJobId);
    }

    private boolean containsStop(long[] stopIds, long stopId) {
        for (long id : stopIds) {
            if (id == stopId) return true;
        }
        return false;
    }
}