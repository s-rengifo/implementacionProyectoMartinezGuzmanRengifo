# scripts/run-performance-test.sh
#!/bin/bash
# Script para ejecutar pruebas de performance con diferentes tamaños de datos

echo "========================================="
echo "Pruebas de Performance SITM-MIO"
echo "========================================="

# Tamaños de prueba (escala logarítmica)
SIZES=(1000000 10000000 100000000)
DATA_FILE="/home/swarch/proyecto-mio/MIO/datagrams4history.csv"
WORKERS=(2 4 8 16)

for SIZE in "${SIZES[@]}"; do
    for NUM_WORKERS in "${WORKERS[@]}"; do
        echo ""
        echo "========================================="
        echo "Prueba: $SIZE registros, $NUM_WORKERS workers"
        echo "========================================="

        # Crear subset de datos si es necesario
        SUBSET_FILE="datagrams_${SIZE}.csv"
        if [ ! -f "$SUBSET_FILE" ]; then
            echo "Creando subset de $SIZE registros..."
            head -n 1 $DATA_FILE > $SUBSET_FILE
            tail -n +2 $DATA_FILE | head -n $SIZE >> $SUBSET_FILE
        fi

        # Ejecutar prueba
        START_TIME=$(date +%s)

        # Aquí iría la llamada al cliente para ejecutar el procesamiento
        # Por ahora, simulamos el tiempo
        sleep 5

        END_TIME=$(date +%s)
        ELAPSED=$((END_TIME - START_TIME))

        echo "Tiempo transcurrido: $ELAPSED segundos"
        echo "Throughput: $((SIZE / ELAPSED)) registros/segundo"

        # Guardar métricas
        echo "$SIZE,$NUM_WORKERS,$ELAPSED" >> performance_results.csv
    done
done

echo ""
echo "Pruebas completadas. Resultados en performance_results.csv"