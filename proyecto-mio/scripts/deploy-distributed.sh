# scripts/deploy-distributed.sh
#!/bin/bash
# Script para desplegar la solución en múltiples nodos

echo "========================================="
echo "Despliegue Distribuido SITM-MIO"
echo "========================================="

# Configuración
MASTER_NODE="swarch@104m01"
WORKER_NODES=("swarch@104m02" "swarch@104m03" "swarch@104m04")
PROJECT_DIR="/home/swarch/proyecto-mio"

# 1. Compilar proyecto
echo "1. Compilando proyecto..."
./gradlew clean build
if [ $? -ne 0 ]; then
    echo "Error en compilación"
    exit 1
fi

# 2. Desplegar en Master
echo "2. Desplegando en Master Node..."
ssh $MASTER_NODE "mkdir -p $PROJECT_DIR"
scp -r build/libs/* $MASTER_NODE:$PROJECT_DIR/
scp -r config/* $MASTER_NODE:$PROJECT_DIR/config/

# 3. Iniciar Master
echo "3. Iniciando Master Server..."
ssh $MASTER_NODE "cd $PROJECT_DIR && nohup java -jar mio-swarch-1.0-SNAPSHOT.jar MasterServer > master.log 2>&1 &"
sleep 5

# 4. Desplegar en Workers
WORKER_PORT=10001
for i in "${!WORKER_NODES[@]}"; do
    WORKER_ID="worker$((i+1))"
    NODE=${WORKER_NODES[$i]}
    PORT=$((WORKER_PORT + i))

    echo "4.$((i+1)). Desplegando Worker en $NODE..."
    ssh $NODE "mkdir -p $PROJECT_DIR"
    scp -r build/libs/* $NODE:$PROJECT_DIR/
    scp -r config/* $NODE:$PROJECT_DIR/config/

    echo "Iniciando Worker $WORKER_ID en puerto $PORT..."
    ssh $NODE "cd $PROJECT_DIR && nohup java -jar mio-swarch-1.0-SNAPSHOT.jar WorkerNode $WORKER_ID $PORT > worker-$WORKER_ID.log 2>&1 &"
    sleep 2
done

echo ""
echo "========================================="
echo "Despliegue completado"
echo "Master: $MASTER_NODE:10000"
echo "Workers: ${#WORKER_NODES[@]} nodos"
echo "========================================="
