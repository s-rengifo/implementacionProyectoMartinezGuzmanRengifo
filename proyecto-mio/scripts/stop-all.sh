# scripts/stop-all.sh
#!/bin/bash

echo "Deteniendo todos los servicios..."

MASTER_NODE="swarch@104m01"
WORKER_NODES=("swarch@104m02" "swarch@104m03" "swarch@104m04")

# Detener Master
echo "Deteniendo Master..."
ssh $MASTER_NODE "pkill -f 'java.*MasterServer'"

# Detener Workers
for NODE in "${WORKER_NODES[@]}"; do
    echo "Deteniendo Worker en $NODE..."
    ssh $NODE "pkill -f 'java.*WorkerNode'"
done

echo "Todos los servicios detenidos"
