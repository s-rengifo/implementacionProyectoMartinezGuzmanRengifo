# scripts/check-status.sh
#!/bin/bash

echo "========================================="
echo "Estado de los Servicios"
echo "========================================="

MASTER_NODE="swarch@104m01"
WORKER_NODES=("swarch@104m02" "swarch@104m03" "swarch@104m04")

# Master
echo "Master ($MASTER_NODE):"
ssh $MASTER_NODE "ps aux | grep 'java.*MasterServer' | grep -v grep"

# Workers
for NODE in "${WORKER_NODES[@]}"; do
    echo ""
    echo "Worker ($NODE):"
    ssh $NODE "ps aux | grep 'java.*WorkerNode' | grep -v grep"
done
