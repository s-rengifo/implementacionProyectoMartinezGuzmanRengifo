# scripts/start-worker.sh
#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Uso: $0 <worker-id> <puerto>"
    echo "Ejemplo: $0 worker1 10001"
    exit 1
fi

WORKER_ID=$1
PORT=$2

echo "Iniciando Worker $WORKER_ID en puerto $PORT..."
./gradlew runWorker -Pargs="$WORKER_ID $PORT"
