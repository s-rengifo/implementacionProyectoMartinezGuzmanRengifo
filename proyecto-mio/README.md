# Sistema de Estimación de Velocidad Promedio SITM-MIO
## Proyecto Final - Arquitectura de Software

### Descripción

Sistema distribuido para calcular velocidades promedio por arcos en las rutas del Sistema Integrado de Transporte Masivo de Occidente (SITM-MIO) de Cali, Colombia. La solución utiliza **ZeroC ICE** para la comunicación distribuida y procesa grandes volúmenes de datos históricos de eventos GPS de los buses.

### Arquitectura

#### Componentes Principales

1. **Master Server (Coordinador)**
    - Orquesta el procesamiento distribuido
    - Divide archivos grandes en tareas (chunks)
    - Distribuye tareas a workers disponibles
    - Agrega resultados parciales

2. **Worker Nodes (Procesadores)**
    - Procesan chunks de datagramas
    - Calculan velocidades entre paradas consecutivas
    - Utilizan algoritmo Haversine para distancias
    - Filtran datos inválidos y outliers

3. **Graph Builder Service**
    - Construye grafos de rutas desde CSVs
    - Identifica arcos válidos (paradas consecutivas)
    - Mantiene información de topología

4. **Result Aggregator Service**
    - Consolida resultados parciales de workers
    - Calcula promedios ponderados
    - Persiste resultados en CSV

5. **Query Service**
    - Provee consultas sobre velocidades calculadas
    - Soporta búsquedas por arco, ruta y zona

#### Patrones de Diseño Aplicados

**Patrones Arquitectónicos:**
- **Master-Worker**: Distribución de carga de procesamiento
- **Pipes and Filters**: Procesamiento en pipeline de datagramas
- **Map-Reduce**: Mapeo paralelo de chunks y reducción de resultados

**Patrones de Diseño:**
- **Strategy**: Diferentes estrategias de cálculo de velocidad
- **Observer**: Notificación de eventos de procesamiento
- **Singleton**: Servicios únicos (GraphBuilder, ResultAggregator)
- **Factory**: Creación de tareas de procesamiento

**Patrones de Concurrencia:**
- **Thread Pool**: ExecutorService para manejo de tareas
- **Producer-Consumer**: Master produce tareas, workers consumen
- **Accumulator**: Agregación thread-safe de resultados parciales

### Estructura del Proyecto

```
proyecto-mio/
├── build.gradle
├── settings.gradle
├── src/
│   └── main/
│       ├── slice/
│       │   └── mio.ice              # Definiciones ICE
│       └── java/
│           └── com/mio/swarch/
│               ├── MasterServer.java
│               ├── WorkerNode.java
│               ├── GraphBuilderService.java
│               ├── ResultAggregatorService.java
│               ├── QueryServiceImpl.java
│               └── ClientApp.java
├── config/
│   ├── master.properties
│   ├── worker.properties
│   └── client.properties
├── scripts/
│   ├── start-master.sh
│   ├── start-worker.sh
│   ├── start-client.sh
│   ├── deploy-distributed.sh
│   ├── stop-all.sh
│   ├── check-status.sh
│   └── run-performance-test.sh
└── README.md
```

### Requisitos

- **Java**: JDK 17 o superior
- **ZeroC ICE**: 3.7.10
- **Gradle**: 7.x o superior
- **Memoria**: Mínimo 8GB RAM por worker
- **Disco**: Espacio suficiente para archivos de datos (67GB+)

### Instalación

#### 1. Instalar ZeroC ICE

```bash
# Ubuntu/Debian
sudo apt-add-repository "deb http://zeroc.com/download/ice/3.7/ubuntu20.04 stable main"
sudo apt-get update
sudo apt-get install zeroc-ice-all-runtime zeroc-ice-all-dev

# Verificar instalación
slice2java --version
```

#### 2. Clonar y Compilar Proyecto

```bash
git clone <repository-url>
cd proyecto-mio

# Compilar
./gradlew clean build

# Generar código ICE
./gradlew compileSlice
```

### Configuración

#### Archivos de Propiedades ICE

**config/master.properties**
```properties
Ice.MessageSizeMax=20480          # 20MB por mensaje
Ice.ThreadPool.Server.Size=10     # Threads base
Ice.ThreadPool.Server.SizeMax=50  # Threads máximo
Ice.Trace.Network=1               # Log de red
```

**config/worker.properties**
```properties
Ice.MessageSizeMax=20480
Ice.ThreadPool.Client.Size=5
Ice.ThreadPool.Client.SizeMax=20
```

### Despliegue

#### Opción 1: Despliegue Local (Desarrollo)

```bash
# Terminal 1: Iniciar Master
./scripts/start-master.sh

# Terminal 2-5: Iniciar Workers
./scripts/start-worker.sh worker1 10001
./scripts/start-worker.sh worker2 10002
./scripts/start-worker.sh worker3 10003
./scripts/start-worker.sh worker4 10004

# Terminal 6: Iniciar Cliente
./scripts/start-client.sh
```

#### Opción 2: Despliegue Distribuido (Producción)

```bash
# Editar scripts/deploy-distributed.sh con tus nodos
# Configurar MASTER_NODE y WORKER_NODES

# Desplegar
./scripts/deploy-distributed.sh

# Verificar estado
./scripts/check-status.sh

# Detener todos los servicios
./scripts/stop-all.sh
```

### Uso

#### Cliente Interactivo

```bash
./gradlew runClient
```

Menú de opciones:
1. **Construir grafo**: Carga archivos CSV de rutas/paradas
2. **Iniciar procesamiento**: Procesa archivo de datagramas
3. **Consultar estado**: Monitorea progreso del job
4. **Obtener resultados**: Recupera velocidades calculadas
5. **Guardar resultados**: Exporta a CSV
6. **Consultar arco específico**: Velocidad de un arco
7. **Consultar ruta completa**: Velocidades de toda una ruta
8. **Estadísticas**: Resumen general del sistema
9. **Prueba completa**: Ejecuta flujo end-to-end

#### Ejemplo de Uso Programático

```java
// Conectar a servicios
MasterCoordinatorPrx master = ...;
GraphBuilderPrx graphBuilder = ...;

// 1. Construir grafo
GraphInfo graph = graphBuilder.buildGraph(
    "lines-241.csv",
    "stops-241.csv",
    "linestops-241.csv"
);

// 2. Iniciar procesamiento
String jobId = master.startProcessing(
    "datagrams4history.csv",
    10  // número de tareas
);

// 3. Monitorear progreso
String status = master.getProcessingStatus(jobId);

// 4. Obtener resultados
ArcSpeed[] results = master.getResults(jobId);

// 5. Consultar arco específico
QueryServicePrx queryService = ...;
ArcSpeed arc = queryService.getArcSpeed(
    2241,     // lineId
    513327,   // originStopId
    514002,   // destStopId
    0         // orientation
);

System.out.printf("Velocidad: %.2f km/h\n", arc.averageSpeed);
```

### Algoritmo de Cálculo de Velocidad

#### Fórmula Haversine (Distancia)

```
a = sin²(Δφ/2) + cos φ₁ × cos φ₂ × sin²(Δλ/2)
c = 2 × atan2(√a, √(1−a))
d = R × c
```

Donde:
- φ = latitud en radianes
- λ = longitud en radianes
- R = radio de la Tierra (6371 km)

#### Cálculo de Velocidad

```
velocidad (km/h) = (distancia_km / tiempo_segundos) × 3600
```

#### Filtros Aplicados

1. **Tiempo mínimo**: 10 segundos entre puntos
2. **Velocidad máxima**: 120 km/h (filtrar outliers)
3. **Velocidad mínima**: 0 km/h (descartar negativos)
4. **Arcos válidos**: Solo arcos definidos en el grafo de rutas

### Pruebas de Performance

#### Ejecutar Benchmarks

```bash
./scripts/run-performance-test.sh
```

#### Tamaños de Prueba Sugeridos

| Tamaño | Registros | Archivo (~)| Workers | Tiempo Esperado |
|--------|-----------|------------|---------|-----------------|
| Pequeño| 1M        | 670MB      | 4       | 2-3 min         |
| Mediano| 10M       | 6.7GB      | 8       | 15-20 min       |
| Grande | 100M      | 67GB       | 16      | 2-3 horas       |

#### Métricas a Medir

- **Throughput**: Registros procesados/segundo
- **Latencia**: Tiempo total de procesamiento
- **Escalabilidad**: Speedup al agregar workers
- **Utilización**: CPU y memoria por nodo

### Drivers de Arquitectura Satisfechos

#### Performance

1. **Escalabilidad Horizontal**
    - Agregar workers linealmente aumenta throughput
    - Patrón Master-Worker permite N workers
    - Procesamiento paralelo de chunks

2. **Baja Latencia**
    - Procesamiento en memoria (sin I/O intermedio)
    - Thread pools para concurrencia
    - ICE optimizado para comunicación eficiente

3. **Alto Throughput**
    - Procesamiento batch de datagramas
    - Agregación lazy de resultados
    - Minimización de comunicación Master-Worker

#### Disponibilidad

- Tolerancia a fallos de workers individuales
- Master continúa con workers disponibles
- Reintentos automáticos de tareas fallidas

#### Mantenibilidad

- Separación clara de responsabilidades
- Interfaces ICE bien definidas
- Código modular y extensible

### Extensiones Futuras

#### Streaming en Tiempo Real (Bonus)

```java
interface StreamProcessor {
    void processDatagram(Datagram datagram);
    void startStreaming();
    void stopStreaming();
}
```

Implementación sugerida:
- Kafka/RabbitMQ para ingesta de eventos
- Workers suscritos a topics
- Ventanas deslizantes para promedios móviles
- Actualización incremental de resultados

#### Optimizaciones

1. **Cache distribuido** (Redis/Hazelcast)
2. **Compresión de mensajes** ICE
3. **Particionamiento espacial** de datagramas
4. **Índices secundarios** para consultas rápidas

### Troubleshooting

#### Error: "Ice.ConnectionRefusedException"

```bash
# Verificar que Master está corriendo
netstat -an | grep 10000

# Verificar firewall
sudo ufw status
sudo ufw allow 10000/tcp
```

#### Error: "OutOfMemoryError"

```bash
# Aumentar heap size
export GRADLE_OPTS="-Xmx8g"
./gradlew runWorker -Pargs="worker1 10001"
```

#### Workers no se registran

```bash
# Verificar conectividad
ping <master-host>
telnet <master-host> 10000

# Revisar logs
tail -f master.log
tail -f worker-*.log
```

### Estructura de Datos de Salida

#### results.csv

```csv
lineId,originStopId,destinationStopId,orientation,averageSpeed,distance,averageTime,sampleCount
2241,513327,514002,0,25.50,1.234,174.12,1523
2241,514002,500103,0,32.10,2.456,275.45,1876
...
```

Columnas:
- `lineId`: Identificador de ruta
- `originStopId`: Parada origen
- `destinationStopId`: Parada destino
- `orientation`: Dirección (0=ida, 1=vuelta)
- `averageSpeed`: Velocidad promedio (km/h)
- `distance`: Distancia promedio (km)
- `averageTime`: Tiempo promedio (segundos)
- `sampleCount`: Número de mediciones

### Autores

- Equipo de Arquitectura de Software
- Universidad del Valle
- 2024

### Referencias

- [ZeroC ICE Documentation](https://doc.zeroc.com/ice/3.7)
- [SITM-MIO Official Website](http://www.metrocali.gov.co/)
- Haversine Formula: https://en.wikipedia.org/wiki/Haversine_formula

### Licencia

MIT License - Ver archivo LICENSE para detalles