# Sistema MIO - Procesamiento Distribuido con ZeroC Ice

### INTEGRANTES: Martinez, Guzman y Rengifo

## 📋 Tabla de Contenidos
- [🎯 Objetivo](#-objetivo)
- [🏗️ Arquitectura](#️-arquitectura)
- [🔧 Requisitos Previos](#-requisitos-previos)
- [🖥️ Configuración de Máquinas](#️-configuración-de-máquinas)
- [📊 Flujo de Trabajo](#-flujo-de-trabajo)
- [⚙️ Comandos por Escenario](#️-comandos-por-escenario)
- [📝 Estructura del Proyecto](#-estructura-del-proyecto)

---

## Objetivo

Sistema distribuido para calcular velocidades promedio de arcos en las rutas del SITM-MIO, procesando millones de datagramas históricos y en tiempo real utilizando ZeroC Ice.

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                      ARQUITECTURA DISTRIBUIDA                   │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
│  │   WORKER 1  │  │   WORKER 2  │  │   WORKER 3  │            │
│  │  (104m03)   │  │  (104m04)   │  │  (104m06)   │            │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘            │
│         │                 │                 │                   │
│  ┌──────┴─────────────────┴─────────────────┴──────┐           │
│  │               MASTER SERVER (104m01)             │           │
│  │  ┌──────────────────────────────────────────┐  │           │
│  │  │   • MasterCoordinator (Puerto: 10000)    │  │           │
│  │  │   • StreamCoordinator                    │  │           │
│  │  │   • GraphBuilder                         │  │           │
│  │  │   • ResultAggregator                     │  │           │
│  │  │   • QueryService                         │  │           │
│  │  └──────────────────────────────────────────┘  │           │
│  └───────────────────────┬────────────────────────┘           │
│                          │                                     │
│  ┌───────────────────────┴────────────────────────┐           │
│  │            STREAMING SERVER (104m01)           │           │
│  │       • StreamBroker (Puerto: 11000)           │           │
│  └───────────────────────┬────────────────────────┘           │
│                          │                                     │
│  ┌───────────────────────┴────────────────────────┐           │
│  │               CLIENTE (104m05)                  │           │
│  │        • Interfaz de usuario y pruebas          │           │
│  └─────────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔧 Requisitos Previos

### Software Requerido
- **Java 11** o superior
- **Gradle 7+**
- **ZeroC Ice 3.7+**
- Acceso SSH a las máquinas 104m01-104m24

### Estructura de Directorios
```
proyecto-mio/
├── src/
│   ├── main/
│   │   ├── java/com/mio/swarch/
│   │   └── slice/mio.ice
│   └── test/
├── data/                     # Datos de prueba
│   ├── datagrams_1M.csv
│   ├── lines-241.csv
│   ├── stops-241.csv
│   └── linestops-241.csv
├── config/                   # Configuraciones
├── results/                  # Resultados generados
└── logs/                     # Logs del sistema
```

---

## 🖥️ Configuración de Máquinas

### MASTER SERVER (104m01)
```bash
# Terminal 1 - Master Server
export MASTER_HOST=192.168.131.101
cd proyecto-mio
./gradlew build
./gradlew runMaster

# Terminal 2 - Streaming Server (Opcional para streaming)
export MASTER_HOST=192.168.131.101
cd proyecto-mio
./gradlew runStreamingServer
```

**Salida esperada:**
```
Master Server iniciado en tcp://192.168.131.101:10000
Servicios disponibles:
  - MasterCoordinator
  - StreamCoordinator 🆕
  - GraphBuilder
  - ResultAggregator
  - QueryService
```

### WORKERS (104m03, 104m04, 104m06, 104m07)

#### Worker 1 (104m03)
```bash
export MASTER_HOST=192.168.131.101
export WORKER_HOST=192.168.131.103
cd proyecto-mio
./gradlew build
./gradlew runWorker -Pargs="worker1 10001"
```

#### Worker 2 (104m04)
```bash
export MASTER_HOST=192.168.131.101
export WORKER_HOST=192.168.131.104
cd proyecto-mio
./gradlew build
./gradlew runWorker -Pargs="worker2 10002"
```

#### Worker 3 (104m06)
```bash
export MASTER_HOST=192.168.131.101
export WORKER_HOST=192.168.131.106
cd proyecto-mio
./gradlew build
./gradlew runWorker -Pargs="worker3 10003"
```

#### Worker 4 (104m07)
```bash
export MASTER_HOST=192.168.131.101
export WORKER_HOST=192.168.131.107
cd proyecto-mio
./gradlew build
./gradlew runWorker -Pargs="worker4 10004"
```

**Salida esperada por Worker:**
```
Worker worker1 iniciado en puerto 10001
✓ Worker worker1 registrado exitosamente con el Master
```

### CLIENTE (104m05)
```bash
export MASTER_HOST=192.168.131.101
cd proyecto-mio
./gradlew build
./gradlew runClient --console=plain
```

**Menú del Cliente:**
```
════════════════════════════════════════════════════════
       SISTEMA MIO - Cliente de Prueba
       Conectado a Master: 192.168.131.101:10000
════════════════════════════════════════════════════════
1. Construir grafo de rutas
2. Iniciar procesamiento de datagramas
3. Consultar estado de procesamiento
4. Obtener resultados
5. Guardar resultados en archivo
6. Consultar velocidad de un arco
7. Consultar velocidades de una ruta
8. Ver estadísticas generales
9. MODO STREAMING (Tiempo Real) 🆕
0. Salir
h. Mostrar menú
════════════════════════════════════════════════════════
```

---

## 📊 Flujo de Trabajo

### Escenario 1: Procesamiento Batch (Histórico)
```
1. Cliente → Opción 1: Construir grafo
   • lines-241.csv
   • stops-241.csv
   • linestops-241.csv

2. Cliente → Opción 2: Procesar archivo
   • Ruta: data/datagrams_1M.csv
   • Tareas: 4

3. Cliente → Opción 3: Monitorear estado
4. Cliente → Opción 4: Obtener resultados
5. Cliente → Opción 5: Guardar en CSV
```

### Escenario 2: Procesamiento Streaming (Tiempo Real)
```
1. Asegurar que StreamingServer esté corriendo
2. Cliente → Opción 9: Modo Streaming
3. Configurar:
   • Archivo: data/datagrams_1M.csv
   • Workers: 3
   • Delay: 10ms
   • Batch: 100
4. Ver resultados actualizándose en tiempo real
5. Presionar ENTER para detener
```

---

## ⚙️ Comandos por Escenario

### Escenario Pequeño (1M registros, 1 Worker)
```bash
# Worker
export MASTER_HOST=192.168.131.101
export WORKER_HOST=192.168.131.103
./gradlew runWorker -Pargs="worker1 10001"

# Cliente - Configuración
Archivo: data/datagrams_1M.csv
Workers: 1
Delay: 30ms
Batch: 50
```

### Escenario Mediano (10M registros, 3 Workers)
```bash
# Workers
./gradlew runWorker -Pargs="worker1 10001"
./gradlew runWorker -Pargs="worker2 10002"
./gradlew runWorker -Pargs="worker3 10003"

# Cliente - Configuración
Archivo: data/datagrams_10M.csv
Workers: 3
Delay: 10ms
Batch: 100
```

### Escenario Grande (100M registros, 6 Workers)
```bash
# Workers (máquinas adicionales)
./gradlew runWorker -Pargs="worker4 10004"
./gradlew runWorker -Pargs="worker5 10005"
./gradlew runWorker -Pargs="worker6 10006"

# Cliente - Configuración
Archivo: data/datagrams_100M.csv
Workers: 6
Delay: 0ms
Batch: 200
```

---

## Opciones de Rendimiento

### Optimizaciones

1. **Chunk Size**: Ajustar tamaño de chunks (default: 200K líneas)
2. **Batch Size**: Ajustar batch para streaming (default: 100)
3. **Delay**: Controlar velocidad de streaming (0ms = máxima)
4. **Workers**: Escalar horizontalmente (más máquinas)

## 📝 Estructura del Proyecto

### Archivos Clave
```
src/main/slice/mio.ice           # Definiciones de interfaces ICE
src/main/java/com/mio/swarch/
├── MasterServer.java            # Servidor principal
├── MasterCoordinatorImpl.java   # Coordinador batch
├── MasterStreamingCoordinator.java # Coordinador streaming
├── WorkerNode.java              # Nodo worker
├── WorkerImpl.java              # Implementación worker
├── ClientApp.java               # Cliente interactivo
├── StreamBrokerService.java     # Broker pub/sub
├── StreamingServer.java         # Servidor streaming
├── StreamSubscriberService.java # Subscriber streaming
├── BusSimulatorService.java     # Simulador de buses
├── GraphBuilderService.java     # Constructor de grafos
├── ResultAggregatorService.java # Agregador de resultados
└── QueryServiceImpl.java        # Servicio de consultas
```

### Comandos Gradle Disponibles
```bash
./gradlew runMaster             # Iniciar Master Server
./gradlew runWorker             # Iniciar Worker Node
./gradlew runClient             # Iniciar Cliente
./gradlew runStreamingServer    # Iniciar Streaming Server
./gradlew clean build           # Limpiar y compilar
```

### Variables de Entorno Críticas
```bash
# Obligatorias
export MASTER_HOST=192.168.131.101    # IP del Master
export WORKER_HOST=<ip-worker>        # IP de cada Worker
```



---

## 🚨 Notas Importantes

1. **Orden de inicio**: Siempre iniciar Master primero, luego Workers, luego, StreamingServer, luego Cliente
2. **Variables de entorno**: Configurar en cada terminal nueva
3. **Puertos**: 10000 (Master), 11000 (Streaming), 10001+ (Workers)
4. **Memoria**: Ajustar según tamaño de datos
5. **Red**: Todas las máquinas deben poder comunicarse entre sí
6. **Firewall**: Permitir puertos 10000-10100 y 11000

---
