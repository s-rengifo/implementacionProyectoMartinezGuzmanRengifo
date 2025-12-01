// src/main/slice/mio.ice
// Espacio de trabajo que agrupa toda las relaciones del MIO
module MIO {

    // ========== Definición de Tipos de Secuencias ==========

    // 1. Para utilizar propiamente un tipo de dato de java no definido en el .ice
    // es decir una (list) utilizamos la equivalencia que permite el .ice que es
    // sequence

    sequence<long> LongList;
    sequence<string> StringList;
    sequence<double> DoubleList;

    // 2. Estas lists se van a utilizar en la misma (structs) definidas en el .ice
    // 3. Esta definicion es un TIPO, no una instancia
    // pero permitira a cada componente crear su propia LongList, StringList...

    // Master crea SU StopList
    //StopList masterStops = new StopList();
    // Worker crea SU StopList
    //StopList workerStops = new StopList();

    // 4.Cada componente tiene su PROPIA copia
    // que puede PASAR a otros componentes via ICE

    // ========== Estructuras de Datos ==========
    // Se definen estas estructuras directamente desde el .ice
    // ya que se necesita distribuir este objeto y se debe manejar
    // en diferentes nodos para operar

    // Datos crudos del bus
    struct Datagram {
        int eventType;          // Tipo de evento (0=posición GPS, etc.)
        string registerDate;    // Fecha de registro "31-MAY-18"
        long stopId;           // ID de la parada (-1 si no aplica)
        int odometer;          // Lectura del odómetro
        double latitude;       // Latitud (formato dividido por 1,000,000)
        double longitude;      // Longitud (formato dividido por 1,000,000)
        int taskId;            // ID de la tarea
        int lineId;            // ID de la ruta
        int tripId;            // ID del viaje
        long unknown1;         // Campo no identificado
        string datagramDate;   // Timestamp "2018-05-31 00:00:00"
        int busId;             // ID único del bus
    };

    sequence<Datagram> DatagramList;
    sequence<Datagram> DatagramSeq;


    // Info de paradas
    struct Stop {
        long stopId;           // ID único de la parada
        string shortName;      // Nombre corto "K109C421"
        string longName;       // Nombre completo "Kr 109 con Cl 42"
        double longitude;      // Longitud decimal (-76.51839806)
        double latitude;       // Latitud decimal (3.358595)
    };

    sequence<Stop> StopList;

    // Relacion parada-ruta
    struct LineStop {
        long lineStopId;       // ID único de la relación
        int stopSequence;      // Orden en la ruta (34, 35, 36...)
        int orientation;       // Dirección (0=ida, 1=vuelta)
        int lineId;            // ID de la ruta
        long stopId;           // ID de la parada
    };

    sequence<LineStop> LineStopList;

    // Info de Arco
    struct Arc {
        long originStopId;      // ID de la parada origen
        long destinationStopId; // ID de la parada origen
        int lineId;             // ID de la ruta
        int orientation;        // Dirección (0=ida, 1=vuelta)
    };

    // Resultado del calculo
    struct ArcSpeed {
        long originStopId;     // Parada origen
        long destinationStopId;// Parada destino
        int lineId;            // Ruta
        int orientation;       // Dirección
        double averageSpeed;   // Velocidad promedio km/h
        int sampleCount;       // Número de muestras
        double distance;       // Distancia en km
        double averageTime;    // Tiempo promedio en segundos
    };

    sequence<ArcSpeed> ArcSpeedList;
    dictionary<string, ArcSpeed> ArcSpeedMap; // key: "lineId-origin-dest-orientation"

    /*
    Se utiliza (ProcessingTask) para hacer un approach ineficiente al momento
    de leer lo datos, ya que no es eficiente que cada worker lea un tamano de GB,
    supongase que son 100GB de datos, que cada Worker tenga que leer los 100GB
    no es eficiente de ahi que con (ProcessingTask) dependiendo del numero de tareas
    se le va a asignar a cada Worker hacer la lectura de cada trocito de archivo

    ARCHIVO: datagrams4history.csv (67 GB)
    ╔═══════════════════════════════════════════════════════════════════╗
    ║ [0 bytes]                 [16.75 GB]           [33.5 GB]          ║
    ║    ↓                          ↓                    ↓              ║
    ║ Task1: 0-16.75GB          Task2: 16.75-33.5GB  Task3: 33.5-50.25GB║
    ║ Worker 1                  Worker 2             Worker 3           ║
    ╚═══════════════════════════════════════════════════════════════════╝
     */
    struct ProcessingTask {
        int taskId;            // ID unico para darle seguimiento a la tarea
        long startOffset;      // Byte donde EMPIEZA este chunk
        long endOffset;        // Byte donde TERMINA este chunk
        string filePath;       // Ruta del archivo compartido
        StringList datagramLines;  //Líneas de datos enviadas directamente desde Master
    };
    /*
    No se utiliza division por lineas ya que dividir 100M líneas exactamente entre N workers
    seria dificil, ineficiente al contar las lineas antes de procesar.

    -I/O Optimizado lo mejor posible y balance de Carga:
    Mientras que con File.length() da tamaño inmediatamente, donde podemos hacer la division
    para que a cada Worker sepa a que chunks le deba hacer lectura (fileSize / numTasks = chunkSize),
    es eficiente ya que cada Worker lee solo su chunk (el asignado, ya que dependiendo del numero de
    worker le podrian tocar mas de una tarea), por ultimo es balanceado ya que tiene chunks de tamano
    similar

    -Tolerancia a Fallos:
    Si un worker falla, solo se re-procesa SU chunk, de ahi que no hay
    que re-procesar archivo completo

    -Escalabilidad:
    Se pueden agregar más workers, ya que se crean más chunks más pequeños
    */

    sequence<ProcessingTask> TaskList;

    // Resultado parcial que cada Worker devuelve
    // al Master después de procesar su chunk
    struct ProcessingResult {
        int taskId;                  // Identifica que chunk procesó este worker
        ArcSpeedMap partialResults;  // Map de resultados del worker
        int processedRecords;        // Numero de registros procesados
        double processingTime;       // Tiempo que tomo procesar (segundos)
        bool success;                // Si la tarea fue exitosa
        string errorMessage;         // Mensaje de error si hubo fallo
    };

    /*
    - Tolerancia a Fallos Parciales:
    Si un worker falla, solo se pierde SU chunk, Los otros
    workers siguen contribuyendo resultados

    - Balanceo de Carga:
    Si un worker es lento debido a que devuelve processingTime ya sea
    por recursos, cargas... Se puede determinar asignar chunks más
    pequeños

    - Debugging:
    Con taskId se puede identificar exactamente que
    chunk causó problemas
     */

    // En pocas palabras, es la topología de rutas que permite a los workers determinar
    // que arcos existen en el sistema real, que paradas son consecutivas
    // en cada ruta, que orientaciones (ida/vuelta) tiene cada ruta
    struct GraphInfo {
        int totalRoutes;       // Número total de rutas (105)
        int totalStops;        // Número total de paradas (2,119)
        int totalArcs;         // Número total de arcos (7.368) paradas por ruta
        StopList stops;        // Lista de todas las paradas
        LineStopList lineStops;// Lista de relaciones parada-ruta
    };

    // ========== Excepciones ==========
    // Con el objetivo de capturar expeciones especificas
    // antes fallos en el proceso para poder debuggear
    // ENVIAR FALLOS MEDIANTE LA RED

    // Errores de Procesamiento
    exception ProcessingException {
        string reason; // Descripción tecnica del error
    };

    // Datos Inexistentes
    exception DataNotFoundException {
        string message; // Mensaje descriptivo para el user
    };

    // Datos Invalidos
    exception InvalidDataException {
        string message; // Explicacion de la invalidez
    };

    // ========== Interfaces de Servicios ==========

    // Interface para Worker Nodes
    interface Worker {
    // Operaciones que los Workers deben implementar

        idempotent ProcessingResult processTask(ProcessingTask task, GraphInfo graphInfo)
            throws ProcessingException;
        // Es idempotente si ejecutarla múltiples veces produce
        // el mismo resultado que ejecutarla una vez
        // ejemplo: consultas, verificaciones...
        // mismos inputs, mismos outputs
        // ICE puede cachear respuestas de métodos idempotentes

        idempotent bool isAvailable();

        idempotent string getWorkerId();

        void shutdown();
    };

    // Interface para Callbacks desde Worker a Master
    interface MasterCallback {
        void taskCompleted(ProcessingResult result);

        void taskFailed(int taskId, string reason);

        void workerHeartbeat(string workerId);
    };

    // Interface del Master/Coordinator
    interface MasterCoordinator {
        // Iniciar procesamiento
        string startProcessing(string dataFilePath, int numTasks)
            throws ProcessingException;

        // Registrar un worker
        void registerWorker(Worker* workerProxy, string workerId);

        // Obtener estado del procesamiento
        idempotent string getProcessingStatus(string jobId);

        // Obtener resultados
        idempotent ArcSpeedList getResults(string jobId)
            throws DataNotFoundException;

        // Cancelar procesamiento
        void cancelProcessing(string jobId);
    };

    // Interface para construcción del grafo
    interface GraphBuilder {
        GraphInfo buildGraph(string linesFile, string stopsFile, string lineStopsFile)
            throws InvalidDataException;

        idempotent GraphInfo getGraphInfo();
    };

    // Interface para agregación de resultados
    interface ResultAggregator {
        void addPartialResult(string jobId, ProcessingResult result);

        idempotent ArcSpeedList getFinalResults(string jobId)
            throws DataNotFoundException;

        void persistResults(string jobId, string outputPath)
            throws ProcessingException;
    };

    // Interface para consultas (para usuarios/controladores)
    interface QueryService {
        // Consultar velocidad de un arco específico
        idempotent ArcSpeed getArcSpeed(int lineId, long originStop, long destStop, int orientation)
            throws DataNotFoundException;

        // Consultar velocidades de una ruta completa
        idempotent ArcSpeedList getRouteSpeed(int lineId, int orientation);

        // Consultar velocidades en una zona (por lista de paradas)
        idempotent ArcSpeedList getZoneSpeeds(LongList stopIds);

        // Obtener estadísticas generales
        idempotent string getSystemStatistics();
    };

    // ========== INTERFACES PARA STREAMING ==========

    /**
     * Subscriber que recibe datagramas del broker en tiempo real.
     */
    interface StreamSubscriber {
        // Callback para recibir datagramas
        void onDatagramReceived(Datagram datagram);
    };

    /**
     * Broker de mensajes para pub/sub de datagramas.
     */
    interface StreamBroker {
        // Publicar un datagrama
        void publish(Datagram datagram);

        // Publicar múltiples datagramas en batch
        void publishBatch(DatagramSeq datagrams);

        // Suscribirse para recibir datagramas
        void subscribe(StreamSubscriber* subscriber);

        // Desuscribirse
        void unsubscribe(StreamSubscriber* subscriber);

        // Obtener estadísticas del broker
        idempotent string getStats();
    };

    /**
     * Simulador de buses que publica datagramas al broker.
     */
    interface BusSimulator {
        // Iniciar simulación desde archivo
        void startStreaming(string dataFilePath, int delayMs, int batchSize)
            throws ProcessingException;

        // Detener simulación
        void stopStreaming();

        // Verificar si está corriendo
        idempotent bool isStreaming();

        // Obtener estadísticas
        idempotent string getStats();
    };

    /**
     * Coordinador de streaming (Master extendido con capacidades de streaming).
     */
    interface StreamCoordinator {
        // Iniciar procesamiento en modo streaming con workers
        string startStreamingProcessing(string dataFilePath, int numWorkers)
            throws ProcessingException;

        // Detener procesamiento streaming
        void stopStreamingProcessing(string jobId);

        // Obtener resultados calculados en tiempo real
        idempotent ArcSpeedList getStreamingResults(string jobId)
            throws DataNotFoundException;

        // Obtener estadísticas de streaming en tiempo real
        idempotent string getStreamingStats(string jobId);
    };
}