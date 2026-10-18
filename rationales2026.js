/* Explicaciones técnicas individualizadas. Se aplican al banco revisado de 2026,
 sin modificar preguntas, opciones, claves, estados ni historial del alumno. */
const RATIONALE_DETAIL={
3:{
A:"Un worker es una máquina del plano de datos que ejecuta tareas Spark y procesa particiones. No aloja la interfaz web ni pertenece íntegramente al control plane.",
B:"JDBC es un protocolo para conectarse a sistemas de datos externos; el origen puede estar en la red del cliente o de un tercero. Databricks se conecta a él, no lo aloja en su control plane.",
D:"DBFS es una abstracción de ficheros cuya implementación comprende servicios y almacenamiento, incluido cloud storage. Por eso no puede considerarse completamente contenido en el control plane.",
E:"El driver coordina y planifica la ejecución de un trabajo Spark en el compute. En la arquitectura clásica, driver y workers están en el data plane, no en la capa de gestión."
},
4:{
A:"El acceso desde SQL, Python, Scala y otros lenguajes lo ofrece la plataforma y las APIs de Spark/SQL. Delta Lake aporta una tabla transaccional común, pero no es quien incorpora los lenguajes.",
B:"La colaboración simultánea en notebooks es una capacidad de la aplicación Workspace. Una tabla Delta se podría leer igualmente desde un motor externo sin colaboración en notebooks.",
C:"Las alertas de consultas pertenecen a Databricks SQL y a mecanismos de monitorización. Delta Lake controla transacciones e historial, pero no programa ni envía alertas de errores de consultas.",
E:"Distribuir el trabajo en particiones y nodos es responsabilidad del motor Apache Spark. Delta Lake organiza archivos y transacciones, aunque Spark ejecute las operaciones distribuidas."
},
7:{
A:"Checkpointing es el mecanismo que registra qué archivos o microbatches ya se han procesado. Auto Loader lo necesita para reanudar sin reprocesar, pero el motor que ejecuta la consulta incremental es Spark Structured Streaming.",
C:"Databricks SQL permite consultar y transformar tablas mediante SQL warehouses, pero Auto Loader funciona a través de lecturas cloudFiles sobre Structured Streaming; un warehouse SQL no sustituye ese motor.",
D:"Unity Catalog autoriza y gobierna tablas, volúmenes, ubicaciones y credenciales. Puede proteger el destino de Auto Loader, pero no supervisa por sí mismo qué ficheros nuevos han llegado."
},
12:{
A:"Es SQL que podría ejecutarse dentro de un editor de SQL, pero no una expresión de PySpark que devuelva un DataFrame. Desde Python necesitarías spark.sql('SELECT * FROM sales') o spark.table('sales').",
B:"SQL y PySpark pueden consultar las mismas tablas registradas en el catálogo, porque ambos utilizan el motor y los metadatos compartidos. No es necesario copiar la tabla ni convertirla.",
C:"spark.sql interpreta su argumento como una consulta SQL completa. 'sales' es solo el nombre de una tabla y no es una sentencia; spark.table('sales') sí interpreta directamente ese nombre.",
D:"La API de SparkSession no tiene un método spark.delta.table estándar. Para obtener el DataFrame de una tabla registrada utiliza spark.table('sales') o, para SQL completo, spark.sql(...)."
},
13:{
A:"DESCRIBE LOCATION no es una sentencia SQL admitida para extraer las propiedades de un esquema de Unity Catalog. DESCRIBE SCHEMA EXTENDED sí muestra los metadatos extendidos.",
B:"DROP SCHEMA borra el registro del esquema (con las restricciones correspondientes), una operación destructiva. La pregunta solicita consultar su ubicación sin modificar ningún objeto.",
D:"ALTER SCHEMA sirve para cambiar propiedades o configuración del esquema, no para inspeccionarlas. Un cambio en DBPROPERTIES no devuelve la ubicación real de almacenamiento.",
E:"USE SCHEMA solo cambia el esquema predeterminado para las consultas posteriores de la sesión. No muestra la ubicación del esquema ni sus propiedades extendidas."
},
17:{
A:"Unity Catalog puede conceder permisos sobre la ubicación de archivos o sobre la tabla de destino, pero no lleva por sí mismo un registro de los ficheros ya ingeridos. Se requiere un mecanismo de descubrimiento incremental.",
B:"Delta Lake es el formato transaccional apropiado para persistir los datos ingeridos. No monitoriza automáticamente una carpeta externa para detectar nuevos archivos; eso lo hace Auto Loader.",
C:"Databricks SQL permite consultas y tareas programadas, pero no incorpora el seguimiento de archivos nuevos propio de Auto Loader (cloudFiles + checkpoint) cuando el directorio acumula ficheros.",
},
25:{
A:"Unity Catalog gobierna acceso, ownership y metadatos. No crea por sí solo una regla del tipo EXPECT (col IS NOT NULL) ni métricas de incumplimientos durante una actualización.",
B:"Catalog Explorer (antes Data Explorer) muestra tablas, permisos, lineage y metadatos. Puedes inspeccionar datos, pero no convierte una condición de calidad en una expectativa ejecutada automáticamente en cada actualización.",
C:"Delta Lake ofrece consistencia ACID, historial y garantías de escritura. Una tabla puede ser perfectamente transaccional y aun contener valores nulos o incorrectos; para calidad de negocio se requieren expectations.",
E:"Auto Loader identifica e ingiere nuevos archivos mediante Structured Streaming; no define políticas de calidad como WARN, DROP ROW o FAIL UPDATE sobre las filas. Puede alimentar un pipeline que sí las utilice."
},
30:{
A:"La fuente rawSalesLocation apunta a ficheros de origen: suele corresponder a una ingesta hacia Bronze, no a una transformación Silver → Gold. No hay agregación o entidad de negocio.",
B:"Calcular avgPrice fila a fila en una tabla detallada es una transformación de limpieza o enriquecimiento típica de Silver. No genera agregados de ventas por tienda propios de un Gold analítico.",
C:"Filtrar filas por units > 0 elimina registros y suele ser una depuración Silver. No transforma el detalle en un agregado de negocio, a diferencia de groupBy(store) + sum(sales)."
},
32:{
A:"DROP ROW simplemente excluye del dataset las filas que incumplen la expectativa y registra métricas. Nunca crea por sí solo una tabla de cuarentena; esta debe diseñarse como flujo independiente.",
B:"Ni DROP ROW ni las expectativas añaden automáticamente una columna is_valid. Para conservar inválidos con una marca debes definir tú una transformación que cree esa columna.",
D:"Esta descripción corresponde al modo por defecto WARN: registra violaciones pero conserva los registros en el resultado. Aquí ON VIOLATION DROP ROW exige no escribirlos en el destino.",
E:"Hacer fallar la actualización completa sería ON VIOLATION FAIL UPDATE, no DROP ROW. Esta cláusula deja continuar el procesamiento de los registros válidos."
},
33:{
A:"La elección de Streaming Table depende de cómo llegan y se procesan los cambios, no del tipo de dataset que se define después. Un downstream puede ser materialized view u otra streaming table.",
C:"No son simples vistas SQL intercambiables: una streaming table mantiene estado y procesamiento incremental; una materialized view conserva el resultado de una consulta y refresca sus datos.",
D:"Un agregado batch sobre el histórico suele encajar mejor en una materialized view; no es el motivo exclusivo para elegir Streaming Table, que requiere una fuente y transformación aptas para streaming.",
E:"Si la fuente nunca cambia, el procesamiento incremental aporta poco. La streaming table se utiliza para incorporar incrementos, no para imponer inmutabilidad de la fuente."
},
37:{
A:"Clonar una tarea solo duplica su definición. Si no añades la relación 'Depends on' a la tarea original, el Job puede ejecutar ambas sin el orden obligatorio nueva → original.",
C:"Si la tarea nueva depende de la original, el DAG queda original → nueva. Es exactamente el orden contrario al necesario para corregir el problema aguas arriba antes de procesar los datos.",
D:"Ejecutar ambas tareas en paralelo no garantiza que la corrección termine antes de que arranque el notebook original. Hace falta una dependencia explícita entre tareas.",
E:"Clonar una tarea a un Job nuevo no establece automáticamente una dependencia con la tarea del Job original. Aquí se requiere que la nueva tarea sea prerequisito de la original dentro del Job existente."
},
41:{
A:"'Depends on' especifica qué tarea previa debe finalizar antes de lanzar otra; no borra ni sustituye ninguna tarea. Si se pretende reemplazar una tarea, hay que modificar el DAG o eliminar su configuración.",
C:"Dos tareas pueden usar exactamente las mismas librerías y ejecutarse en paralelo. Compartir dependencias de Python/Spark no significa tener una dependencia de ejecución entre tareas del Job.",
D:"El campo Depends on controla precedencia y condiciones de ejecución, no el tamaño del cluster ni el consumo de DBUs. Para limitar recursos se configuran las opciones de compute."
},
42:{
A:"Una configuración Spark aislada no declara qué datasets se crean ni qué lecturas y transformaciones tienen. El pipeline necesita definiciones SQL o Python de sus tablas, vistas y flujos.",
C:"En Unity Catalog, la ubicación de almacenamiento puede estar gestionada y el origen de código ser archivo o notebook. No existe un requisito universal de definir una raíz DBFS de salida para cada pipeline.",
D:"Lakeflow puede publicar objetos en Unity Catalog y no exige el antiguo Hive metastore como destino. Esta alternativa impone una condición histórica que contradice la configuración moderna."
},
43:{
A:"Un SQL Warehouse ejecuta consultas SQL optimizadas; no actúa como pool de nodos Spark para reducir el arranque de clusters clásicos asociados a notebooks y tareas PySpark.",
B:"Un job cluster es adecuado para jobs y se crea al arrancar la ejecución, pero todavía puede necesitar aprovisionar VMs. Asociarlo a un instance pool reduce precisamente esa fase de espera.",
C:"Un cluster single-node puede reducir nodos a provisionar, pero sigue requiriendo iniciar su VM y puede perder paralelismo de Spark. No reutiliza instancias preparadas.",
E:"Autoscaling cambia el número de workers mientras el cluster ejecuta una carga. No asegura que existan VMs preparadas antes del primer arranque; ese problema es el que aborda un pool."
},
44:{
A:"Esta sentencia no concede ALL PRIVILEGES y además usa una nomenclatura DATABASE heredada. USE SCHEMA permite acceder al espacio de nombres; no equivale a crear, modificar y otras acciones del esquema.",
B:"La sintaxis invierte principal y objeto y aplica privilegios a una base que no está identificada correctamente. La concesión correcta especifica GRANT ALL PRIVILEGES ON SCHEMA catalog.schema TO principal.",
C:"SELECT se concede sobre tablas o vistas, no convierte al esquema completo en administrable. También omite varias facultades que pueden incluirse en ALL PRIVILEGES.",
D:"OWNERSHIP es un control de propiedad distinto: el propietario administra el objeto y la delegación. ALL PRIVILEGES no confiere automáticamente propiedad ni MANAGE."
},
45:{
A:"VIEW no es el privilegio USE SCHEMA que habilita resolver nombres dentro de un esquema UC; además, aquí se está intentando operar sobre un catálogo con nombres invertidos.",
B:"CREATE autoriza crear objetos, pero no es el permiso preciso de uso requerido por el caso; no identifica además el esquema catalog.schema de forma válida.",
C:"USE CATALOG debe concederse en un CATALOG, no en un SCHEMA, y a un principal. El enunciado ya presupone USE CATALOG y pide el siguiente nivel, USE SCHEMA.",
D:"SELECT requiere un objeto TABLE o VIEW concreto, y la instrucción propuesta intercambia identificadores. Aunque existiera un SELECT válido, todavía se necesita USE SCHEMA."
},
63:{
A:"IS NULL da TRUE justo cuando falta location, lo contrario de una regla de obligatoriedad. Además, al no llevar ON VIOLATION FAIL UPDATE, el modo por defecto es registrar el incumplimiento sin detener la actualización.",
C:"La condición de obligatoriedad es correcta, pero DROP ROW simplemente elimina del resultado las filas sin location. El negocio exige que la actualización falle para que se investigue el dato ausente.",
D:"La condición IS NOT NULL es adecuada, pero ON VIOLATION FAIL no es la cláusula admitida. Debe escribirse exactamente ON VIOLATION FAIL UPDATE."
},
72:{
A:"Describe el modo triggered: procesa los datos disponibles y termina el update. El enunciado establece Continuous, que permanece activo esperando nuevos datos.",
B:"En el banco antiguo, B y E describen casi lo mismo. La supuesta diferencia en la motivación de persistencia de recursos no refleja una distinción técnica objetiva; esta pregunta debería revisarse por ambigüedad.",
C:"Continuous no procesa una sola vez para quedarse totalmente ocioso; mantiene el pipeline activo y realiza procesamiento cuando llegan novedades.",
D:"La finalización tras una sola actualización correspondería a triggered, no a continuous. Mantener algunos recursos para desarrollo no cambia ese hecho."
},
76:{
B:"El resultado ingerido por Auto Loader puede alimentar modelos de ML, pero ML no es el tipo de ejecución de lectura que utiliza cloudFiles. La ingesta se construye con Structured Streaming.",
C:"Serverless describe quién administra el compute y su escalado; Auto Loader puede ejecutarse en distintos tipos compatibles de compute. No necesita exclusivamente serverless.",
D:"Auto Loader puede ejecutarse como trabajo acotado (por ejemplo AvailableNow), pero la API de lectura sigue siendo Structured Streaming; 'batch' como tipo de carga no describe el motor de Auto Loader.",
E:"Un dashboard consulta información ya persistida y transformada. Auto Loader se sitúa antes, en la ingestión de archivos desde almacenamiento, no en la capa de visualización."
},
80:{
A:"Eso es ON VIOLATION DROP ROW: descarta solo los registros inválidos y registra métricas. FAIL UPDATE no permite confirmar con éxito la actualización afectada.",
C:"FAIL UPDATE no dispone automáticamente de una tabla de cuarentena. Si necesitas investigar filas rechazadas, diseña una salida adicional desde una fuente que conserve esos registros.",
D:"Retener las filas y registrar el incumplimiento es el comportamiento WARN, que es diferente de abortar la actualización con FAIL UPDATE.",
E:"La cláusula no agrega un campo is_invalid al resultado: esa es una transformación que tendrías que definir tú. Con FAIL UPDATE no se confirma el lote inválido."
},
81:{
A:"Bronze procura preservar datos del origen, con errores y heterogeneidad posibles. Silver normaliza, valida o deduplica; por ello el nivel de limpieza no se invierte.",
B:"Las agregaciones pueden aparecer en cualquier capa según necesidad, pero suelen asociarse a Gold para consumo. Silver no se define por estar agregada, sino por su calidad y conformidad.",
C:"Silver puede tener más filas si normaliza o expande registros, o menos si deduplica/filtra. La cantidad nunca determina por sí sola la calidad de una capa.",
E:"Una limpieza que elimina duplicados reduce filas, pero un explode o una normalización puede aumentarlas. No hay regla arquitectónica que obligue a Silver a tener menos registros."
},
82:{
A:"Una política Spot/Reliability de clusters no es un ajuste que resuelva el arranque en segundos de un SQL Warehouse Serverless. El factor decisivo es el tipo de warehouse y su aprovisionamiento.",
B:"Auto Stop apaga warehouses ociosos para reducir costes; cuando vuelve una consulta hay que encenderlos y aparece justamente la espera por cold start que se quiere evitar.",
C:"Aumentar tamaño puede acelerar la ejecución de consultas una vez activo el warehouse, pero no garantiza mejorar el tiempo de aprovisionamiento de un warehouse detenido.",
E:"Aumentar la escala máxima permite más capacidad para múltiples consultas concurrentes, pero no aborda de forma directa el tiempo de arranque inicial desde estado detenido."
},
92:{
A:"Git folders/versionado en repositorios sirve para colaboración y control de cambios de informes, SQL y notebooks. Un pool solo reserva instancias cloud para arrancar clusters clásicos más rápidamente.",
B:"Permisos para ejecutar o consultar informes se gestionan con roles, ACL y Jobs/SQL. Un instance pool no distribuye el informe ni autoriza a los destinatarios.",
D:"La reproducibilidad depende de código versionado, librerías y entorno reproducible. Un pool reduce esperas de aprovisionamiento, pero no congela datos, runtime ni dependencias."
},
107:{
B:"El tamaño de la tabla no determina que DROP TABLE elimine sus archivos. Una external puede ocupar 2 GB y conservarlos, y una managed de mayor tamaño sigue bajo gestión de su metastore.",
C:"Las managed tables también viven físicamente en una ubicación de cloud storage, aunque esté administrada. No tener LOCATION explícita no significa que carezcan de archivos.",
D:"En una external, el catálogo deja de referenciar la tabla, pero los archivos subyacentes permanecen normalmente en el almacenamiento externo. El comportamiento descrito encaja con managed."
},
130:{
A:"Aumentar el tamaño de un único warehouse puede acelerar una consulta pesada, pero el problema son muchas consultas simultáneas que hacen cola. Elevar la cantidad de clusters máximos aumenta concurrencia.",
C:"Auto Stop reduce coste apagando un warehouse ocioso; no incrementa la cantidad de consultas atendidas en paralelo mientras está activo.",
D:"Serverless puede aportar escalado rápido, pero el ajuste concreto solicitado en un warehouse ya encendido y con cola de consultas es elevar la escala máxima de clusters, no solo cambiar el tipo."
},
137:{
B:"El contenido es JSON, no un documento YAML válido como definición de recursos de un Declarative Automation Bundle. El CLI espera estructuras YAML bajo 'resources:' y 'jobs:' (y un databricks.yml raíz).",
C:"Asignar un diccionario a una variable Python no constituye la definición YAML de recursos que interpreta el Databricks CLI; no hay un mapeo resources/jobs válido.",
D:"Las llaves y expresiones corresponden a HCL/Terraform, no a la estructura declarativa YAML que utiliza Databricks Bundles."
},
144:{
B:"cloud_files es el formato para Auto Loader de ficheros en object storage. No es un conector para consumir un topic Kafka. Para Kafka se necesita readStream.format('kafka') con bootstrap.servers y subscribe.",
C:"La referencia FROM STREAM kafka.\`...\` no configura el conector Kafka en la API SQL de este escenario. El código correcto utiliza spark.readStream.format('kafka') y opciones de brokers/topic.",
D:"cloud_files('kafka://...') es una confusión entre ingesta de archivos y mensajería. Kafka se lee mediante un source Kafka; cloud_files requiere un path soportado de archivos."
},
149:{
A:"Databricks Connect no obliga a utilizar un IDE concreto. Permite desarrollar en un IDE local compatible conectándose a compute remoto para ejecutar operaciones Spark.",
B:"La ventaja de Connect es precisamente ejecutar/depurar código desde el entorno local; limitarlo a notebooks de la web elimina ese caso de uso.",
C:"Connect necesita conexión y credenciales para enviar operaciones a Databricks compute. No convierte tu portátil sin red en un cluster Databricks remoto independiente."
},
151:{
A:"El decorador y spark.read son adecuados para un materialized view batch, pero format('csv') leería el JSON del origen como CSV, produciendo parseo incorrecto o errores de esquema.",
B:"spark.format.json(...) no es una API válida de SparkSession para lectura. Es spark.read.json(path) o spark.read.format('json').load(path).",
D:"format('parquet') espera archivos Parquet; el enunciado indica explícitamente archivos JSON. Debe seleccionarse el lector del formato real."
},
153:{
A:"25 MB no coincide con el límite de salida total documentado para notebook tasks en Jobs (30 MB). Se trata de la salida serializada del notebook, no del volumen de datos procesados por Spark.",
B:"10 MB es inferior al límite de 30 MB aplicable a la salida total de un notebook ejecutado como tarea de Job. No es un tamaño máximo de tabla o DataFrame.",
D:"15 MB no coincide con el límite documentado de 30 MB para salida total del notebook. Ese límite no restringe cuánto dataset puede leer y transformar Spark."
},
160:{
A:"Esta opción mezcla sintaxis de una tabla declarativa y STREAM sin paréntesis; además, aunque una expectation con DROP ROW podría filtrar NULL, aquí el ejemplo pide una sentencia correcta con FROM STREAM(orders_raw).",
B:"Le falta FROM STREAM(orders_raw), que indica lectura incremental de la fuente en SQL Lakeflow. No basta con que la tabla destino sea streaming si la lectura se expresa de manera incompatible.",
D:"CREATE OR REPLACE STREAMING TABLE y FILTER (...) como cláusula de tabla no son la sintaxis SQL de este caso. El filtro debe aparecer en WHERE y la fuente en FROM STREAM(...)."
},
187:{
A:"Escoger las mismas columnas de ZORDER mantiene la selección manual y puede no adaptarse a un patrón de filtros cambiante. Automatic Liquid Clustering decide claves según el uso observado para reducir administración.",
B:"El caching puede beneficiar lecturas repetidas concretas, pero no corrige el layout físico de una Delta table que recibe filtros variables y requiere optimización sostenida.",
C:"Cambiar manualmente claves ZORDER y ejecutar OPTIMIZE repetidamente conserva la carga operativa. Con UC managed y predictive optimization, Automatic Liquid Clustering es la alternativa moderna para este escenario."
},
192:{
A:"Un pipeline legado con APIs externas y JDBC/ODBC puede depender de conectores, red o configuraciones no disponibles en serverless. No es un candidato inicial seguro para reducir riesgo.",
C:"El soporte de Scala tiene restricciones según la modalidad de serverless; no es un candidato de primera migración tan directo como una transformación Python/SQL compatible.",
D:"Dashboard BI poco frecuente no prueba un ahorro significativo por migración de un pipeline de ingeniería de datos. El caso pide escoger un primer workload compatible y recurrente."
},
193:{
B:"Un cluster dedicado a uso continuo no se crea y termina para cada ejecución batch; mantiene ocioso compute fuera de la ventana de trabajo, lo que puede aumentar coste.",
C:"All-purpose está diseñado para sesiones interactivas compartidas; mantenerlo disponible para tareas nocturnas puede suponer costes ociosos y facturación menos ventajosa que jobs compute.",
D:"SQL Serverless está optimizado para consultas y tareas SQL, no sustituye necesariamente tareas ETL Spark configuradas con requisitos clásicos de cluster. Si el workload fuese compatible con serverless jobs, esa sería otra alternativa."
},
201:{
A:"Classic SQL Warehouse ofrece un servicio SQL básico, pero Pro añade funcionalidades de rendimiento avanzadas para consulta analítica con networking controlado. La necesidad de conectividad privada no obliga a elegir la opción más básica.",
B:"Serverless SQL Warehouse suele ser la opción recomendada, pero en este enunciado no se configuró la conectividad desde serverless al origen en red privada. Sin ruta de red, la mayor velocidad de arranque no ayuda.",
D:"Un All-purpose compute cluster puede ejecutar Spark SQL en un notebook, pero no es un SQL Warehouse para consultas SQL concurrentes de analistas con la configuración específica de warehouse que exige la pregunta."
},
214:{
A:"Un script Python puede generar configuraciones como herramienta auxiliar, pero la CLI de bundles exige reconocer un databricks.yml raíz con al menos el mapping bundle; no lo reemplaza un script arbitrario.",
C:"Archivos TXT dispersos no forman el manifiesto de proyecto ni representan automáticamente un mapeo válido de recursos/targets para Databricks CLI. YAML es el formato de configuración.",
D:"bundle_config.json no es el nombre de configuración raíz que descubre el CLI. Debe existir databricks.yml, aunque incluya otros archivos YAML de recursos."
},
216:{
B:"ALL PRIVILEGES sobre un catálogo se hereda por sus objetos gobernados según reglas UC, pero crear EXTERNAL LOCATION es una operación del metastore y storage credential, no de un esquema del catálogo.",
C:"Ser Workspace Admin da control sobre el workspace, pero no sustituye necesariamente los privilegios sobre la credencial de almacenamiento y el metastore; el error requiere permisos específicos.",
D:"No es necesario dar Metastore Admin para que alguien cree una ubicación externa: CREATE EXTERNAL LOCATION sobre metastore y storage credential aplica mínimo privilegio."
},
218:{
B:"Los retries de task normalmente vuelven a intentar ejecuciones fallidas bajo la misma política, pero no expresan una reparación selectiva del run original con las opciones explícitas de repairRun.",
C:"Publicar de nuevo un bundle cambia la definición desplegada del Job, pero no vuelve a ejecutar automáticamente solo las tareas fallidas de un run concreto conservando la relación de repair de ese run.",
D:"Run now crea un nuevo run independiente. No sustituye ni elimina el historial del anterior y no se vincula como reparación selectiva, a diferencia de repairRun."
},
225:{
A:"Un HTML exportado del notebook describe la salida de su ejecución, no necesariamente el event log estructurado de Lakeflow ni las métricas por expectation y dataset.",
C:"Spark UI muestra planes físicos, stages y rendimiento; no es el registro de métricas de calidad declarativa para contar violaciones por regla y tabla del último update.",
D:"Cambiar a DEBUG genera logs de diagnóstico del driver, pero el event log de Lakeflow ya registra métricas estructuradas; parsear Log4j es más frágil y no identifica de forma fiable expectations."
},
233:{
A:"Liquid Clustering reorganiza datos según claves para acelerar filtros. No automatiza por sí solo el calendario de VACUUM ni sustituye al servicio de predictive optimization.",
B:"Auto Compaction/Optimized Writes ayudan a los tamaños y disposición de ficheros al escribir, pero no programan el mantenimiento completo con VACUUM según necesidad de cada tabla.",
C:"Auto Loader descubre ficheros entrantes y los ingiere incrementalmente; sus ajustes de schema evolution no ejecutan OPTIMIZE ni VACUUM de tablas administradas UC."
},
238:{
A:"El standard connector/ingesta full refresh de Snowflake relee todos los registros, incluidos los que no cambiaron. Eso contradice el requisito de replicar solo cambios cada cuatro horas.",
C:"Un filtro de fecha hecho a mano solo detectaría cambios si existe columna confiable y no captura automáticamente deletes o cambios sin marca. Además, polling cada hora incumple la cadencia solicitada de cuatro horas.",
D:"Releer toda la tabla de Snowflake en cada ciclo consume recursos/red; aunque luego MERGE evite duplicados en el destino, no cumple la obligación de extraer únicamente cambios."
},
243:{
B:"Eliminar runs anteriores quita comparaciones necesarias para detectar si la latencia pasó de cinco a doce minutos de forma sostenida o solo en ejecuciones puntuales.",
C:"Una sola reejecución puede variar por carga o cachés y no sustituye comparar las métricas históricas de compute, stages y duración de los runs de la semana.",
D:"Incrementar compute a ciegas podría reducir duración, pero no identifica la causa raíz: skew, I/O, espera de recursos o aumento del volumen podrían requerir soluciones distintas."
},
247:{
A:"Un único conector no representa la semántica de SQL CDC, ficheros de S3 y un topic Kafka; además, desactivar historial de versiones contradice auditorías con snapshots temporales.",
C:"Full loads constantes pueden ser viables para algunas fuentes, pero pierden eficiencia incremental y no establecen por sí solos captura fiel de cambios ni lineage homogéneo entre orígenes.",
D:"Un batch con un único conector no consume Kafka como flujo de eventos ni implementa la semántica de CDC de SQL. Guardar snapshots externos tampoco asegura lineage UC."
},
252:{
B:"Un retraso fijo de cinco minutos no sabe cuánto tarda la limpieza; si tarda seis, la consulta arrancará demasiado pronto. Depends on expresa la condición real de orden y resultado.",
C:"On Failure ejecuta una rama ante fallo, justo lo contrario del requisito de generar el reporte solo después del éxito de la limpieza.",
D:"Encadenar código en un solo notebook puede forzar secuencia local, pero deja de modelar explícitamente dos tasks independientes y sus dependencias, retries y observabilidad en Lakeflow Jobs."
},
262:{
A:"Una variable global en tu ordenador sirve a esa sesión local, pero no define una asignación reproducible dev/stage/prod compartida y versionada que la CLI pueda desplegar a cada destino.",
B:"Incrustar host o path en el notebook mezcla código de transformación con configuración del entorno y obliga a editar código entre STAGE y PROD, aumentando riesgo de publicar al destino incorrecto.",
D:"El README es solo documentación humana. La CLI no interpreta su contenido como la configuración de host, workspace.root_path o variables de un target."
},
275:{
A:"Un SQL Warehouse es apropiado para tareas SQL concretas y analítica, pero no sustituye compute de Jobs para notebooks PySpark o tareas mixtas de ingeniería nocturna.",
B:"All-purpose puede quedar encendido entre lotes y requiere gestión de clusters; suele ser menos eficiente para automatizaciones nocturnas que Jobs Serverless.",
D:"Classic Jobs compute se crea al ejecutar el Job y es válido, pero exige seleccionar runtime, nodos y políticas. Serverless gestiona aprovisionamiento y escalado para workloads compatibles y reduce tareas operativas."
}
};
function applyRationaleDetails(q){
  const map=RATIONALE_DETAIL[q.id];
  if(!map)return;
  if(!q.wrongWhy||typeof q.wrongWhy!=="object")q.wrongWhy={};
  for(const [key,explanation] of Object.entries(map)){
    if(q.options.some(o=>o.key===key)&&!q.correct.includes(key))q.wrongWhy[key]=explanation;
  }
}
