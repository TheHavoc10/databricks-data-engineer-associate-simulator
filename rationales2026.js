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
B:"La opción mezcla procesamiento continuo con apagar recursos tras el primer refresco. En un pipeline continuous, el procesamiento permanece activo hasta que se detiene.",
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

// Segundo bloque: SQL, transacciones, gobierno, ingesta y control de versiones.
Object.assign(RATIONALE_DETAIL,{
5:{E:"Una tabla Delta no equivale a un único fichero de datos. Suele contener múltiples Parquet y un _delta_log con commits, cambios de esquema y estadísticas, que hacen posibles ACID y Time Travel."},
6:{
A:"SELECT * ... WHERE age > 25 identifica filas, pero no altera la tabla persistida. La orden que elimina físicamente filas del conjunto lógico es DELETE FROM my_table WHERE age > 25.",
B:"UPDATE modifica columnas existentes mediante SET column = value; este fragmento ni tiene SET ni ordena borrar filas. Para quitar registros hay que utilizar DELETE FROM con predicado WHERE.",
E:"DELETE sí elimina filas, pero age <= 25 selecciona las de 25 años o menos. El requisito es borrar las que superan 25, por lo que la condición debe ser age > 25."
},
8:{
A:"COUNT(member_id) omite por definición las filas donde member_id es NULL; si una tabla tiene 10 filas y 3 NULL, devuelve 7, justo lo contrario de lo pedido.",
D:"NULL no es una función agregada y SELECT null(member_id) no cuenta registros. Para saber cuántos valores faltan se evalúa COUNT_IF(member_id IS NULL) o COUNT(*) - COUNT(member_id)."
},
9:{
A:"Poder almacenar datos estructurados y no estructurados es una ventaja de un data lake tradicional; no garantiza que un write parcialmente fallido deje la tabla consistente como lo haría una transacción ACID.",
C:"Un motor SQL puede consultar un CSV sin transacciones ACID. Que exista SELECT no evita lecturas inconsistentes ni escrituras parciales; ACID sí define esas garantías.",
D:"Parquet y otros formatos abiertos facilitan interoperabilidad, pero un fichero Parquet por sí solo no mantiene un registro transaccional ni asegura atomicidad de cambios concurrentes.",
E:"Entrenar modelos o usar IA es un caso de uso de la plataforma; puede realizarse sobre datos inconsistentes. La mejora de integridad de una tabla frente a un data lake tradicional viene de ACID."
},
10:{C:"Una Delta Table conserva físicamente registros en archivos administrados por el formato Delta, aunque nadie la consulte. La pregunta pide combinar tablas temporalmente sin persistir resultados, por lo que la vista temporal es mejor."},
11:{
A:"Un representante comercial de Databricks no obtiene automáticamente MANAGE ni ownership sobre los objetos de Unity Catalog de tu metastore. Debe intervenir un usuario con permisos de administración técnica.",
B:"Unity Catalog permite ALTER TABLE ... OWNER TO cuando lo ejecuta un principal autorizado, como un metastore administrator. El hecho de que el propietario original se marche no vuelve la tabla intransferible."
},
14:{
A:"La sentencia CREATE TABLE puede llevar COMMENT 'Contains PII' para documentar que la tabla contiene datos personales. Lo que no hace COMMENT por sí solo es aplicar una política automática de mascarado.",
E:"Escribir PII sin un campo ni una cláusula SQL no añade metadatos a la tabla. Se necesita COMMENT '...' o una propiedad/etiqueta válida en la definición del objeto."
},
15:{A:"La interfaz web de Databricks es un servicio gestionado por Databricks en el control plane clásico. La pregunta busca lo almacenado en la cuenta cloud del cliente, donde se alojan los datos en storage."},
16:{
A:"DROP TABLE retira un objeto del catálogo; no compara claves entre origen y destino ni conserva una fila existente si coincide con una nueva.",
B:"IGNORE por sí solo no es una instrucción general de upsert en Delta. MERGE INTO permite especificar cuándo se actualiza una coincidencia y cuándo se inserta la fila nueva.",
D:"APPEND añade todas las filas entrantes sin comprobar si la clave ya existe. Repetir un batch puede introducir duplicados; MERGE enlaza fuentes y destinos mediante ON.",
E:"INSERT INTO incorpora filas nuevas sin resolver qué hacer con las que ya existen según una clave. MERGE INTO permite MATCHED UPDATE y NOT MATCHED INSERT en una sola operación."
},
19:{
A:"COPY INTO acepta el formato de los ficheros mediante FILEFORMAT = 'PARQUET', entre otros ajustes. El motivo del segundo run sin filas nuevas es el seguimiento de ficheros ya cargados, no la imposibilidad de especificar formato.",
B:"COPY INTO puede leer todos los ficheros elegibles de un directorio. La cláusula FILES restringe un subconjunto, pero no es obligatoria para el seguimiento incremental.",
D:"Parquet es un formato admitido como entrada de COPY INTO, igual que CSV y JSON según la configuración. El que una recarga no inserte filas apunta a idempotencia, no a incompatibilidad del formato.",
E:"Una carga COPY INTO confirmada se ve en la tabla sin un segundo REFRESH obligatorio. La reejecución ignora los ficheros ya cargados, salvo que se fuerce expresamente."
},
20:{
A:"Cambiar LOCATION o mover ficheros es un problema de almacenamiento y metadatos, no de matching entre filas de dos tablas. MERGE se emplea cuando se necesita actualizar/insertar según claves.",
B:"Una tabla Delta externa puede recibir tanto INSERT como MERGE si cumple permisos y requisitos. Ser external solo describe quién gestiona su ubicación, no la semántica de deduplicación.",
C:"MERGE requiere un destino Delta compatible; el origen puede proceder de distintas lecturas. Que el source no sea Delta no obliga a utilizar MERGE en lugar de INSERT."
},
21:{
B:"En Spark el lector JDBC se identifica con format('jdbc') y opciones como url/dbtable. Auto Loader trabaja con nuevos archivos desde cloud storage y no sustituye el conector de una base transaccional.",
C:"sqlite-stream no es el identificador de un proveedor JDBC admitido por spark.read.format. Un origen SQL compatible se lee usando el provider jdbc y el driver adecuado al motor."
},
23:{
A:"No hay umbral de 10 GB que cambie qué elimina DROP TABLE. Las tablas managed y external responden distinto por su modo de administración, no por el tamaño de sus archivos.",
B:"Que una tabla pese menos de 10 GB tampoco hace que DROP TABLE elimine todos los ficheros. El efecto sobre storage depende de si el catálogo administra los datos o solo la referencia externa."
},
24:{
B:"Una FUNCTION recibe argumentos y devuelve un valor o relación calculada al ejecutarse, pero no crea por sí sola una tabla persistente para diferentes equipos y sesiones.",
D:"Una TEMP VIEW existe solo en el ámbito temporal correspondiente y consulta su fuente; no materializa archivos compartidos y puede desaparecer al terminar la sesión."
},
27:{
A:"DateType es una clase de esquema Spark para almacenar fechas en columnas, no un mecanismo de planificación. Para definir una cadencia de Job se utiliza una expresión cron o un trigger.",
C:"TimestampType representa el tipo de un valor temporal dentro de un DataFrame. No define a qué hora del día o día de semana se dispara un Lakeflow Job."
},
28:{
A:"El código SQL escrito solo no ejecuta una consulta desde Python. Al integrarlo en una variable DataFrame necesitas una llamada como spark.sql('SELECT * FROM sales').",
B:"spark.delta.table no es un método estándar de SparkSession. spark.table('sales') lee una tabla por nombre, mientras spark.sql(query) ejecuta una sentencia SQL completa."
},
31:{B:"Llamar trigger() sin los parámetros necesarios no fija la latencia pedida de cinco segundos. El ejemplo correcto explicita trigger(processingTime='5 seconds') para microbatches periódicos."},
35:{
A:"Git folders permite generar commits de tus cambios en el workspace vinculado a un repo. La integración completa entre ramas normalmente ocurre en el proveedor Git remoto; no se pide un commit.",
B:"Pull trae del proveedor remoto los últimos cambios a la rama activa del Git folder. Es una operación disponible en la experiencia Databricks, así que no es la que normalmente haces solo desde GitHub.",
D:"Clone crea una copia conectada de un repo en el workspace; forma parte de la configuración inicial de Git folders. Merge de ramas mediante PR normalmente se hace en el proveedor externo."
},
39:{
A:"INNER JOIN empareja registros de dos tablas con una condición ON; no concatena marzo y abril. Las ventas de meses diferentes se apilan con UNION/UNION ALL, no por claves comunes.",
C:"OUTER JOIN busca filas relacionadas entre conjuntos y añade NULL cuando falta correspondencia. No es la operación para reunir todas las transacciones de dos meses en las mismas columnas.",
D:"INTERSECT devuelve únicamente filas presentes en ambas tablas. La pregunta requiere unir todas las transacciones de marzo y abril, incluidas las exclusivas de cada mes."
},
40:{D:"Bajar el tamaño del warehouse cambia recursos por unidad de tiempo, pero si se mantiene encendido todo el día seguirá generando tiempo ocioso. Auto Stop lo apaga automáticamente después de un periodo de inactividad."},
46:{
A:"Merge integra cambios entre ramas en el proveedor, pero la acción que actualiza el Git folder existente con commits del remoto es Pull. Mezclar ramas no es lo mismo que traer cambios de su rama remota.",
B:"Push publica en remoto commits realizados localmente; aquí se quiere recibir los que otra persona ya publicó, que corresponde a Pull.",
D:"Commit guarda cambios del workspace en un commit local de Git; no importa los cambios ya hechos en el repositorio remoto.",
E:"Clone crearía otro Git folder a partir del remoto. Si ya tienes uno conectado, lo correcto es actualizarlo mediante Pull, sin duplicar la copia de trabajo."
},
49:{
B:"Refrescar informes rápidamente suele depender del arranque de compute, de SQL warehouses o del tamaño del workload. Single-node elimina paralelismo y no proporciona por sí solo baja latencia para un informe automatizado.",
D:"Un único nodo no tiene workers distribuidos entre los que repartir cargas; no ofrece escalado horizontal real para procesamientos que crecen mucho.",
E:"Transformaciones manuales sobre muchos datos suelen aprovechar particiones en varios workers. En single-node toda la carga compite por CPU/memoria de una máquina."
},
51:{
A:"REDUCE es un patrón de agregación/transformación de colecciones en Spark, no una operación de mantenimiento que compacte múltiples archivos pequeños Delta.",
C:"COMPACTION no es el comando de mantenimiento SQL que se pide de forma independiente. Para compactar ficheros de una tabla Delta se usa OPTIMIZE tabla.",
E:"VACUUM borra archivos antiguos que ya no están referenciados y superan el periodo de retención. OPTIMIZE compacta archivos pequeños para mejorar lectura; son tareas diferentes."
},
56:{
A:"Una celda de un notebook Databricks puede usar SQL aunque el notebook sea Python, mediante %sql al principio de esa celda. No hay que crear otro notebook.",
B:"Elegir el intérprete SQL de una celda no obliga a conectarla a un SQL Warehouse distinto: la magia %sql indica el lenguaje de la celda y utiliza el entorno admitido.",
C:"Si el notebook utiliza Python por defecto, escribir SELECT sin %sql haría que Python intentase interpretar un código que no es Python. Se necesita la magia de lenguaje.",
E:"Cambiar el lenguaje por defecto a SQL podría afectar al resto de celdas Python existentes. Con %sql cambias solo una celda y conservas la mezcla de lenguajes."
},
57:{
A:"TRANSFORM sirve para aplicar expresiones a colecciones en SQL de Spark, pero no redistribuye automáticamente valores de una categoría como nuevas columnas de salida.",
C:"SUM agrega importes en una sola expresión; para convertir categorías en cabeceras de columnas necesitas la operación PIVOT, quizá acompañada de SUM como agregador.",
D:"CONVERT se relaciona con conversión de valores/tipos según el dialecto, no con reorientar filas a columnas por categoría.",
E:"WHERE elimina las filas que no cumplen un predicado; no genera nuevas columnas a partir de los valores de una columna."
},
61:{
C:"ALL PRIVILEGES concede más facultades que SELECT para consultar una vista y puede incluir acciones no deseadas. Con el modelo de permisos de UC, acceder a la vista no exige propiedad de sus tablas base al consumidor.",
D:"No es necesario ALL PRIVILEGES del esquema para ejecutar una vista: basta USE CATALOG, USE SCHEMA y SELECT sobre la vista, según el contexto de UC."
},
65:{
B:"Un pipeline declarativo de Lakeflow puede combinar definiciones SQL y Python como fuentes de datasets diferentes. No es un requisito traducir todas las transformaciones Python a SQL.",
C:"Las Streaming Tables forman parte del procesamiento admitido en Lakeflow; convertirlas forzosamente a batch haría perder su semántica incremental, sin que lo exija el producto.",
D:"Tampoco tienes que reescribir todas las consultas SQL en Python. El motor crea el grafo de dependencias a partir de los datasets definidos en ambos lenguajes."
},
66:{
A:"En Python, una función se define con def add_integers(x, y), no con function. Con la sintaxis propuesta, el intérprete falla antes de ejecutar la suma.",
B:"Tiene dos problemas: Python no reconoce function como declaración y además la expresión x + y no se devuelve. La firma correcta utiliza def y el cuerpo return x + y.",
C:"print(x+y) muestra un valor en la salida, pero la función devuelve None. Si una tarea posterior usa el resultado, necesita return x+y.",
E:"En Python una expresión sin return se evalúa y descarta dentro del cuerpo, por lo que la llamada devuelve None. La pregunta exige devolver el número sumado."
},
67:{
B:"processingTime establece cadencia entre microbatches y acepta duraciones como '5 seconds', no 'once'. Para consumir los datos disponibles y terminar se usa availableNow=True.",
C:"Continuous es otro tipo de trigger y no acepta 'once' como valor en este contexto. AvailableNow permite procesar el backlog en microbatches acotados y terminar después."
},
70:{
A:"TRUNCATE vacía la versión vigente y recargar el snapshot antiguo cambia lo que verían los consumidores actuales. Time Travel permite consultar una versión sin sobrescribir el presente.",
D:"VACUUM elimina archivos de versiones antiguas según la retención, justo los que Time Travel podría necesitar para recuperar el estado de hace dos semanas. No sirve para consultar un snapshot."
},
73:{
A:"Convertir timestamps a un formato común mejora la calidad y homogeneidad del detalle, una labor de Silver. Gold normalmente modela KPIs, agregados o entidades de consumo de negocio.",
B:"Agregar directamente datos no depurados puede propagar duplicados, códigos incoherentes y errores de origen al resultado. Antes de construir Gold se suelen aplicar controles de Silver.",
C:"Eliminar registros corruptos es depuración de calidad típica de Silver; aún no construye una métrica agregada o tabla modelada para consumo de usuarios de negocio.",
E:"Leer eventos de una fuente streaming y almacenarlos fielmente pertenece a Bronze (ingesta cruda). El paso Gold no es simplemente copiar el origen a una tabla."
},
78:{
C:"Una tabla Delta registrada puede leerse incrementalmente por su nombre usando spark.readStream.table('nombre'). No necesitas sustituir el nombre por su path físico si tienes metadatos y permisos válidos.",
D:"El formato sigue siendo delta: stream expresa el modo de lectura con readStream, no es un formato de archivos para la opción format(...)."
},
79:{
A:"La agregación groupBy(store).sum(sales) convierte detalle en métricas de negocio y suele representar Silver → Gold, no la ingesta incremental raw → Bronze pedida.",
B:"La opción filtra una tabla sales ya existente y no consume los ficheros raw recién llegados; corresponde a limpieza de datos que ya se encuentran en una capa anterior.",
C:"withColumn(avgPrice, ...) deriva una columna de una tabla previa, pero no realiza la lectura inicial de archivos de origen, requisito específico del paso hacia Bronze.",
D:"read y write son las APIs batch de Spark; la ingesta incremental necesita readStream y writeStream con checkpoint para registrar el progreso entre ejecuciones."
},
83:{
C:"SELECT concede capacidad de consulta, no el conjunto de privilegios aplicables a la tabla, como MODIFY según el contexto. ALL PRIVILEGES no equivale a solo lectura.",
D:"La instrucción coloca el principal team donde debe ir una tabla y el nombre de tabla donde debe ir el principal; el orden correcto es GRANT ... ON TABLE tabla TO principal."
},
84:{
A:"Databricks Jobs admite notificaciones de ejecución/fallo configurables, de modo que no es necesario que cada celda del notebook implemente manualmente lógica de envío de avisos.",
D:"Lakeflow Jobs puede emitir notificaciones cuando fallan runs o tareas, según configuración. Negar la funcionalidad omite una opción nativa de observabilidad."
},
87:{
B:"Enviar por correo a un destinatario email no es lo mismo que llamar al webhook del servicio de mensajería del equipo. Para integrarlo debe crearse el destino apropiado compatible con el webhook.",
C:"One-time modifica la política/frecuencia con que se dispara la notificación, pero no configura el endpoint externo donde el equipo necesita recibirla.",
E:"Configurar una alerta sin destino de notificaciones deja el resultado en Databricks, pero nadie recibe el mensaje en el canal externo solicitado."
},
88:{
B:"Separar un dashboard en un warehouse diferente no asegura apagar recursos ociosos; puede sumar otro servicio encendido. Auto Stop sí desconecta el warehouse tras inactividad.",
C:"Disminuir tamaño reduce algunos costes instantáneos, pero un warehouse pequeño encendido toda la noche sigue consumiendo recursos. El enunciado busca reducir tiempo activo no usado.",
D:"Compartir SQL Warehouse entre consultas puede aprovechar la infraestructura, pero no garantiza cero gasto durante periodos sin peticiones. Auto Stop actúa sobre la inactividad."
},
100:{
B:"La gramática de SQL es INSERT INTO my_table VALUES (...). La opción invierte INTO y VALUES, así que no es una sentencia válida para insertar la fila.",
C:"UPDATE modifica filas existentes y exige SET columna=valor (y normalmente WHERE). Para incorporar una fila nueva se usa INSERT INTO ... VALUES.",
D:"La cadena mezcla UPDATE con VALUES sin tabla en una posición válida ni cláusula SET. No ejecuta ni una modificación SQL ni una inserción."
},
104:{
A:"DROP elimina la tabla u otro objeto, no compara filas por una clave de negocio. Para evitar duplicados sin perder el destino utiliza MERGE INTO con coincidencia ON.",
B:"INSERT añade filas sin cotejar claves existentes; ejecutar dos veces el mismo lote puede repetir registros. MERGE permite escoger MATCHED/NOT MATCHED.",
D:"APPEND es una modalidad de escritura que agrega registros al final sin búsqueda de claves. No implementa upsert ni deduplicación de filas existentes."
},
108:{
C:"FROM CSV no especifica el proveedor de una tabla externa en CREATE TABLE de Spark SQL. Para datos CSV se requiere la cláusula USING CSV y, si procede, LOCATION.",
D:"USING DELTA asume que la ubicación ya contiene una tabla Delta con _delta_log. Si solo hay archivos CSV, el lector Delta no puede interpretarlos como una tabla Delta."
},
110:{
A:"TRANSFORM aplica funciones a arrays y otros datos, pero no reorganiza los valores de una dimensión como columnas nuevas de un resultado agregado.",
C:"SUM agrega una medida, pero no convierte valores categóricos en columnas; puedes utilizarlo dentro de PIVOT, que es la operación que cambia la orientación.",
D:"CONVERT no designa el operador Spark SQL para reorganizar categorías como columnas. La sentencia solicitada utiliza PIVOT."
},
111:{A:"spark.delta.sql(...) no es método de SparkSession. La consulta SQL dinámica se ejecuta con spark.sql(f'SELECT ... FROM {table_name}'), teniendo en cuenta validar identificadores."},
119:{C:"Gold se destina a consumo y KPIs, por lo que normalmente ofrece una versión más modelada para negocio que las capas anteriores. No es la capa de datos más cruda del lakehouse."},
134:{
B:"DROP TABLE es DDL, pero elimina un objeto ya existente; no responde a una pregunta que pide crear la estructura de una nueva tabla.",
C:"ALTER TABLE ... ADD COLUMN es DDL y modifica una tabla existente; no crea una tabla desde cero como CREATE TABLE.",
D:"INSERT INTO es DML porque agrega registros a una tabla existente, no cambia su definición. Además, el ejemplo carece de una coma entre 1 y 'Alice'."
},
139:{
A:"coalesce reduce particiones sin garantizar una redistribución homogénea; si ya hay una partición sobrecargada, puede seguir igual o concentrar más datos. Repartitioning o salting se valoran ante skew.",
D:"Aumentar memoria puede evitar ciertos spills, pero una única task que procesa la mayoría de filas seguirá durando mucho más que las demás. El origen es distribución desigual por clave."
},
141:{
B:"Reglas de conformidad, validación y enriquecimiento corresponden habitualmente a Silver, no a Bronze, que intenta conservar datos del origen para poder reprocesarlos.",
C:"Limpiar exhaustivamente al ingerir podría descartar información necesaria para auditoría o reproceso. Bronze suele preservar el original y pasar la limpieza a Silver.",
D:"Agregar varias fuentes para responder preguntas de negocio se asocia con Silver conformada o Gold; Bronze es la capa de aterrizaje y trazabilidad de las fuentes."
},
142:{
A:"Delta Lake es un formato de tabla con ACID, no el mecanismo para consultar bases de datos externas en su origen sin copiarlas. Ese caso corresponde a federación de consultas.",
C:"MLflow registra experimentos y modelos de machine learning. No convierte SQL remoto en consultas federadas contra sistemas OLTP o warehousing externos.",
D:"Databricks Connect permite desarrollar localmente usando compute Databricks remoto, no es un conector general para consultar bases corporativas de forma federada."
},
148:{D:"Un SQL Warehouse ejecuta consultas SQL sobre los datos, no tareas genéricas de Python o notebooks arbitrarios. Para esas transformaciones se necesita compute de notebook/job compatible."},
150:{
A:"groupBy('category') agrega por categoría, pero el requisito es obtener el total por región. La clave del groupBy debe coincidir con la dimensión solicitada.",
C:"En la API DataFrame, sum no se encadena antes de groupBy sobre el DataFrame de esta manera. Primero agrupas por región y luego agregas sum('sales_amount').",
D:"groupBy es un método del DataFrame que prepara grupos, no de una expresión Column dentro de agg. El orden válido es df.groupBy('region').agg(sum('sales_amount'))."
}
});

function applyRationaleDetails(q){
  const map=RATIONALE_DETAIL[q.id];
  if(!map)return;
  if(!q.wrongWhy||typeof q.wrongWhy!=="object")q.wrongWhy={};
  for(const [key,explanation] of Object.entries(map)){
    if(q.options.some(o=>o.key===key)&&!q.correct.includes(key))q.wrongWhy[key]=explanation;
  }
}
