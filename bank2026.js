/* Auditoría 2026. La clave original y los 280 IDs se conservan para compatibilidad histórica.
 * Fuentes: examen 04-05-2026 y documentación oficial de Lakeflow, UC, Bundles y Git folders.
 */
const BANK26_ARCHIVE=new Set([26,38,60,68,69,85,86,90,93,97,101,102,103,105,106,109,112,113,114,115,116,117,120,123,124,126,127,128,162,175,183,185,186,196,206,209,230]);
const BANK26_REVIEW=new Set([]);
const BANK26_UPDATE=new Set([11,63,199,201,257,13,15,18,21,25,27,32,33,34,35,36,40,42,43,44,45,46,49,55,56,61,65,67,72,77,80,81,82,83,88,92,94,99,107,111,121,129,130,131,132,135,136,137,138,143,144,146,149,151,153,157,160,166,169,170,171,184,187,189,190,192,193,194,195,208,214,216,218,224,228,233,238,241,242,244,249,252,262,264,268,270,271,272,275]);
const BANK26_DUP={
38:86,68:1,69:54,85:45,90:40,93:3,97:46,101:51,102:24,103:19,105:12,109:58,112:1,113:62,114:66,115:31,116:25,117:36,120:33,123:71,126:32,127:43,128:37,162:201,183:187,185:181,186:182,209:198
};
const BANK26_ISSUES={};
const BANK26_RETIRE_REASON={
26:"Versión duplicada y contradictoria: la respuesta afirmaba que el pipeline no puede combinar fuentes SQL/Python o streaming. Conservamos la nº 65.",
60:"Ninguna alternativa responde realmente a cómo garantizar el cumplimiento del horario de workflows.",
86:"Pregunta dependiente de una interfaz antigua de programación, sin alternativa inequívoca en la experiencia actual.",
106:"Exige memorizar una ruta DBFS del metastore Hive legado, poco representativa de Unity Catalog.",
124:"Premisa falsa: Silver no tiene necesariamente menos filas que Bronze.",
175:"La opción marcada no describe correctamente Repair run; caso repetido y defectuoso.",
196:"Delta Sharing no garantiza evitar costes de egress entre regiones y nubes.",
206:"Duplicada de un escenario de Repair run con opciones incorrectas.",
230:"La clave solo crea una función UDF, pero nunca ejecuta ALTER TABLE ... SET ROW FILTER."
};
const BANK26_NOTES={
11:"Validada en 2026: transferir propiedad es competencia de owner, owner del contenedor, MANAGE o metastore admin. Se precisa el escenario para que C sea inequívoca.",
63:"Validada en 2026: reemplazado '!= NULL' por 'IS NOT NULL' y conservado ON VIOLATION FAIL UPDATE. B correcta.",
199:"Validada en 2026: OpenSharing puede compartir tablas y archivos de notebooks; no se comparten automáticamente las definiciones ni la ejecución de Jobs.",
201:"Validada en 2026: Pro SQL Warehouse para red personalizada cuando no se ha configurado conectividad privada para serverless; Serverless es preferible en general cuando cumple requisitos.",
257:"Validada en 2026: ALTER TABLE SET MANAGED requiere reiniciar los streams afectados con el mismo checkpoint y configuración. No borrar estado.",

13:"Para objetos de Unity Catalog usa DESCRIBE SCHEMA EXTENDED catalog.schema y comprueba la ubicación en los metadatos.",
15:"La respuesta sobre almacenamiento en la cuenta del cliente describe compute clásico; serverless tiene un plano de ejecución gestionado diferente.",
18:"Los horarios se orquestan con Lakeflow Jobs; las condiciones semanales también pueden configurarse mediante tareas separadas o lógica de control.",
21:"Con Spark JDBC usa format('jdbc'), URL y dbtable; evita memorizar proveedores de clases antiguos.",
25:"Las Expectations de Lakeflow Spark Declarative Pipelines permiten monitorizar, descartar o rechazar datos según la regla.",
27:"La programación y el lanzamiento de tareas corresponden a Lakeflow Jobs, con schedules y triggers.",
33:"Streaming Table procesa novedades; Materialized View mantiene una consulta materializada. Son datasets dentro de pipelines declarativos.",
34:"STREAM(source) solicita semántica de lectura incremental. No implica que solo pueda leer otra Streaming Table.",
35:"Databricks Repos se llama ahora Git folders; el merge de ramas se realiza habitualmente en GitHub u otro proveedor remoto.",
40:"SQL endpoint se llama SQL Warehouse. Auto Stop minimiza tiempo sin uso; Serverless reduce operación y tiempo de arranque.",
42:"La fuente del pipeline puede ser código en archivos SQL, Python o notebooks; no es obligatorio un notebook.",
44:"Unity Catalog otorga privilegios sobre SCHEMA (catalog.schema), y ALL PRIVILEGES no equivale a OWNERSHIP ni MANAGE.",
45:"Para explorar un esquema de UC considera USE CATALOG y USE SCHEMA; BROWSE ofrece descubrimiento de metadatos según configuración.",
55:"Los horarios se configuran a nivel de consultas/Jobs según la experiencia actual; SQL endpoint ahora es SQL Warehouse.",
61:"En vistas Unity Catalog se comprueban SELECT sobre la vista, privilegios de uso y el modelo de acceso del propietario.",
65:"En Spark Declarative Pipelines pueden coexistir fuentes y transformaciones SQL y Python; no hay que reemplazar streaming por batch.",
72:"Continuous y Triggered son modos de ejecución del pipeline; Streaming Table no significa necesariamente Continuous.",
80:"FAIL UPDATE hace fallar la actualización afectada y evita confirmar la escritura; no equivale a descartar una sola fila.",
81:"Silver es más depurado que Bronze; no implica menos filas, ni obliga a tener agregaciones.",
82:"El problema de latencia al reactivar un SQL Warehouse se analiza con Serverless y las opciones de arranque.",
83:"Para tabla en UC utiliza nombre de tres niveles si conviene y distingue ALL PRIVILEGES de propiedad y MANAGE.",
88:"SQL endpoint se denomina SQL Warehouse. Auto Stop y Serverless son estrategias diferentes de coste y arranque.",
92:"Un instance pool reduce arranques de clusters clásicos, pero no mejora automáticamente los tiempos de un Job Serverless.",
94:"Git folders reemplaza Databricks Repos y permite ramas Git; distingue commits locales, merges y pull requests.",
107:"DROP TABLE en managed y external UC difiere en ciclo de vida de archivos; considera retención y recuperación.",
121:"Distingue Streaming Table y Materialized View de los modos Continuous/Triggered y del perfil de compute.",
132:"Compartir entre nubes puede generar costes de egress. No presupongas transferencias gratuitas.",
136:"Declarative Automation Bundles es el nombre actual de Databricks Asset Bundles; los comandos bundle de CLI se mantienen.",
138:"La herramienta declarativa de calidad son las EXPECTATIONS definidas en el pipeline.",
144:"La API moderna de Python utiliza from pyspark import pipelines as dp; @dp.table crea una Streaming Table.",
151:"En pipelines modernos, una fuente batch definida en Python puede materializarse con @dp.materialized_view.",
153:"El límite documentado de salida total de un notebook ejecutado como Job es de 30 MB; no lo confundas con el tamaño de datos leídos.",
160:"La sintaxis actual usa CREATE OR REFRESH STREAMING TABLE y FROM STREAM(orders_raw), no CREATE STREAMING LIVE TABLE.",
166:"All-purpose clásico sirve para exploración, pero Databricks recomienda serverless para workloads compatibles.",
169:"Delta Sharing permite datos para destinatarios fuera de Databricks; no exige darles permiso para editar el workspace.",
170:"OPTIMIZE reordena archivos; en managed UC, predictive optimization puede automatizar parte del mantenimiento.",
171:"En sharing D2D identifica el metastore del destinatario; evita pedir credenciales del cliente.",
187:"Automatic Liquid Clustering es una alternativa moderna a particiones con ZORDER; no se arregla siempre con OPTIMIZE manual.",
192:"Serverless elimina parte del aprovisionamiento y administración, pero no evita límites de compatibilidad y costes.",
193:"Para jobs programados evalúa Serverless como primera opción si es compatible; job cluster clásico sigue siendo válido en escenarios específicos.",
195:"Unity Catalog gobierna permisos de tablas externas, credenciales y ubicaciones.",
199:"Compartir notebooks o datasets no despliega automáticamente la definición ejecutable de un Job.",
201:"Pro frente a Serverless SQL depende de requisitos de red y soporte actual; la respuesta debe validarse.",
216:"CREATE EXTERNAL LOCATION requiere privilegios específicos sobre el metastore y el storage credential de referencia.",
218:"Repair run es una ejecución de reparación vinculada al job; valida qué cambios en tasks/compute se recogen.",
230:"Para seguridad por filas se necesita crear la función y aplicarla con ALTER TABLE ... SET ROW FILTER.",
233:"Predictive optimization automatiza parte del mantenimiento en tablas administradas por Unity Catalog.",
257:"La conversión de external a managed puede requerir evaluar el origen y checkpoint del stream.",
262:"Los targets por entorno se configuran en databricks.yml dentro de Declarative Automation Bundles."
};
const BANK26_DOCS={
lakeflow:"https://docs.databricks.com/aws/en/ldp/concepts/where-is-dlt",
sql:"https://docs.databricks.com/aws/en/ldp/developer/sql-dev",
uc:"https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference",
bundles:"https://docs.databricks.com/aws/en/dev-tools/bundles/faqs",
git:"https://docs.databricks.com/aws/en/repos/what-happened-repos",
exam:"https://www.databricks.com/sites/default/files/2026-03/databricks-certified-data-engineer-associate-exam-guide-may-4-2026.pdf",
compute:"https://docs.databricks.com/aws/en/jobs/compute"
};
function bank26Reason(q){
 if(BANK26_RETIRE_REASON[q.id])return BANK26_RETIRE_REASON[q.id];
 if(BANK26_DUP[q.id])return "Variante repetida de la pregunta nº "+BANK26_DUP[q.id]+". Se conserva el original para consultar el material histórico.";
 if(BANK26_ISSUES[q.id])return BANK26_ISSUES[q.id];
 if(BANK26_NOTES[q.id])return BANK26_NOTES[q.id];
 const t=q.question+" "+q.options.map(o=>o.text).join(" ");
 if(/asset bundles|declarative automation bundle/i.test(t))return "Usar la denominación Declarative Automation Bundles (antes Databricks Asset Bundles) y mantener los comandos bundle.";
 if(/repos|git folder/i.test(t))return "Git folders es la denominación actual de Repos; verificar diferencias entre UI y Git remoto.";
 if(/delta live|dlt\b|streaming live|\blive table\b/i.test(t))return "Usar Lakeflow Spark Declarative Pipelines y distinguir streaming table, materialized view y modo de ejecución.";
 if(/sql endpoint/i.test(t))return "Usar la denominación SQL Warehouse y diferenciar serverless de compute clásico.";
 if(/delta sharing/i.test(t))return "Mantener los conceptos de Delta Sharing y advertir los costes de transferencia y los permisos del destinatario.";
 if(/cluster|compute|serverless|pool/i.test(t))return "Contextualizar la respuesta clásica frente a serverless, compute de jobs y políticas actuales.";
 return "Contexto revisado respecto al temario Data Engineer Associate de mayo de 2026. Contrasta detalles de interfaz o comandos en la documentación oficial.";
}
function bank26Apply(questions){
 for(const q of questions){
   q.bank26Original={question:q.question,options:q.options.map(o=>({key:o.key,text:o.text})),correct:q.correct.slice()};
   q.bank26State=BANK26_ARCHIVE.has(q.id)?"archive":BANK26_REVIEW.has(q.id)?"review":BANK26_UPDATE.has(q.id)?"updated":"current";
   q.bank26Note=bank26Reason(q);
   if(q.bank26State!=="updated")continue;
   let edited=false;
   function swap(re,rep){const v=q.question.replace(re,rep);if(v!==q.question){q.question=v;edited=true}}
   // Cambios terminológicos que no alteran la lógica de las alternativas.
   swap(/Databricks Repos/gi,"Git folders (antes Databricks Repos)");
   swap(/Databricks Asset Bundles/gi,"Declarative Automation Bundles (antes Databricks Asset Bundles)");
   swap(/\bSQL endpoint(s)?\b/gi,"SQL warehouse$1");
   swap(/Delta Live Tables(?: \(DLT\))?/gi,"Lakeflow Spark Declarative Pipelines (antes Delta Live Tables)");
   swap(/Delta Live Table pipeline/gi,"Lakeflow Spark Declarative Pipeline");
   swap(/DLT pipeline/gi,"Lakeflow Spark Declarative Pipeline");
   swap(/Delta Sharing/gi,"OpenSharing (antes Delta Sharing)");
   // Las respuestas correctas que contienen un nombre antiguo se renombran;
   // NO convertimos automáticamente las opciones erróneas en otras soluciones.
   q.options.forEach(o=>{
     if(!q.correct.includes(o.key))return;
     const old=o.text;
     o.text=o.text.replace(/Databricks Asset Bundles/gi,"Declarative Automation Bundles (antes Databricks Asset Bundles)")
       .replace(/Databricks Repos/gi,"Git folders")
       .replace(/\bSQL endpoint(s)?\b/gi,"SQL warehouse$1")
       .replace(/Delta Live Tables/gi,"Lakeflow Spark Declarative Pipelines (antes DLT)")
       .replace(/Delta Sharing/gi,"OpenSharing (antes Delta Sharing)");
     if(o.text!==old)edited=true;
   });
   const custom=bank26Custom[q.id];
   if(custom){custom(q);edited=true}
   q.bank26Edited=edited;
   q.bank26Doc=/DLT|LIVE|pipeline|streaming/i.test(q.bank26Original.question)?BANK26_DOCS.lakeflow:
     /Repo|Git folder/i.test(q.bank26Original.question)?BANK26_DOCS.git:
     /Asset Bundle|Declarative Automation/i.test(q.bank26Original.question)?BANK26_DOCS.bundles:
     /catalog|grant|database|schema/i.test(q.bank26Original.question)?BANK26_DOCS.uc:BANK26_DOCS.exam;
 }
}
const bank26Custom={
11(q){
  q.question="A Unity Catalog table owner has left the company and no longer has access. The table is not in the workspace's default workspace catalog. No remaining team member owns the table's parent schema/catalog or has the MANAGE privilege on the table. Who among the following can transfer ownership of the table to the new lead data engineer using Catalog Explorer?";
  q.options=[
    {key:"A",text:"Databricks account representative"},
    {key:"B",text:"Nobody: ownership cannot be transferred"},
    {key:"C",text:"Unity Catalog metastore administrator"},
    {key:"D",text:"The new lead data engineer, solely because of their role"},
    {key:"E",text:"The former owner, despite having no account access"}
  ];
  q.answerWhy="Un administrador del metastore puede transferir la propiedad de tablas de Unity Catalog. También podrían hacerlo el propietario actual, el propietario del catálogo/esquema contenedor y quien tenga MANAGE; se han excluido expresamente esas otras posibilidades del escenario.";
  q.wrongWhy={
    A:"Un representante comercial no es un rol con privilegios de Unity Catalog.",
    B:"La propiedad sí puede transferirse a otro usuario o grupo autorizado.",
    D:"Ser el nuevo responsable del equipo no otorga automáticamente MANAGE ni propiedad.",
    E:"El antiguo propietario sin acceso ya no puede ejecutar acciones sobre la tabla."
  };
  q.issue=null;
  q.sourceLinks=[{label:"Databricks · Transfer ownership",url:"https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/ownership"}];
},
63(q){
  q.question="A Lakeflow Spark Declarative Pipeline processes employee travel reimbursements. The field location must be provided. The pipeline must fail the affected update if any record has NULL in location. Which expectation is correct?";
  q.options=[
    {key:"A",text:"CONSTRAINT valid_location EXPECT (location IS NULL)"},
    {key:"B",text:"CONSTRAINT valid_location EXPECT (location IS NOT NULL) ON VIOLATION FAIL UPDATE"},
    {key:"C",text:"CONSTRAINT valid_location EXPECT (location IS NOT NULL) ON VIOLATION DROP ROW"},
    {key:"D",text:"CONSTRAINT valid_location EXPECT (location IS NOT NULL) ON VIOLATION FAIL"}
  ];
  q.answerWhy="IS NOT NULL detecta si falta el valor, y ON VIOLATION FAIL UPDATE detiene y revierte la actualización afectada. En modo triggered otros flujos independientes podrían continuar; en continuous se detiene también el procesamiento dependiente.";
  q.wrongWhy={
    A:"IS NULL acepta precisamente los registros sin ubicación; además, sin ON VIOLATION se registran métricas pero se conservan filas inválidas.",
    C:"DROP ROW descarta registros inválidos, pero no cumple el requisito de hacer fallar la actualización.",
    D:"La sintaxis documentada es ON VIOLATION FAIL UPDATE, no FAIL a secas."
  };
  q.issue=null;
  q.sourceLinks=[{label:"Databricks · Expectations",url:"https://docs.databricks.com/aws/en/ldp/expectations"}];
},
199(q){
  q.question="A partner organization also uses a Unity Catalog-enabled Databricks workspace. You must securely share Unity Catalog-managed Delta tables and the notebook files containing the SQL/Python source code that produces those tables. The partner does not need your Lakeflow Job definitions, schedules, compute settings, or run permissions. Which approach supports this?";
  q.options=[
    {key:"A",text:"Email a ZIP of the data and notebooks and grant direct access to your cloud data lake."},
    {key:"B",text:"Share the tables and notebook files using OpenSharing (formerly Delta Sharing) and manage the shares and recipients with Unity Catalog."},
    {key:"C",text:"Grant Unity Catalog SELECT on your own tables and assume that this automatically publishes the notebooks and data to an external organization."},
    {key:"D",text:"Share a GitHub repository and expect Git access to share the governed table data automatically."}
  ];
  q.answerWhy="OpenSharing admite compartir tablas y archivos de notebooks con recipients. Las definiciones ejecutables, programación y credenciales de los Jobs no se publican por compartir un notebook; esos recursos requieren un despliegue o permisos propios.";
  q.wrongWhy={
    A:"Correo y acceso directo al storage no ofrecen el mecanismo de distribución gobernada de OpenSharing.",
    C:"Los grants de UC en tu propio metastore no publican automáticamente notebooks y tablas como recursos compartidos entre organizaciones.",
    D:"Git versiona notebooks y código, pero no crea shares de tablas gobernados mediante Unity Catalog."
  };
  q.issue=null;
  q.sourceLinks=[{label:"Databricks · OpenSharing assets",url:"https://docs.databricks.com/aws/en/opensharing/create-share"}];
},
201(q){
  q.question="A data engineer needs SQL exploratory analysis over a database reachable only inside the company's customer-managed cloud network. Custom networking is required and private connectivity from Databricks Serverless to that database has not been configured. Which SQL Warehouse type is most appropriate?";
  q.answerWhy="Pro SQL Warehouse se ejecuta en el entorno cloud del cliente y se recomienda cuando hay redes personalizadas y conectividad de tipo federado/híbrido. Serverless ofrece mejor arranque y escalado para exploración general cuando su conectividad y límites sí cumplen los requisitos.";
  q.wrongWhy={
    A:"Classic proporciona funciones SQL básicas pero no Predictive IO; Pro es más adecuado para la combinación de red personalizada y rendimiento.",
    B:"Serverless es la recomendación general, pero el escenario excluye la conectividad privada necesaria para llegar al origen.",
    D:"All-purpose es un recurso de notebooks, no un SQL Warehouse para ejecutar estas consultas SQL."
  };
  q.issue=null;
  q.sourceLinks=[{label:"Databricks · SQL warehouse types",url:"https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-types"}];
},
257(q){
  q.question="A Unity Catalog external Delta table is converted to a managed table using ALTER TABLE catalog.schema.table_name SET MANAGED. A Structured Streaming job reading the table was running during conversion and stops after the conversion. How should the engineer restore processing while preserving progress?";
  q.answerWhy="Databricks indica reiniciar las lecturas/escrituras streaming tras SET MANAGED con la misma configuración y checkpoint. La redirección de lecturas basadas en ruta requiere runtimes compatibles; conviene migrar las lecturas a nombres de tabla cuando corresponda.";
  q.wrongWhy={
    A:"El error de conversión no se resuelve normalmente concediendo acceso directo al nuevo almacenamiento gestionado: Unity Catalog gobierna el acceso.",
    C:"REFRESH TABLE invalida cachés de metadatos y datos, pero no reinicia el stream ni reescribe su checkpoint.",
    D:"Eliminar el checkpoint pierde el seguimiento incremental y puede provocar reprocesos; Databricks pide reiniciar conservando la configuración."
  };
  q.issue=null;
  q.sourceLinks=[{label:"Databricks · Convert to managed",url:"https://docs.databricks.com/aws/en/tables/convert-to-managed"}];
},

13(q){
 q.question="In a Unity Catalog-enabled Databricks workspace, the schema main.customer360 has a managed storage location. Which SQL statement shows extended schema metadata, including its location?";
 q.options=[{key:"A",text:"DESCRIBE LOCATION main.customer360;"},{key:"B",text:"DROP SCHEMA main.customer360;"},{key:"C",text:"DESCRIBE SCHEMA EXTENDED main.customer360;"},{key:"D",text:"ALTER SCHEMA main.customer360 SET DBPROPERTIES ('location' = '/user');"},{key:"E",text:"USE SCHEMA main.customer360;"}];
 q.answerWhy="DESCRIBE SCHEMA EXTENDED catalog.schema muestra metadatos extendidos del esquema en Unity Catalog.";
},
21(q){
 q.question="A notebook needs to read a supported transactional source through JDBC before writing to a Unity Catalog table. Which Spark DataFrame reader format is correct?";
 q.options=[{key:"A",text:"spark.read.format('jdbc').option('url', jdbc_url).option('dbtable', 'customer360').load()"},{key:"B",text:"spark.read.format('autoloader').option('url', jdbc_url).load()"},{key:"C",text:"spark.read.format('sqlite-stream').load(jdbc_url)"},{key:"D",text:"spark.read.format('unity-catalog-jdbc').load(jdbc_url)"}];
 q.answerWhy="En Spark, .format('jdbc') con url y dbtable utiliza el lector JDBC para ingerir fuentes compatibles. Requiere el driver JDBC del motor.";
},
25(q){
 const o=q.options.find(x=>x.key==="D");if(o)o.text="Lakeflow Spark Declarative Pipelines (antes Delta Live Tables), mediante expectations";
 q.answerWhy="Las expectations de Lakeflow permiten evaluar reglas de calidad, registrar métricas, descartar filas inválidas o hacer fallar actualizaciones.";
},
33(q){
 q.question="In Lakeflow Spark Declarative Pipelines, when should CREATE OR REFRESH STREAMING TABLE be used instead of CREATE OR REFRESH MATERIALIZED VIEW?";
 q.options=[{key:"A",text:"Only when the next pipeline step is a static table."},{key:"B",text:"When new records must be processed incrementally using streaming semantics."},{key:"C",text:"It is redundant because all declarative datasets behave as ordinary views."},{key:"D",text:"Only for complex full-batch aggregations over all historical records."},{key:"E",text:"Only when the source table never changes."}];
 q.answerWhy="Una Streaming Table procesa incrementos con semántica streaming, mientras que Materialized View mantiene materializado el resultado de una consulta.";
},
34(q){
 q.question="A Lakeflow pipeline contains the following SQL:\nCREATE OR REFRESH STREAMING TABLE loyal_customers AS\nSELECT customer_id FROM STREAM(customers) WHERE loyalty_level = 'high';\nWhy is STREAM(customers) used?";
 const c=q.options.find(o=>o.key==="C");if(c)c.text="To read customers with streaming semantics and process new records incrementally.";
 q.answerWhy="STREAM(source) indica una lectura streaming; no exige que la fuente ya sea otra Streaming Table.";
},
35(q){q.question="Working with Databricks Git folders (formerly Repos), which Git operation is typically performed in the external Git provider rather than as the main workspace action?";},
42(q){
 q.question="Which of the following best describes a valid code source when configuring a Lakeflow Spark Declarative Pipeline?";
 q.options=[{key:"A",text:"A mandatory key-value Spark configuration with no source code."},{key:"B",text:"SQL or Python pipeline definitions supplied through supported source files or notebooks."},{key:"C",text:"A manually selected DBFS root data directory is mandatory."},{key:"D",text:"A Hive database target is mandatory even when using Unity Catalog."}];
 q.answerWhy="Lakeflow admite definiciones declarativas en SQL/Python y no exige que la fuente sea un notebook; archivos del workspace también son válidos.";
},
44(q){
 q.question="A team must work with the objects of Unity Catalog schema main.customers. Which SQL GRANT statement best matches ALL PRIVILEGES on this schema (not schema ownership)?";
 q.options=[{key:"A",text:"GRANT USAGE ON DATABASE customers TO team;"},{key:"B",text:"GRANT ALL PRIVILEGES ON DATABASE team TO customers;"},{key:"C",text:"GRANT SELECT ON TABLE main.customers TO team;"},{key:"D",text:"GRANT OWNERSHIP ON SCHEMA main.customers TO team;"},{key:"E",text:"GRANT ALL PRIVILEGES ON SCHEMA main.customers TO team;"}];
 q.answerWhy="ALL PRIVILEGES ON SCHEMA se aplica al esquema y a sus objetos según herencia de privilegios; no concede OWNERSHIP ni MANAGE. Para acceso efectivo al esquema, también se necesita USE CATALOG.";
},
45(q){
 q.question="An engineering group team already has USE CATALOG on main. Which grant lets members use schema main.customers in Unity Catalog?";
 q.options=[{key:"A",text:"GRANT VIEW ON CATALOG customers TO team;"},{key:"B",text:"GRANT CREATE ON DATABASE customers TO team;"},{key:"C",text:"GRANT USE CATALOG ON SCHEMA team TO customers;"},{key:"D",text:"GRANT SELECT ON TABLE team TO customers;"},{key:"E",text:"GRANT USE SCHEMA ON SCHEMA main.customers TO team;"}];
 q.answerWhy="USE SCHEMA permite utilizar objetos del esquema, con los privilegios específicos correspondientes. Para descubrir metadatos también existe BROWSE a nivel de catálogo.";
},
80(q){
 const b=q.options.find(o=>o.key==="B");if(b)b.text="The affected pipeline update fails and the invalid data is not committed to the target table.";
 q.answerWhy="ON VIOLATION FAIL UPDATE provoca fallo de la actualización/flujo afectado; no registra la fila incorrecta como si fuera válida, ni crea automáticamente cuarentena.";
},
83(q){q.question="A team needs applicable ALL PRIVILEGES on Unity Catalog table main.sales_data.sales (not ownership). Which GRANT statement is correct?";
 q.options=[{key:"A",text:"GRANT ALL PRIVILEGES ON TABLE main.sales_data.sales TO team;"},{key:"B",text:"GRANT SELECT CREATE MODIFY ON TABLE main.sales_data.sales TO team;"},{key:"C",text:"GRANT SELECT ON TABLE main.sales_data.sales TO team;"},{key:"D",text:"GRANT ALL PRIVILEGES ON TABLE team TO main.sales_data.sales;"}];
 q.answerWhy="ALL PRIVILEGES sobre una tabla concede los privilegios aplicables a esa tabla; no concede propiedad y requiere los permisos de uso en catalog/schema para el acceso.";
},
144(q){
 q.question=q.question.replace(/\bDLT\b/gi,"Lakeflow Spark Declarative Pipelines");
 const a=q.options.find(o=>o.key==="A");if(a)a.text=a.text.replace(/import dlt/g,"from pyspark import pipelines as dp").replace(/@dlt\.table/g,"@dp.table");
 q.answerWhy="En la API moderna, from pyspark import pipelines as dp y @dp.table definen una Streaming Table. spark.readStream.format('kafka') lee Kafka incrementalmente.";
},
151(q){
 q.question=q.question.replace(/\bDLT\b/gi,"Lakeflow Spark Declarative Pipelines");
 const map={A:"from pyspark import pipelines as dp\n@dp.materialized_view\ndef raw_customers():\n    return spark.read.format('csv').load('s3://my-bucket/raw-customers/')",
 B:"from pyspark import pipelines as dp\n@dp.materialized_view\ndef raw_customers():\n    return spark.format.json('s3://my-bucket/raw-customers/')",
 C:"from pyspark import pipelines as dp\n@dp.materialized_view\ndef raw_customers():\n    return spark.read.json('s3://my-bucket/raw-customers/')",
 D:"from pyspark import pipelines as dp\n@dp.materialized_view\ndef raw_customers():\n    return spark.read.format('parquet').load('s3://my-bucket/raw-customers/')"};
 q.options.forEach(o=>{if(map[o.key])o.text=map[o.key]});
 q.answerWhy="Para una lectura batch de JSON en una transformación declarativa Python se usa @dp.materialized_view y spark.read.json(...). Para ingestión incremental haría falta readStream y @dp.table.";
},
160(q){q.question=q.question.replace(/using DLT/gi,"using Lakeflow Spark Declarative Pipelines");},
193(q){q.question+="\nAssume this workload requires configurable classic compute rather than serverless jobs.";q.answerWhy="Con el requisito explícito de compute clásico, un Job Cluster escala conforme al job y finaliza tras él. En cargas compatibles, Databricks suele recomendar Serverless Jobs como alternativa.";},
};
