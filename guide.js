
/* Glosario de Databricks Associate: definiciones contrastadas, interfaz local. */
const GUIDE_DOCS={
delta:"https://docs.databricks.com/aws/en/delta/",
catalog:"https://docs.databricks.com/aws/en/data-governance/unity-catalog/",
lakeflow:"https://docs.databricks.com/aws/en/ldp/concepts",
pipeline:"https://docs.databricks.com/aws/en/ldp/concepts/pipelines",
modes:"https://docs.databricks.com/aws/en/ldp/concepts/pipeline-mode",
flows:"https://docs.databricks.com/aws/en/ldp/flows",
expect:"https://docs.databricks.com/aws/en/ldp/expectations",
loader:"https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema",
copy:"https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into",
compute:"https://docs.databricks.com/aws/en/jobs/compute",
classic:"https://docs.databricks.com/aws/en/compute/use-compute",
pool:"https://docs.databricks.com/aws/en/compute/pools",
cluster:"https://docs.databricks.com/aws/en/delta/clustering",
optimize:"https://docs.databricks.com/aws/en/delta/optimize",
bundle:"https://docs.databricks.com/aws/en/dev-tools/bundles/settings",
cli:"https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands",
deploy:"https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes"
};
const GUIDE_CATS=["Datos y gobierno","Ingesta","Pipelines","Compute","Rendimiento","DevOps"];
const GUIDE_RAW=[
["Datos y gobierno","Delta Lake","▰","Capa de tablas transaccionales sobre archivos (principalmente Parquet), con registro Delta y garantías ACID.","Batch y streaming pueden leer y escribir tablas Delta; no es el motor de cálculo.","Lakehouse con tablas Delta sobre OneLake: principio muy parecido.","delta"],
["Datos y gobierno","ACID","◈","Garantías transaccionales: atomicidad, consistencia, aislamiento y durabilidad.","Una operación no queda confirmada a medias; la calidad semántica de los datos sigue siendo responsabilidad tuya.","Las tablas Delta de Fabric ofrecen garantías ACID.","delta"],
["Datos y gobierno","Unity Catalog","⬡","Sistema de gobierno de datos: objetos, privilegios, lineage y descubrimiento.","Jerarquía catalog.schema.table; USE CATALOG + USE SCHEMA + SELECT para leer una tabla.","No equivale solo a Purview: combina catálogo, permisos de ejecución y gobierno.","catalog"],
["Datos y gobierno","Managed / External Table","▣","En managed, Unity Catalog administra metadatos y ciclo de vida de archivos; en external, la ubicación de datos es externa.","DROP de external elimina referencia, no los archivos. Managed se gestiona según políticas de retención.","Tablas administradas y externas del Lakehouse ofrecen una comparación aproximada.","catalog"],
["Datos y gobierno","Time Travel","◷","Consulta versiones anteriores de tablas Delta mediante VERSION AS OF o TIMESTAMP AS OF.","Depende de que los archivos necesarios sigan retenidos; VACUUM puede impedir recuperar una versión.","Delta Time Travel también está disponible en Fabric Spark.","delta"],
["Datos y gobierno","Change Data Feed (CDF)","⇄","Registro de cambios de filas de una tabla Delta habilitada para CDF.","Expone inserts, deletes y preimagen/posimagen de updates; no se habilita retroactivamente en tablas antiguas.","Delta CDF existe en el ecosistema Delta, pero sus superficies compatibles varían.","delta"],
["Datos y gobierno","OPTIMIZE","◫","Reorganiza o compacta archivos para mejorar su disposición física.","No es lo mismo que VACUUM: OPTIMIZE mejora el layout; VACUUM elimina archivos obsoletos según retención.","En Fabric también hay mantenimiento y optimización de tablas Delta.","optimize"],
["Datos y gobierno","VACUUM","⌫","Limpia archivos de datos ya no referenciados que superan la retención aplicable.","Puede limitar Time Travel porque elimina archivos necesarios para estados históricos.","Fabric también usa VACUUM sobre tablas Delta, con consideraciones de retención.","delta"],
["Datos y gobierno","Liquid Clustering","▤","Organiza datos según claves de clustering y permite cambiar esas claves con flexibilidad.","Alternativa a particionamiento tradicional y ZORDER; puede ser útil con claves de cardinalidad alta.","No hay equivalencia general 1:1 para todas sus funciones en Fabric.","cluster"],
["Datos y gobierno","Delta Sharing","↗","Protocolo abierto para compartir datos con otros destinatarios sin copiar manualmente CSV.","Compartir tablas no equivale a compartir notebooks o permisos de edición.","Fabric tiene OneLake data sharing y otras opciones, con mecanismos distintos.","catalog"],
["Datos y gobierno","Lakehouse Federation","◎","Permite consultar determinadas fuentes externas sin ingerirlas previamente en tablas Delta.","Federación = consultar en origen; Lakeflow Connect = traer datos.","Fabric ofrece accesos federados, Mirroring y shortcuts según el origen, pero no son idénticos.","catalog"],
["Datos y gobierno","Lakebase","▥","Base de datos PostgreSQL operacional para apps y agentes, distinta de Delta Lake.","Lakebase = OLTP; Lakehouse/Delta = analítica y procesamiento a gran escala.","Parecido conceptualmente a Azure Database for PostgreSQL conectado con Fabric.","lakeflow"],

["Ingesta","Batch / Streaming","⤳","Batch procesa conjuntos delimitados; streaming procesa novedades según una consulta incremental.","Una fuente streaming no obliga a tener un pipeline encendido las 24 horas.","Pipelines/notebooks batch frente a streaming en Fabric RTI o Spark.","pipeline"],
["Ingesta","Spark Structured Streaming","≋","API/motor de Spark para consultas incrementales sobre streams.","Auto Loader se apoya en Structured Streaming; el checkpoint ayuda a guardar progreso.","Es el mismo Apache Spark Structured Streaming que puedes usar en Fabric.","loader"],
["Ingesta","Auto Loader","⇣","Ingiere archivos nuevos desde object storage, con seguimiento y opciones de evolución de esquema.","cloudFiles y readStream; puede usar triggers acotados como AvailableNow.","Patrón equivalente mediante Spark/Pipelines, pero Auto Loader es una utilidad propia de Databricks.","loader"],
["Ingesta","Checkpoint","▢","Estado persistente de progreso y, cuando corresponde, del procesamiento de Structured Streaming.","No es el archivo origen ni una copia de todos los datos; evita relecturas/reprocesos indebidos.","Spark Streaming de Fabric también utiliza checkpoints.","loader"],
["Ingesta","Trigger.AvailableNow","◴","Ejecuta todos los datos disponibles en microbatches y después finaliza.","Streaming incremental no implica ejecución continua ni cluster permanente.","Disponible en Structured Streaming; patrón comparable en notebooks Spark Fabric.","modes"],
["Ingesta","File Arrival Trigger","♧","Disparador de Lakeflow Jobs que inicia una ejecución al detectar archivos nuevos en una ubicación supervisada.","El trigger arranca el Job; Auto Loader decide qué archivos ingerir mediante su checkpoint.","Fabric tiene activadores por eventos para iniciar procesos, con implementación diferente.","compute"],
["Ingesta","COPY INTO","⇥","Comando SQL idempotente para cargar archivos de una ubicación hacia una tabla Delta.","En sucesivas ejecuciones omite ficheros ya cargados, salvo opciones de forzado.","En Fabric replicarías el patrón con Copy Activity y control de ingesta, o Spark.","copy"],
["Ingesta","CTAS","⊞","CREATE TABLE AS SELECT: crea una tabla a partir de la salida de una consulta.","Ideal para creación/carga inicial o puntual; no mantiene por sí solo control de nuevos ficheros.","Igual concepto en Spark SQL de Fabric.","copy"],
["Ingesta","Lakeflow Connect","⇩","Familia de herramientas/conectores para obtener datos desde archivos, SaaS o bases de datos.","Connect ingiere; SDP transforma; Jobs orquesta.","La ingesta de Fabric se distribuye entre Data Factory, Mirroring y otros servicios.","lakeflow"],
["Ingesta","Managed vs Standard Connectors","◇","Managed: ingestión de orígenes empresariales con infraestructura administrada. Standard: acceso a datos ya disponibles en fuentes compatibles, como archivos.","En el test: fuente SaaS/BD externa → normalmente managed; archivos en cloud → standard/Auto Loader.","Data Factory y Mirroring cubren escenarios parecidos, no 1:1.","lakeflow"],
["Ingesta","_rescued_data","▧","Columna JSON de Auto Loader que retiene campos inesperados, conflictos de tipo o de mayúsculas/minúsculas.","No confundas dato rescatado con un registro completamente corrupto.","Se puede implementar cuarentena/schema drift en Fabric, pero no igual de integrado.","loader"],
["Ingesta","schemaHints","≔","Indica tipos de columnas durante la inferencia del esquema sin imponer que estén presentes en todos los archivos.","Columna ausente en un fichero → NULL cuando forma parte del esquema inferido.","En Fabric puedes definir/inferir esquemas en Spark, sin el mismo parámetro.","loader"],
["Ingesta","_metadata","⌁","Columna con datos del fichero de entrada: ruta, nombre, tamaño y fecha de modificación, entre otros.","Sirve para trazabilidad en Bronze; no es el contenido ni el esquema del JSON.","Spark en Fabric permite extraer metadatos de ficheros mediante otras APIs.","loader"],
["Ingesta","CDC / AUTO CDC","↺","CDC registra cambios de sistemas origen; AUTO CDC los aplica mediante flujos declarativos, con soporte SCD1/SCD2.","CDC de una BD externa no es lo mismo que Delta CDF de una tabla Delta.","Fabric Mirroring y pipelines pueden cubrir CDC con mecanismos distintos.","lakeflow"],

["Pipelines","Lakeflow","◎","Suite de ingeniería de datos: Connect (ingesta), Pipelines (transformaciones) y Jobs (orquestación).","Lakeflow no es una base de datos ni un único tipo de pipeline.","Combina capacidades que en Fabric están repartidas entre Data Factory, Spark y otros.","lakeflow"],
["Pipelines","Spark Declarative Pipelines (SDP)","☷","Definición SQL/Python de datasets y dependencias; el motor calcula el DAG.","El código está en fuentes/notebooks; no describes el orden manual de ejecución de cada notebook.","Se aproxima a notebooks Spark + transformaciones gestionadas, no al Pipeline-orquestador de Fabric.","pipeline"],
["Pipelines","DLT (Delta Live Tables)","◈","Nombre histórico que aún aparece en temarios; las capacidades se encuadran hoy en Lakeflow pipelines.","Lee preguntas antiguas de DLT como conceptos de streaming tables, expectativas y pipelines declarativos.","No hay equivalente 1:1 en Fabric.","pipeline"],
["Pipelines","Streaming Table","⇢","Tabla incremental alimentada por un flujo streaming, adecuada para fuentes que crecen.","Puede actualizarse en modo triggered; no equivale a un pipeline continuous.","Se parece a una tabla Delta alimentada con Structured Streaming.","lakeflow"],
["Pipelines","Materialized View","▰","Resultado persistido de una consulta, actualizado para reflejar las fuentes; puede refrescar incrementalmente cuando es posible.","Especialmente útil para joins, agregaciones o Gold; no es una view virtual.","Materialized views de Fabric Warehouse tienen otros requisitos y comportamientos; no son 1:1.","lakeflow"],
["Pipelines","View","▤","Consulta lógica no materializada, evaluada bajo demanda.","VIEW no guarda por sí sola los resultados físicos como una materialized view.","SQL views de Fabric Warehouse / Spark SQL, según el motor.","lakeflow"],
["Pipelines","Flow / CREATE FLOW","⬈","Unidad de lectura/proceso/escritura dentro de un pipeline; permite declarar flujos independientes.","CREATE FLOW explícito permite varios flujos de origen hacia un mismo target.","En Fabric tenderías a construir actividades o notebooks separados.","flows"],
["Pipelines","Triggered / Continuous","⏱","Son modos de ejecución del pipeline: triggered procesa lo disponible y se detiene; continuous sigue activo.","Modo del pipeline ≠ tipo de tabla. Una streaming table puede ser triggered.","Comparable con ejecución programada versus proceso streaming sostenido en Fabric.","modes"],
["Pipelines","Expectations: WARN","ⓘ","Condición de calidad que registra incumplimientos sin descartar registros.","EXPECT sin cláusula ON VIOLATION retiene las filas inválidas y produce métricas.","Fabric requiere controles explícitos o librerías de data quality.","expect"],
["Pipelines","Expectations: DROP ROW","⊘","Descarta del destino las filas que incumplen la regla, y registra métricas de descarte.","No crea cuarentena automática; Bronze/origen puede conservar los datos.","En Fabric puedes filtrar y separar inválidos con Spark o dataflows.","expect"],
["Pipelines","Expectations: FAIL UPDATE","✕","Si un registro incumple, falla la actualización afectada; una transacción de tabla se revierte.","No implica siempre detener todos los flujos independientes: triggered permite otros paralelos.","Puedes implementar validaciones fail-fast mediante código, no igual de declarativo.","expect"],
["Pipelines","Medallion","◉","Patrón Bronze (fiel al origen) → Silver (depurado y conformado) → Gold (consumo de negocio).","Bronze no es Gold; Silver no tiene por qué agregar; Gold suele optimizarse para consumo.","Es el mismo patrón arquitectónico en Fabric.","lakeflow"],
["Pipelines","Lakeflow Jobs / Tasks","▦","Orquestador de tareas (notebooks, SQL, pipelines, etc.), dependencias, triggers y alertas.","Job = cuándo/qué tareas ejecutar; SDP = relaciones y transformaciones de datos.","Mucho más cercano al Pipeline de Data Factory/Fabric.","compute"],
["Pipelines","Repair Run","↩","Permite reparar una ejecución fallida, reejecutando tareas fallidas/omitidas de un job según las opciones disponibles.","No es necesariamente necesario relanzar todas las tareas ya correctas.","Se parece a reintentos/reanudación gestionada de una orquestación.","compute"],

["Compute","Control Plane","⌘","Servicios de gestión, UI, APIs, metadatos y orquestación de Databricks.","La web application pertenece al control plane; workers y driver clásicos no.","Comparable con el plano de control administrado de Fabric.","classic"],
["Compute","Compute / Data Plane","▣","Capa que ejecuta procesos y accede a datos, distinta de la UI y servicios de control.","En compute clásico, driver y workers viven en el data plane.","Fabric abstrae la infraestructura Spark, aunque sigue existiendo procesamiento separado.","classic"],
["Compute","All-Purpose Cluster","▤","Compute clásico de uso interactivo para desarrollo, notebooks y exploración.","Flexible, pero no es la opción recomendada por defecto para producción programada.","Similar a sesiones Spark interactivas de Fabric, sin equivalencia de configuración exacta.","classic"],
["Compute","Classic Job Cluster","⚙","Compute clásico creado para ejecutar tareas de jobs y terminado al acabar, con posibilidad de compartirlo entre tareas.","Comparado con serverless, suele penalizar el arranque; pools pueden ayudar.","Comparable con iniciar compute Spark para tareas de Fabric; no idéntico.","compute"],
["Compute","Serverless","☁","Databricks administra aprovisionamiento y escalado del compute para workloads compatibles.","No todas las tareas ni todas las opciones de runtime son compatibles; revisar límites.","Fabric abstrae compute con capacidad y Spark serverless-like, con facturación distinta.","compute"],
["Compute","SQL Warehouse","▥","Compute optimizado para consultas SQL/BI; puede ser Serverless o Classic/Pro según disponibilidad.","No es un notebook ni un cluster de jobs genérico.","Aproximadamente, el motor SQL de Warehouse/SQL analytics en Fabric.","compute"],
["Compute","Instance Pool","◷","Reserva instancias para acelerar creación/arranque de compute clásico.","El pool no procesa tareas directamente; mantener instancias inactivas puede generar coste cloud.","No hay equivalencia directa visible como objeto dentro de Fabric.","pool"],
["Compute","Autoscaling","⇅","Ajusta el número de workers/recursos según carga y límites de configuración.","Autoscaling no es lo mismo que auto-termination ni scale-to-zero.","En Fabric el escalado y las capacidades se gestionan de otro modo.","compute"],
["Compute","Photon","ϟ","Motor de ejecución vectorizado que acelera cargas SQL y DataFrame compatibles.","No es un almacenamiento ni reemplaza a Unity Catalog o Delta.","Fabric tiene optimizaciones propias; no equivalencia de motor 1:1.","compute"],

["Rendimiento","Narrow / Wide Transformations","⇢","Narrow: dependencia localizada entre particiones; wide: redistribución entre particiones.","groupBy y algunos joins requieren wide transformations; filter y select suelen ser narrow.","Son conceptos de Apache Spark, aplicables igual en Fabric.","compute"],
["Rendimiento","Shuffle","⇄","Movimiento/redistribución de datos entre particiones y nodos durante una operación.","Wide transformation → suele provocar shuffle; puede implicar red, lectura y escritura.","Igual en el Spark de Fabric.","compute"],
["Rendimiento","Spill","▤","Datos intermedios se escriben en disco cuando la memoria de ejecución es insuficiente.","Spill no es lo mismo que shuffle; shuffle grande puede favorecer spill.","Igual en Spark Fabric; se analiza con Spark UI.","compute"],
["Rendimiento","Broadcast Join","⇉","Difunde una tabla pequeña a los ejecutores para evitar redistribuir la grande en ciertos joins.","Broadcast de la tabla pequeña, no de la tabla masiva.","Exactamente el mismo patrón de PySpark en Fabric.","compute"],
["Rendimiento","Skew","⚖","Reparto desigual de datos entre particiones; pocas tareas tardan mucho más.","Mediana muy inferior al máximo de duración puede ser una pista de skew.","Se diagnostica igual en Spark UI de Fabric.","compute"],
["Rendimiento","Partition Pruning","⊟","Descarta particiones enteras antes de leer archivos si el filtro lo permite.","Filtrar por columna particionada suele reducir bytes leídos.","Mecanismo de lectura también aplicable a tablas particionadas en Fabric.","optimize"],
["Rendimiento","Data Skipping","⇤","Evita leer archivos cuyos metadatos/estadísticas indican que no cumplen el filtro.","Complementa (no reemplaza) partition pruning; depende de estadísticas útiles.","Las tablas Delta de Fabric también pueden aprovechar estadísticas.","optimize"],
["Rendimiento","AQE","◇","Adaptive Query Execution ajusta estrategias de ejecución Spark según estadísticas de runtime.","Puede optimizar joins y particiones, pero no elimina la necesidad de analizar un Spark UI.","También existe en Apache Spark de Fabric.","compute"],

["DevOps","Declarative Automation Bundles","{}","Proyecto versionable que define recursos (jobs, pipelines, etc.) y configuración en YAML.","Antes llamados Databricks Asset Bundles (DAB). Se gestionan con Databricks CLI.","Se aproxima a Git + CI/CD de Fabric, no a un Deployment Pipeline 1:1.","bundle"],
["DevOps","databricks.yml","▧","Archivo principal obligatorio situado en la raíz de un bundle; puede incluir otros YAML.","Solo un databricks.yml raíz; recursos y targets pueden dividirse en archivos incluidos.","Fabric usa definiciones Git de artefactos y parámetros por entorno.","bundle"],
["DevOps","Targets DEV / STAGE / PROD","♙","Destinos con workspaces, catálogos, variables y políticas de despliegue propias.","Un target configura el entorno; una rama Git por sí sola no aísla datos.","Muy similar a separar workspaces/capacidades y configuración por entorno en Fabric.","deploy"],
["DevOps","CLI: validate / deploy / run","⌲","validate comprueba bundle; deploy publica recursos; run ejecuta un recurso desplegado.","deploy no implica ejecutar; -t stage selecciona el target stage.","En Fabric separarías publicar artefactos y lanzar una ejecución.","cli"],
["DevOps","Tests unit / integration / system","✓","Unit valida funciones aisladas; integration valida interacción entre componentes; system comprueba end-to-end.","assertDataFrameEqual es útil en unit tests de DataFrames; no sustituye integración.","Aplicable del mismo modo a proyectos PySpark de Fabric.","bundle"],
["DevOps","Continuous Delivery / Deployment","↗","Delivery deja un cambio listo para producción; Deployment publica automáticamente lo que supera las validaciones.","La distinción de examen es la aprobación manual final (típica de delivery).","Mismo concepto en GitHub Actions / Azure DevOps para Fabric.","deploy"]
];
const GUIDE_PAIRS=[
["Streaming Table","Continuous mode","Streaming Table describe cómo se procesa el dataset. Triggered/Continuous describe cuándo y durante cuánto tiempo funciona el pipeline."],
["Lakeflow Job","Spark Declarative Pipeline","El Job orquesta tareas y horarios. El pipeline declarativo define tablas, vistas, flujos y dependencias; el propio motor construye el DAG."],
["Auto Loader","COPY INTO","Ambos ingieren archivos incrementalmente. Auto Loader se apoya en Structured Streaming; COPY INTO es un comando SQL de carga idempotente por lotes."],
["Serverless","Job Cluster clásico","Serverless lo aprovisiona y escala Databricks. En classic jobs configuras el compute y asumes arranque/operación de clusters."],
["Pool","Cluster","Pool = conjunto de instancias preparadas. Cluster = recurso de compute que ejecuta Spark. El pool, por sí solo, no ejecuta consultas."],
["OPTIMIZE","VACUUM","OPTIMIZE reorganiza archivos para rendimiento. VACUUM retira archivos obsoletos; puede afectar al alcance del Time Travel."],
["CDF","CDC de origen","CDF emite cambios de una tabla Delta habilitada. CDC de un sistema transaccional detecta sus cambios de origen y los replica."],
["View","Materialized View","La view se evalúa al consultarla; una materialized view conserva el resultado persistido y se refresca."],
["Shuffle","Spill","Shuffle = redistribución entre particiones. Spill = escritura temporal a disco por presión de memoria."],
["FAIL UPDATE","DROP ROW","FAIL detiene y revierte la actualización afectada. DROP descarta únicamente las filas inválidas del destino."]
];
function guideElt(tag,className,txt){const x=document.createElement(tag);if(className)x.className=className;if(txt!==undefined)x.textContent=String(txt);return x}
function guideLink(label,href){const a=guideElt("a","guide-doc-link",label+" ↗");a.href=href;a.target="_blank";a.rel="noopener noreferrer";return a}
function renderGuide(root){
 const state=window.__guideUiState||(window.__guideUiState={cat:"Todas",query:""});
 const panel=guideElt("section","guide-page");
 const header=guideElt("div","guide-heading");
 header.append(guideElt("span","guide-kicker","DATABRICKS • REPASO DE CERTIFICACIÓN"));
 header.append(guideElt("h1","guide-title","Mapa de conceptos"));
 header.append(guideElt("p","guide-subtitle","Un diccionario visual para identificar qué hace cada pieza, reconocer las trampas del examen y compararla con Microsoft Fabric."));
 panel.append(header);
 const hero=guideElt("section","guide-hero");
 const heroText=guideElt("div","guide-hero-text");
 heroText.append(guideElt("span","guide-hero-eyebrow","ANTES DE RESPONDER"));
 heroText.append(guideElt("h2","","Distingue el dato, el proceso y el compute."));
 heroText.append(guideElt("p","","Datos: Delta y Unity Catalog · Proceso: Auto Loader, SDP y Jobs · Compute: classic, serverless y pools."));
 hero.append(heroText);
 const formula=guideElt("div","guide-flow");
 [["Origen","Auto Loader / Connect"],["Transformar","Spark Declarative Pipelines"],["Orquestar","Lakeflow Jobs"],["Almacenar","Delta + Unity Catalog"]].forEach((pair,i)=>{
   if(i)formula.append(guideElt("span","guide-arrow","→"));
   const node=guideElt("div","guide-node");node.append(guideElt("span","",pair[0]),guideElt("strong","",pair[1]));formula.append(node);
 });
 hero.append(formula);panel.append(hero);
 const pairBox=guideElt("section","guide-compare-box");
 const cmpHead=guideElt("div","guide-section-heading");cmpHead.append(guideElt("div","", "⚡ No confundas estos conceptos"));
 const notes=guideElt("span","guide-help","Toca una comparación para ver la diferencia");
 cmpHead.append(notes);pairBox.append(cmpHead);
 const pairGrid=guideElt("div","guide-pairs");
 GUIDE_PAIRS.forEach(([a,b,why])=>{
  const details=guideElt("details","guide-pair");
  const summary=guideElt("summary","");summary.append(guideElt("strong","",a),guideElt("span","guide-vs","≠"),guideElt("strong","",b));
  details.append(summary,guideElt("p","",why));pairGrid.append(details);
 });
 pairBox.append(pairGrid);panel.append(pairBox);
 const glossary=guideElt("section","guide-glossary");
 const title=guideElt("div","guide-section-heading");title.append(guideElt("h2","","Diccionario de términos"));
 const count=guideElt("span","guide-count",String(GUIDE_RAW.length)+" conceptos");title.append(count);glossary.append(title);
 const filterBox=guideElt("div","guide-filter");
 const input=guideElt("input","guide-search");input.type="search";input.autocomplete="off";input.placeholder="Buscar término: serverless, streaming, Delta…";input.value=state.query;input.setAttribute("aria-label","Buscar en el diccionario");
 filterBox.append(input);
 const tabs=guideElt("div","guide-tabs");tabs.setAttribute("aria-label","Filtrar por categoría");
 const allTabs=["Todas",...GUIDE_CATS];
 const chipButtons=[];
 allTabs.forEach(cat=>{const chip=guideElt("button","guide-chip",cat);chip.type="button";chip.setAttribute("aria-pressed",cat===state.cat?"true":"false");chip.onclick=()=>{state.cat=cat;renderCards()};chipButtons.push([cat,chip]);tabs.append(chip)});
 filterBox.append(tabs);glossary.append(filterBox);
 const empty=guideElt("p","guide-empty");
 const cards=guideElt("div","guide-grid");glossary.append(empty,cards);panel.append(glossary);
 panel.append(guideElt("p","guide-bottom-note","Guía complementaria y no oficial. Las claves de examen son reglas prácticas, no sustituyen la documentación. Algunas preguntas de bancos antiguos usan nombres anteriores o simplificaciones."));
 input.addEventListener("input",()=>{state.query=input.value;renderCards()});
 function renderCards(){
   cards.replaceChildren();
   chipButtons.forEach(([cat,chip])=>chip.setAttribute("aria-pressed",cat===state.cat?"true":"false"));
   const q=state.query.trim().toLocaleLowerCase("es");
   const filtered=GUIDE_RAW.filter(row=>(state.cat==="Todas"||row[0]===state.cat)&&(!q||[row[1],row[3],row[4],row[5]].join(" ").toLocaleLowerCase("es").includes(q)));
   count.textContent=filtered.length+" de "+GUIDE_RAW.length+" conceptos";
   empty.textContent=filtered.length?"":"No se encontraron términos. Prueba otra palabra o selecciona «Todas».";
   for(const row of filtered){
     const [cat,term,symbol,summary,tip,fabric,doc]=row;
     const card=guideElt("article","guide-card");
     card.dataset.category=GUIDE_CATS.indexOf(cat);
     const top=guideElt("div","guide-card-top");
     const icon=guideElt("div","guide-card-icon",symbol);icon.setAttribute("aria-hidden","true");
     const ttl=guideElt("div","guide-term");ttl.append(guideElt("span","guide-category",cat),guideElt("h3","",term));
     top.append(icon,ttl);card.append(top);
     card.append(guideElt("p","guide-definition",summary));
     const trap=guideElt("div","guide-exam-tip");trap.append(guideElt("span","guide-exam-title","CLAVE DEL EXAMEN"),guideElt("p","",tip));card.append(trap);
     const more=guideElt("details","guide-fabric");more.append(guideElt("summary","","↔ Comparar con Fabric"),guideElt("p","",fabric));card.append(more);
     if(GUIDE_DOCS[doc])card.append(guideLink("Documentación oficial",GUIDE_DOCS[doc]));
     cards.append(card);
   }
 }
 renderCards();root.append(panel);
}
