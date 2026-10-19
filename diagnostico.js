/* Diagnóstico didáctico 2026. No recopila datos en servidor.
   Cada pregunta se asigna a un concepto primario; los errores describen
   qué pregunta se falló, no infieren el estado mental del estudiante. */
const DIAG_GROUPS=[
 {id:"joins",name:"JOIN, NULL y condiciones SQL",area:"JOIN / SQL",ids:[1,8,10,18,39,75,174,217,222,258,278,279],
 why:"Un JOIN combina registros según claves; INNER conserva coincidencias, LEFT conserva todo el lado izquierdo y ANTI devuelve filas sin coincidencia. NULL representa ausencia de valor, no cero ni texto vacío. Con =, NULL = NULL resulta UNKNOWN; en Databricks SQL, <=> devuelve TRUE si ambos son NULL.",
 contrast:"LEFT JOIN ≠ ANTI JOIN; = ≠ <=>; IS NULL ≠ = NULL. COALESCE(columna, '') puede equiparar por error un NULL y una cadena vacía.",
 exam:"Si el enunciado necesita tratar dos NULL como iguales, piensa en <=>. Si busca clientes sin pedidos, piensa en LEFT ANTI JOIN.",
 example:"WHERE customer_id <=> :cid considera NULL = NULL verdadero; WHERE customer_id = :cid no."},
 {id:"sqlapi",name:"SQL, PySpark y sintaxis de transformaciones",area:"JOIN / SQL",ids:[12,21,28,48,53,56,57,62,66,99,100,108,110,111,134,145,147,150,229,236,240,250,254,256],
 why:"Spark DataFrames usan APIs como df.groupBy(...).agg(...); Spark SQL recibe sentencias completas con spark.sql('SELECT ...'). Las funciones y operadores se eligen según si debes filtrar filas, agrupar, añadir columnas, pivotar o insertar.",
 contrast:"spark.table('sales') lee un nombre de tabla; spark.sql('sales') NO es una consulta válida. WHERE filtra, GROUP BY agrega, PIVOT convierte categorías en columnas y MERGE ejecuta upserts.",
 exam:"Identifica primero si el enunciado quiere leer, transformar o escribir. Descarta métodos inexistentes y ordenaciones imposibles como sum().groupBy().",
 example:"df.groupBy('region').agg(sum('sales_amount')) agrupa por región; agrupar por category no responde a esa métrica."},
 {id:"deltacore",name:"Delta Lake: ACID, log y versiones",area:"Delta Lake",ids:[4,5,6,9,23,52,59,70,91,98,140,203,212,248,280],
 why:"Delta Lake añade un registro transaccional (_delta_log) a archivos de datos Parquet. ACID evita confirmaciones parciales y Time Travel permite consultar versiones retenidas de una tabla.",
 contrast:"Delta Lake es tecnología de tabla, no Delta Live Tables/Lakeflow. Time Travel consulta versiones; VACUUM elimina archivos antiguos y puede limitar la consulta histórica.",
 exam:"Si se menciona fiabilidad de escrituras: ACID. Si se mencionan VERSION AS OF o TIMESTAMP AS OF: Time Travel. Si se mencionan archivos físicos: Parquet + _delta_log.",
 example:"SELECT * FROM tabla VERSION AS OF 4 consulta una versión retenida sin restaurarla como versión actual."},
 {id:"deltamerge",name:"MERGE, tablas administradas y mantenimiento Delta",area:"Delta Lake",ids:[16,20,24,51,58,64,104,107,155,170,187,188,211,219,233,251,257],
 why:"MERGE compara origen y destino por una condición y ejecuta acciones MATCHED/NOT MATCHED. OPTIMIZE reordena/compacta archivos; VACUUM borra archivos obsoletos. Las tablas managed y external difieren en quién gestiona el ciclo de vida de los datos.",
 contrast:"INSERT/APPEND agrega, MERGE permite actualizar e insertar; OPTIMIZE ≠ VACUUM; Liquid Clustering organiza datos por claves y no es un simple ZORDER manual.",
 exam:"Problemas de upsert → MERGE. Muchos ficheros pequeños → OPTIMIZE. Borrado de archivos históricos → VACUUM. Layout moderno → Liquid Clustering / Predictive Optimization.",
 example:"MERGE INTO destino d USING origen o ON d.id=o.id WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ..."},
 {id:"autoloader",name:"Auto Loader, COPY INTO y esquema",area:"Ingesta / Streaming",ids:[7,17,19,71,76,103,122,123,133,158,168,179,221,232,234,255,265,270],
 why:"Auto Loader descubre archivos nuevos en almacenamiento usando Structured Streaming y estado de progreso; COPY INTO carga archivos con SQL y evita volver a cargar los ya registrados. Ambos sirven para ingesta incremental de archivos, con interfaces distintas.",
 contrast:"Auto Loader usa cloudFiles/Structured Streaming; COPY INTO es SQL; CTAS es creación/carga ad hoc. _rescued_data contiene datos incompatibles o inesperados, schemaHints no exige que el campo esté físicamente en todos los ficheros.",
 exam:"«Llegan archivos nuevos continuamente» → Auto Loader; «sentencia SQL que omite ficheros ya cargados» → COPY INTO; «carga inicial puntual» → CTAS.",
 example:"spark.readStream.format('cloudFiles').option('cloudFiles.format','json').load(path) registra progreso mediante checkpoint."},
 {id:"stream",name:"Structured Streaming y checkpoints",area:"Ingesta / Streaming",ids:[30,31,67,78,79,118,125,144,151,152,227,235,246],
 why:"Structured Streaming trata novedades incrementalmente usando readStream/writeStream, checkpoints y triggers. AvailableNow procesa el backlog y termina; un pipeline Continuous permanece activo. Streaming Table es un tipo de dataset, no un horario de ejecución.",
 contrast:"read vs readStream; trigger AvailableNow vs Continuous; checkpoint de offsets/estado ≠ _delta_log. El destino debe admitir el modo de salida correspondiente a la operación.",
 exam:"«Reanudar tras fallo sin perder progreso» → checkpoint. «Procesar lo disponible y detenerse» → AvailableNow. «Kafka» → fuente kafka, no cloudFiles.",
 example:"spark.readStream.table('bronze').writeStream.option('checkpointLocation', path).toTable('silver')"},
 {id:"connectors",name:"CDC, Lakeflow Connect e ingesta empresarial",area:"Ingesta / Streaming",ids:[238,247,263],
 why:"La ingesta empresarial puede extraer incrementos de SaaS, bases de datos o eventos. CDC refleja INSERT/UPDATE/DELETE en el origen; un full refresh relee todo y no equivale a detectar cambios.",
 contrast:"Delta CDF expone cambios de una tabla Delta ya habilitada; CDC desde un origen captura cambios de ese sistema; Lakeflow Connect gestiona conectores, Jobs orquesta cuándo se ejecutan.",
 exam:"Si necesitas replicar solo cambios, comprueba que el conector permita CDC, preserve deletes y tenga el intervalo de actualización adecuado.",
 example:"Full load de Snowflake cada hora no equivale a CDC cada cuatro horas si se exige transferir únicamente cambios."},
 {id:"medallion",name:"Medallion: Bronze, Silver y Gold",area:"Lakeflow / Medallion",ids:[29,50,54,73,81,119,141,159,202,204,245,273,154],
 why:"Bronze preserva datos lo más fieles al origen; Silver valida, deduplica, normaliza e integra; Gold presenta datasets de negocio, métricas y agregaciones para consumo.",
 contrast:"No es obligatorio que Silver tenga menos filas que Bronze: explotar arrays o normalizar puede aumentarlas. Gold no significa necesariamente tablas grandes; su objetivo es el consumo empresarial.",
 exam:"Fuente raw → Bronze. Limpieza y conformidad → Silver. KPIs por región o modelos de negocio → Gold.",
 example:"Bronze.events_raw → Silver.events_clean → Gold.daily_sales_by_region."},
 {id:"declarative",name:"Lakeflow SDP, DLT y tipos de tabla",area:"Lakeflow / Medallion",ids:[33,34,36,42,65,72,121,146,160,213],
 why:"Spark Declarative Pipelines define datasets y transformaciones en SQL/Python; la plataforma determina dependencias. Streaming Table procesa incrementos y Materialized View materializa una consulta y se refresca según cambios disponibles.",
 contrast:"DLT es el nombre anterior de esta tecnología; Spark Declarative Pipelines NO es el orquestador Lakeflow Jobs. Una Streaming Table puede ejecutarse en un pipeline Triggered o Continuous.",
 exam:"Si define tablas y flujos declarativos → SDP. Si pregunta cuándo se ejecutan notebooks/tareas → Jobs. Streaming Table ≠ Continuous.",
 example:"CREATE OR REFRESH STREAMING TABLE silver AS SELECT * FROM STREAM(bronze);"},
 {id:"expectations",name:"Expectations y calidad de datos",area:"Lakeflow / Medallion",ids:[25,32,63,80,138,225],
 why:"Las Expectations permiten evaluar reglas de calidad en actualizaciones declarativas. WARN registra incumplimientos y conserva filas; DROP ROW elimina inválidos del resultado; FAIL UPDATE hace fallar la actualización afectada.",
 contrast:"DROP ROW no crea automáticamente una tabla de cuarentena. FAIL UPDATE no significa descartar solamente una fila. SQL no compara correctamente NULL con != NULL: utiliza IS NOT NULL.",
 exam:"«Registrar sin bloquear» → WARN; «descartar inválidos» → DROP ROW; «fallar el update» → FAIL UPDATE.",
 example:"CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE"},
 {id:"jobs",name:"Lakeflow Jobs, dependencias y reintentos",area:"Jobs / DevOps",ids:[27,37,41,55,84,87,129,131,135,172,218,223,239,241,252,253,261,264,274,276],
 why:"Un Job orquesta tareas y programa ejecuciones. Si B depende de A, A debe terminar antes; Run if controla condiciones de éxito/fallo. Repair run permite reejecutar selectivamente tareas fallidas u omitidas en un run cuando procede.",
 contrast:"Depends on define precedencia; Run if define condición de ejecución; Job Cluster aporta compute; SDP define datasets, no el horario del Job.",
 exam:"Si una corrección debe ejecutarse antes del notebook original, configura el original para depender de la tarea correctiva. No inviertas las flechas del DAG.",
 example:"Tarea limpiar → tarea reporte. En reporte: Depends on limpiar y Run if = All succeeded."},
 {id:"bundles",name:"Bundles, Git y despliegues",area:"Jobs / DevOps",ids:[35,46,47,94,95,136,137,189,190,208,214,228,242,244,249,262,271,277],
 why:"Declarative Automation Bundles (antes Asset Bundles) empaqueta jobs, pipelines y configuración en YAML. databricks.yml identifica el bundle; targets definen settings DEV/STAGE/PROD. Deploy publica; Run ejecuta.",
 contrast:"Git folders permite versionar código; un bundle versiona también recursos de despliegue. Continuous delivery normalmente conserva aprobación manual final; continuous deployment no.",
 exam:"databricks bundle deploy -t stage publica hacia ese target. Las variables --var sobrescriben valores de menor prioridad.",
 example:"targets: production: mode: production; usa variables y workspace diferentes, no solo ramas con nombres diferentes."},
 {id:"compute",name:"Job Clusters, Pools, Spot y Serverless",area:"Compute / Spark",ids:[3,15,43,49,77,92,148,157,164,166,173,180,191,192,193,207,259,275],
 why:"Job Cluster ejecuta tareas y suele ser efímero; All-Purpose se orienta a desarrollo interactivo. Instance Pool prepara instancias para acelerar arranque de clusters clásicos, pero no ejecuta código. Serverless gestiona el aprovisionamiento. Spot reduce precio de VM pero puede interrumpirse.",
 contrast:"Pool ≠ cluster; Spot ≠ autoscaling; Photon optimiza ejecución y no es un descuento de VMs. Un cluster clásico sin pool puede arrancar lento; serverless tiene límites de compatibilidad.",
 exam:"Primero mira si el workload es compatible con Serverless. Si pide explícitamente cluster clásico, distingue arranque (pool), coste (Spot), capacidad (autoscaling) y uso (Jobs frente a interactivo).",
 example:"Un Job batch tolerante a interrupciones puede usar Spot; un SLA estricto quizá requiera On-Demand aunque cueste más."},
 {id:"warehouse",name:"SQL Warehouse: Pro, Serverless y escalado",area:"Compute / Spark",ids:[40,82,88,130,153,167,201,220],
 why:"SQL Warehouse sirve consultas SQL y BI. Serverless acelera el aprovisionamiento en entornos compatibles; Pro suele elegirse si hace falta networking personalizado en escenarios que no dispone de conexión serverless. Auto Stop apaga compute inactivo; escala máxima atiende concurrencia.",
 contrast:"Warehouse SQL ≠ All-Purpose Spark cluster; Auto Stop ahorra tiempo encendido, no acelera un arranque frío; escalado de clusters concurrentes ≠ tamaño de cluster.",
 exam:"Problema de inicio tras Auto Stop → considera Serverless; muchas consultas en cola → analiza escalado de concurrencia; red personalizada no soportada por serverless → Pro.",
 example:"Bajar el tamaño del SQL Warehouse no evita que esté encendido toda la noche; Auto Stop sí reduce tiempo ocioso."},
 {id:"spark",name:"Spark: shuffle, spill y optimización",area:"Compute / Spark",ids:[2,139,177,178,181,182,224,237,243,266,267,268,269,272],
 why:"Wide transformations como groupBy y algunos joins provocan redistribución (shuffle). Si falta memoria para resultados intermedios, Spark puede escribirlos a disco (spill). Broadcast Join difunde una tabla pequeña y puede evitar un shuffle de la grande.",
 contrast:"Shuffle = movimiento de datos; spill = memoria insuficiente y escrituras temporales. Skew = distribución desigual de claves; aumentar memoria no lo arregla necesariamente.",
 exam:"Si ves stages muy descompensados → skew. Join con una tabla pequeña → broadcast. df.toPandas() lleva datos al driver, con riesgo de OOM.",
 example:"events.join(broadcast(dim), 'id') puede evitar redistribuir eventos masivos si dim cabe en memoria."},
 {id:"connect",name:"Databricks Connect y desarrollo local",area:"Compute / Spark",ids:[149,200,205,215],
 why:"Databricks Connect permite programar y depurar en un IDE local mientras operaciones Spark se ejecutan en compute Databricks. No significa que el portátil se convierta en cluster ni que funcione sin conexión.",
 contrast:"Git folders gestiona repositorios, Connect conecta IDE y compute, Bundles publica recursos. Son herramientas de fases distintas.",
 exam:"«IDE local que ejecuta PySpark con compute Databricks» → Databricks Connect, no un SQL Warehouse ni un pool.",
 example:"VS Code con Databricks Connect ejecuta transformaciones remotas; debe existir compatibilidad de versiones/entorno."},
 {id:"uc",name:"Unity Catalog: permisos, owners y seguridad",area:"Gobierno / Sharing",ids:[11,13,14,22,44,45,61,74,83,89,96,156,161,163,165,176,195,197,210,216,226,231,260],
 why:"Unity Catalog gobierna catálogos, esquemas, tablas, vistas y permisos. Para consultar una tabla normalmente necesitas USE CATALOG, USE SCHEMA y SELECT, además de condiciones aplicables. OWNERSHIP, MANAGE y ALL PRIVILEGES no son equivalentes.",
 contrast:"Conceder SELECT a una vista no otorga acceso directo a todas las fuentes; vistas UC pueden evaluarse según derechos del owner. External Location/Storage Credential gobiernan acceso a almacenamiento, no son tablas.",
 exam:"Problemas de grants: verifica objeto (catalog/schema/table), privilegio y principal. Transferir propiedad y otorgar ALL PRIVILEGES son operaciones distintas.",
 example:"GRANT USE SCHEMA ON SCHEMA main.sales TO analysts; no reemplaza SELECT ON TABLE main.sales.orders."},
 {id:"sharing",name:"Delta Sharing, federación y lineage",area:"Gobierno / Sharing",ids:[132,142,143,169,171,184,194,198,199],
 why:"Compartir datos con destinatarios externos es diferente de conceder permisos internos. Delta Sharing/OpenSharing gobierna publicaciones hacia recipients. Federation consulta fuentes externas sin copiarlas; lineage muestra el origen y las dependencias del dato.",
 contrast:"Share no despliega Jobs ni notebooks como tareas; git no comparte tablas gobernadas. Compartir entre regiones o nubes puede generar egress; no presupongas que es gratuito.",
 exam:"«Acceso a un dataset desde una organización externa» → sharing; «consultar BD en origen sin copiar» → federation; «de dónde proviene la columna» → lineage.",
 example:"SELECT sobre una vista UC interna no concede automáticamente lectura a un destinatario fuera del metastore."},
 {id:"quality",name:"Pruebas, monitorización y troubleshooting",area:"Jobs / DevOps",ids:[136,243],
 why:"Unit tests verifican funciones aisladas, integración verifica el intercambio entre componentes y system/end-to-end comprueba el flujo completo. Los logs, event logs y Spark UI permiten diagnosticar fallos de datos y rendimiento.",
 contrast:"Un test de una función no detecta necesariamente un join Bronze → Silver que pierde filas. Un notebook sin errores no implica que el pipeline entero conserve integridad referencial.",
 exam:"«join que pierde registros entre capas» → integration test; «assertDataFrameEqual de una función» → unit test.",
 example:"Valida esquema y conteos tras la unión Bronze/Silver con un test de integración."}
];
const DIAG_MAP=new Map();for(const c of DIAG_GROUPS)for(const id of c.ids)if(!DIAG_MAP.has(id))DIAG_MAP.set(id,c.id);
const DIAG_BY_ID=Object.fromEntries(DIAG_GROUPS.map(c=>[c.id,c]));
const DIAG_CATEGORIES=["JOIN / SQL","Compute / Spark","Delta Lake","Ingesta / Streaming","Gobierno / Sharing","Lakeflow / Medallion","Jobs / DevOps","Fundamentos / Otros"];
const DIAG_FALLBACK={
"JOIN / SQL":"sqlapi","Compute / Spark":"compute","Delta Lake":"deltacore",
"Ingesta / Streaming":"autoloader","Gobierno / Sharing":"uc",
"Lakeflow / Medallion":"declarative","Jobs / DevOps":"jobs",
"Fundamentos / Otros":"medallion"
};
function diagConcept(q){return DIAG_BY_ID[DIAG_MAP.get(q.id)||DIAG_FALLBACK[q.category]||"sqlapi"]}
function diagSessionAvailable(){return !!(app.session&&app.session.finished&&Array.isArray(app.session.ids)&&app.session.ids.some(id=>app.session.checks?.[id]==="wrong"||app.session.checks?.[id]==="correct"))}
function diagPoints(scope){
 const current=typeof CURRENT_QUESTIONS!=="undefined"?CURRENT_QUESTIONS:QUESTIONS.filter(q=>q.bank26State!=="archive");
 const s=app.session,session=scope==="session"&&diagSessionAvailable();
 const items=[];
 for(const q of current){
   let tries=0,wrong=0,correct=0,selected=null;
   if(session){
     const check=s.checks?.[q.id];if(!s.ids.includes(q.id)||!["correct","wrong"].includes(check))continue;
     tries=1;wrong=check==="wrong"?1:0;correct=tries-wrong;
     if(wrong&&Array.isArray(s.answers?.[q.id]))selected=s.answers[q.id];
   }else{
     const h=app.history?.[q.id];tries=Math.max(0,Number(h?.attempts)||0);
     wrong=Math.max(0,Math.min(tries,Number(h?.wrong)||0));correct=tries-wrong;
     if(!tries)continue;
     const past=(app.answerEvents||[]).filter(x=>x.id===q.id);
     if(past.length)selected=past[past.length-1].selected;
   }
   items.push({q,tries,wrong,correct,selected,concept:diagConcept(q)});
 }
 const buckets=new Map();
 for(const c of DIAG_GROUPS)buckets.set(c.id,{...c,tries:0,wrong:0,correct:0,questions:0,failed:0,items:[]});
 for(const row of items){
   const b=buckets.get(row.concept.id);if(!b)continue;
   b.tries+=row.tries;b.wrong+=row.wrong;b.correct+=row.correct;b.questions++;
   if(row.wrong)b.failed++;b.items.push(row);
 }
 const sorted=[...buckets.values()].filter(x=>x.tries>0);
 sorted.forEach(b=>{b.rate=Math.round(100*b.wrong/b.tries);b.priority=b.wrong*2+b.rate/15;b.lowSample=b.questions<3});
 sorted.sort((a,b)=>b.wrong-a.wrong||b.rate-a.rate||b.questions-a.questions);
 const totals={tries:items.reduce((n,r)=>n+r.tries,0),wrong:items.reduce((n,r)=>n+r.wrong,0),correct:items.reduce((n,r)=>n+r.correct,0),questions:items.length};
 const areas=DIAG_CATEGORIES.map(area=>{
   const list=items.filter(x=>x.q.category===area);
   return {area,tries:list.reduce((n,x)=>n+x.tries,0),wrong:list.reduce((n,x)=>n+x.wrong,0),questions:list.length};
 }).filter(x=>x.tries>0).map(x=>({...x,pct:Math.round(100*x.wrong/x.tries)})).sort((a,b)=>b.wrong-a.wrong||b.pct-a.pct);
 return {buckets:sorted,totals,areas,scope:session?"session":"history"};
}
function diagAdd(tag,cl,str){const x=document.createElement(tag);if(cl)x.className=cl;if(str!==undefined)x.textContent=String(str);return x}
function diagMetric(label,val){const box=diagAdd("div","diag-metric");box.append(diagAdd("strong","",val),diagAdd("span","",label));return box}
function diagBar(pct){const track=diagAdd("div","diag-track"),fill=diagAdd("span","diag-fill");fill.style.width=Math.max(0,Math.min(100,pct))+"%";track.append(fill);return track}
function diagBtn(label,callback,cls="btn"){const b=diagAdd("button",cls,label);b.type="button";b.addEventListener("click",callback);return b}
function diagStartFocus(b,scope){
 const rows=b.items.filter(row=>row.wrong>0);
 const ids=rows.map(x=>x.q.id);
 if(!ids.length)return;
 if(app.session&&!app.session.finished&&!confirm("Hay una sesión sin terminar. ¿Quieres sustituirla por este repaso focalizado?"))return;
 app.session={ids,mode:"practice",kind:"concept-"+b.id,pos:0,answers:{},checks:{},finished:false,startedAt:Date.now(),durationMs:null,historyRecorded:false};
 explainOpen=false;whyOpen=false;save();go("practice");
}
function diagRender(parent){
 const root=diagAdd("section","diag-page");
 root.append(heading("Tu diagnóstico de aprendizaje","Identifica qué materias y conceptos te están costando más, entiende la diferencia técnica y practica exactamente las preguntas que has fallado.","ANÁLISIS PERSONALIZADO"));
 const tabs=diagAdd("div","diag-switch");
 const recent=diagSessionAvailable();
 const state=window.__diagState||(window.__diagState={scope:recent?"session":"history",expanded:null});
 if(!recent)state.scope="history";
 for(const [value,label] of [["session","Última sesión"],["history","Todo el historial"]]){
   const b=diagBtn(label,()=>{state.scope=value;diagRender(parent)},"diag-tab");
   b.disabled=value==="session"&&!recent;b.setAttribute("aria-pressed",String(state.scope===value));
   tabs.append(b);
 }root.append(tabs);
 const data=diagPoints(state.scope);
 if(!data.totals.tries){
   const empty=diagAdd("div","panel padded");empty.append(diagAdd("h2","minititle","Todavía no hay respuestas evaluadas"),diagAdd("p","small muted","Realiza unas preguntas y vuelve aquí. El análisis aparece automáticamente con tus respuestas guardadas."));
   root.append(empty);parent.replaceChildren(root);return;
 }
 const metrics=diagAdd("div","diag-metrics");
 metrics.append(diagMetric("Preguntas analizadas",data.totals.questions),diagMetric("Fallos",data.totals.wrong),diagMetric("Tasa de error",Math.round(data.totals.wrong/data.totals.tries*100)+"%"));
 root.append(metrics);
 const annotation=diagAdd("p","diag-method",
 data.scope==="session"?
 "Última sesión: una respuesta por pregunta, usando el resultado y las opciones que marcaste. Los fallos de diferentes conceptos no se suman dos veces.":
 "Histórico: se cuentan todos los intentos por pregunta vigente. La opción seleccionada solo está disponible en la última sesión o en intentos registrados desde esta versión. Los conceptos se asignan por la temática de cada pregunta.");
 root.append(annotation);
 const areaPanel=diagAdd("section","panel padded diag-area-panel");
 areaPanel.append(diagAdd("h2","minititle","1. ¿En qué materias fallas más?"));
 areaPanel.append(diagAdd("p","small muted","Ordenadas por cantidad de fallos. El porcentaje permite distinguir una materia amplia de otra con pocas preguntas."));
 for(const area of data.areas){
   const line=diagAdd("div","diag-area-row");
   const upper=diagAdd("div","diag-area-head");upper.append(diagAdd("strong","",area.area),diagAdd("span","",area.wrong+" fallos de "+area.tries+" intentos · "+area.pct+"%"));
   line.append(upper,diagBar(area.pct));areaPanel.append(line);
 }
 root.append(areaPanel);
 const section=diagAdd("section","diag-concepts");
 section.append(diagAdd("h2","diag-heading","2. Tus conceptos prioritarios"));
 section.append(diagAdd("p","diag-sub","Ordenados por errores acumulados, no solo por porcentaje. Abre cada uno para entenderlo, ver tus fallos y practicarlo."));
 const weak=data.buckets.filter(b=>b.wrong>0);
 if(!weak.length)section.append(diagAdd("p","diag-empty","No se han detectado errores en las preguntas evaluadas. Puedes revisar conceptos en la pestaña Guía."));
 for(const [idx,b] of weak.entries()){
   const card=diagAdd("details","diag-concept-card");
   const isOpen=state.expanded===b.id;card.open=isOpen;
   const summary=diagAdd("summary","");
   const top=diagAdd("div","diag-concept-title");
   top.append(diagAdd("span","diag-rank",String(idx+1)),diagAdd("strong","",b.name));
   const scores=diagAdd("div","diag-scores");
   scores.append(diagAdd("span","diag-error-count",b.wrong+" fallos"),diagAdd("span","diag-rate",b.rate+"% error"));
   summary.append(top,scores);card.append(summary);
   const inside=diagAdd("div","diag-details");
   const intro=diagAdd("p","diag-inference","Se han fallado "+b.failed+" de "+b.questions+" preguntas distintas de este concepto"+(data.scope==="history"?" ("+b.wrong+" fallos acumulados en "+b.tries+" intentos).":" en la última sesión."));
   inside.append(intro);
   if(b.lowSample)inside.append(diagAdd("p","diag-caution","Muestra pequeña: solo "+b.questions+" pregunta(s). Prioridad provisional; conviene practicar más antes de concluir que es una debilidad estable."));
   for(const [label,text] of [["QUÉ SIGNIFICA Y CÓMO FUNCIONA",b.why],["NO CONFUNDAS",b.contrast],["LO QUE DEBES RECONOCER EN EL EXAMEN",b.exam]]){
     const block=diagAdd("div","diag-lesson");block.append(diagAdd("strong","",label),diagAdd("p","",text));inside.append(block);
   }
   const example=diagAdd("div","diag-example");
   example.append(diagAdd("strong","","Ejemplo que conviene memorizar"),diagAdd("p","",b.example));inside.append(example);
   const wrongs=b.items.filter(x=>x.wrong>0).sort((x,y)=>y.wrong-x.wrong||x.q.id-y.q.id);
   const list=diagAdd("details","diag-list");
   list.append(diagAdd("summary","","Ver mis preguntas falladas ("+wrongs.length+")"));
   const items=diagAdd("div","diag-questions");
   for(const row of wrongs){
     const q=row.q,part=diagAdd("div","diag-question-entry");
     const hdr=diagAdd("div","diag-question-header");
     hdr.append(diagAdd("strong","","Original #"+q.id),diagAdd("span","",data.scope==="history"?row.wrong+" fallo(s) · "+row.tries+" intento(s)":"Fallada en última sesión"));
     part.append(hdr,diagAdd("p","",q.question.replace(/\s+/g," ").slice(0,210)+(q.question.length>210?"…":"")));
     if(row.selected?.length){
       const chose=q.options.filter(o=>row.selected.includes(o.key));const picked=diagAdd("div","diag-picked");
       for(const o of chose){
         picked.append(diagAdd("strong","","Elegiste "+o.key+": "+o.text.replace(/\s+/g," ").slice(0,170)));
         if(!q.correct.includes(o.key))picked.append(diagAdd("p","",q.wrongWhy?.[o.key]||"Esta alternativa no cumple el requisito del enunciado."));
       }part.append(picked);
     }
     part.append(diagAdd("p","diag-answer","Correcta: "+q.correct.join(", ")+" · "+(q.answerWhy||q.explanation||"Consulta su explicación dentro del simulador.").slice(0,310)));
     items.append(part);
   }
   list.append(items);inside.append(list);
   const actions=diagAdd("div","diag-actions");
   actions.append(diagBtn("Practicar mis "+wrongs.length+" pregunta(s) fallada(s) →",()=>diagStartFocus(b,state.scope),"btn primary"));
   actions.append(diagBtn("Abrir guía de términos",()=>go("guide"),"btn"));
   inside.append(actions);card.append(inside);
   card.addEventListener("toggle",()=>{if(card.open)state.expanded=b.id;else if(state.expanded===b.id)state.expanded=null});
   section.append(card);
 }
 root.append(section);
 const method=diagAdd("div","diag-method-box");
 method.append(diagAdd("strong","","Cómo interpretar el diagnóstico"));
 method.append(diagAdd("p","","Cada pregunta se atribuye a un concepto principal según su contenido. Los resultados describen dónde has fallado, no demuestran por sí solos la causa exacta. Cuando una pregunta ya se ha acertado tras varios intentos, sus fallos anteriores siguen contando en la vista histórica; la última sesión refleja tu estado reciente."));
 method.append(diagAdd("p","","Se excluyen las 37 descatalogadas de este análisis, pero sus historiales y el resto de tus estadísticas originales permanecen guardados. El botón de practicar crea una sesión nueva solo cuando lo pulsas."));
 root.append(method);
 parent.replaceChildren(root);
}
