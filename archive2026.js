/* Vista de consulta: banco histórico y soluciones en revisión, sin puntuar. */
function archive2026Page(){
 const root=document.querySelector("#root");
 root.append(heading("Preguntas descatalogadas","Conservamos el banco de 280 preguntas intacto y separado por vigencia. Estas preguntas no entran en los nuevos simulacros.","AUDITORÍA DE OCTUBRE DE 2026"));
 const main=el("div","catalog-summary");
 for(const [num,title,desc] of [[238,"Banco vigente","Aparecen en sesiones nuevas"],[37,"Descatalogadas","Duplicadas o defectuosas"],[5,"En revisión","Respuesta sin verificar"]]){
  const card=el("div","catalog-stat");
  card.append(el("strong","",String(num)),el("span","",title),el("small","",desc));main.append(card);
 }
 root.append(main);
 const info=el("div","catalog-info");
 info.append(el("strong","","Tus estadísticas no se pierden."));
 info.append(el("p","","Cada pregunta conserva su número y su historial anterior. Las 37 descatalogadas y las 5 pendientes no cuentan en nuevos simulacros. Puedes abrirlas aquí para consultar su enunciado y las respuestas del PDF, sin modificar tu puntuación."));
 root.append(info);
 const nav=el("div","catalog-tabs"),content=el("div","catalog-content"),search=el("input","catalog-search");
 search.type="search";search.placeholder="Buscar por número o texto…";search.setAttribute("aria-label","Buscar preguntas históricas");
 let view="archive";
 const views=[["archive","Descatalogadas (37)"],["review","Pendientes (5)"]];
 const filters={};
 views.forEach(([key,label])=>{
  const b=btn(label,()=>{view=key;search.value="";render()}, "catalog-tab");filters[key]=b;nav.append(b);
 });
 root.append(nav,search,content);
 function render(){
   content.replaceChildren();
   views.forEach(([key])=>filters[key].setAttribute("aria-pressed",String(view===key)));
   const query=search.value.toLowerCase().trim();
   const qs=QUESTIONS.filter(q=>q.bank26State===view&&(!query||("#"+q.id+" "+q.question+" "+q.bank26Note).toLowerCase().includes(query)));
   if(!qs.length){content.append(el("p","small muted","No hay coincidencias."));return;}
   for(const q of qs){
     const card=el("details","catalog-item");
     const summary=el("summary","");
     summary.append(el("span","catalog-id","#"+q.id));
     const grouping=view==="review"?"Solución pendiente":BANK26_DUP[q.id]?"Pregunta repetida":"Pregunta con defecto";
     const labels=el("div","catalog-item-labels");
     labels.append(el("strong","",grouping),el("span","",q.category||""));
     summary.append(labels,el("span","catalog-chevron","⌄"));card.append(summary);
     const body=el("div","catalog-item-body");
     const note=el("div","catalog-why");note.append(el("strong","",view==="review"?"Por qué se revisa":"Por qué se descatalogó"),el("p","",q.bank26Note));body.append(note);
     const question=el("div","catalog-question");question.append(renderRich(q.bank26Original.question));body.append(question);
     const options=el("div","catalog-option-list");
     for(const o of q.bank26Original.options){
       const line=el("div","catalog-option");line.append(el("b","",o.key+"."));
       const t=el("div","");t.append(renderRich(o.text));line.append(t);options.append(line);
     }body.append(options);
     const foot=el("p","catalog-original-key","Solución del PDF original: "+q.bank26Original.correct.join(", ")+(view==="review"?" · NO VALIDADA":BANK26_DUP[q.id]?" · Pregunta duplicada":" · No usar para puntuar"));
     body.append(foot);
     card.append(body);content.append(card);
   }
 }
 search.addEventListener("input",render);render();
 root.append(el("p","footer-note","Auditoría contextualizada para el examen vigente desde mayo de 2026. Conservar una pregunta en el archivo no significa que su solución original sea correcta."));
}
