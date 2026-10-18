/* Vista histórica: descatalogadas sin puntuar y preguntas validadas para comparar cambios. */
function archive2026Page(){
 const root=document.querySelector("#root");
 root.append(heading("Banco histórico y auditoría","Consulta las preguntas descatalogadas y las cinco que hemos corregido y reactivado, sin perder el texto del PDF.","AUDITORÍA DE OCTUBRE DE 2026"));
 const main=el("div","catalog-summary");
 for(const [num,title,desc] of [[243,"Banco vigente","Aparecen en sesiones nuevas"],[37,"Descatalogadas","Duplicadas o defectuosas"],[5,"Validadas","Recuperadas y puntuables"]]){
  const card=el("div","catalog-stat");
  card.append(el("strong","",String(num)),el("span","",title),el("small","",desc));main.append(card);
 }
 root.append(main);
 const info=el("div","catalog-info");
 info.append(el("strong","","Tus estadísticas no se pierden."));
 info.append(el("p","","Cada pregunta conserva su número y su historial anterior. Las 37 descatalogadas no aparecen en nuevos simulacros. Las cinco validadas (#11, #63, #199, #201 y #257) vuelven a ser puntuables. Puedes comparar el texto original con su versión actual."));
 root.append(info);
 const nav=el("div","catalog-tabs"),content=el("div","catalog-content"),search=el("input","catalog-search");
 search.type="search";search.placeholder="Buscar por número o texto…";search.setAttribute("aria-label","Buscar preguntas históricas");
 let view="archive";
 const views=[["archive","Descatalogadas (37)"],["validated","Recuperadas (5)"]];
 const filters={};
 views.forEach(([key,label])=>{
  const b=btn(label,()=>{view=key;search.value="";render()}, "catalog-tab");filters[key]=b;nav.append(b);
 });
 root.append(nav,search,content);
 function render(){
   content.replaceChildren();
   views.forEach(([key])=>filters[key].setAttribute("aria-pressed",String(view===key)));
   const query=search.value.toLowerCase().trim();
   const qs=QUESTIONS.filter(q=>(view==="validated"?[11,63,199,201,257].includes(q.id):q.bank26State==="archive")&&(!query||("#"+q.id+" "+q.question+" "+q.bank26Note).toLowerCase().includes(query)));
   if(!qs.length){content.append(el("p","small muted","No hay coincidencias."));return;}
   for(const q of qs){
     const card=el("details","catalog-item");
     const summary=el("summary","");
     summary.append(el("span","catalog-id","#"+q.id));
     const grouping=view==="validated"?"Validada y activa":BANK26_DUP[q.id]?"Pregunta repetida":"Pregunta con defecto";
     const labels=el("div","catalog-item-labels");
     labels.append(el("strong","",grouping),el("span","",q.category||""));
     summary.append(labels,el("span","catalog-chevron","⌄"));card.append(summary);
     const body=el("div","catalog-item-body");
     const note=el("div","catalog-why");note.append(el("strong","",view==="validated"?"Por qué se ha recuperado":"Por qué se descatalogó"),el("p","",q.bank26Note));body.append(note);
     const question=el("div","catalog-question");question.append(renderRich(q.bank26Original.question));body.append(question);
     const options=el("div","catalog-option-list");
     for(const o of q.bank26Original.options){
       const line=el("div","catalog-option");line.append(el("b","",o.key+"."));
       const t=el("div","");t.append(renderRich(o.text));line.append(t);options.append(line);
     }body.append(options);
     const foot=el("p","catalog-original-key","Solución del PDF original: "+q.bank26Original.correct.join(", ")+(view==="validated"?" · Validada tras corregir el enunciado":BANK26_DUP[q.id]?" · Pregunta duplicada":" · No usar para puntuar"));
     body.append(foot);
     if(view==="validated"){
       const present=el("details","catalog-item");present.style.marginTop="12px";
       present.append(el("summary","","Ver versión corregida y puntuable"));
       const content=el("div","catalog-item-body");content.append(renderRich(q.question));
       const opts=el("div","catalog-option-list");
       for(const o of q.options){const line=el("div","catalog-option");if(q.correct.includes(o.key))line.style.borderColor="var(--green)";line.append(el("b","",o.key+"."));const txt=el("div","");txt.append(renderRich(o.text));line.append(txt);opts.append(line)}
       content.append(opts,el("p","catalog-original-key","Clave actual: "+q.correct.join(", ")+" · "+q.answerWhy));
       present.append(content);body.append(present);
     }
     card.append(body);content.append(card);
   }
 }
 search.addEventListener("input",render);render();
 root.append(el("p","footer-note","Se conserva la versión histórica. Las cinco recuperadas usan enunciados actualizados, y sus soluciones se han contrastado con documentación oficial. Las descatalogadas no cuentan en simulacros nuevos."));
}
