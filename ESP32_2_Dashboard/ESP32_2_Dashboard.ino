/*
  ESP32 #2 - DASHBOARD FÍSICO (servidor web con gráficas)
  -------------------------------------------------------
  Consulta periódicamente las tablas GOLD (y los últimos eventos del ESP32 #1 en BRONZE)
  mediante la API REST "SQL Statement Execution" de Databricks y sirve una página web
  con KPIs, gráfico de barras, gráfico de líneas, filtros y tabla en vivo.

  Abrir en el navegador (celular o PC en la misma red):  http://dashboard-esp32.local
  o la IP que aparece en el Monitor Serie.

  Las gráficas se dibujan con <canvas> y JavaScript puro: no dependen de internet en el navegador.

  Semáforo de estado (cada LED con resistencia de 220-330 ohm a GND):
    - LED VERDE    (GPIO 25): fijo = datos de Gold al día | 3 destellos = llegaron eventos nuevos del ESP32 #1.
    - LED AMARILLO (GPIO 26): encendido mientras consulta Databricks (parpadea al conectar Wi-Fi).
    - LED ROJO     (GPIO 27): la última consulta falló o no hay Wi-Fi.
    - Al encender: autodiagnóstico (verde -> amarillo -> rojo).

  Librerías: ArduinoJson (v7). Placa: "ESP32 Dev Module".
*/

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <WebServer.h>
#include <ESPmDNS.h>
#include <ArduinoJson.h>

// ===================== CONFIGURACIÓN (EDITAR) =====================
const char* WIFI_SSID        = "EDI";
const char* WIFI_PASS        = "12345678";
const char* DATABRICKS_HOST  = "dbc-2e9d45b0-5357.cloud.databricks.com";
const char* DATABRICKS_TOKEN = "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
const char* WAREHOUSE_ID     = "415fe1a16c83e7ea";
const char* CATALOG          = "proyecto_nota3";   // o "workspace"
// Red propia del ESP32 #2 (punto de acceso): sirve para abrir el dashboard aunque el hotspot aísle a los clientes.
// Conecta el PC o el celular a esta red y abre http://192.168.4.1
const char* AP_SSID = "Dashboard-ESP32";
const char* AP_PASS = "12345678";                   // mínimo 8 caracteres
const uint32_t CONSULTA_CADA_SEG = 15;              // cada cuánto consulta Databricks (4 consultas ~8 s; no bajar de 10)
// ===================================================================

const int PIN_LED      = 2;    // LED integrado (espejo del amarillo)
const int LED_VERDE    = 25;
const int LED_AMARILLO = 26;
const int LED_ROJO     = 27;
WebServer server(80);
long eventosEspPrevios = -1;   // para detectar cuando el ESP32 #1 envía datos nuevos

void amarillo(bool on) { digitalWrite(LED_AMARILLO, on); digitalWrite(PIN_LED, on); }

void autodiagnostico() {
  int leds[] = {LED_VERDE, LED_AMARILLO, LED_ROJO};
  for (int i = 0; i < 3; i++) { digitalWrite(leds[i], HIGH); delay(250); }
  delay(400);
  for (int i = 0; i < 3; i++) digitalWrite(leds[i], LOW);
}

void destellosVerde() {
  for (int i = 0; i < 3; i++) {
    digitalWrite(LED_VERDE, LOW);  delay(90);
    digitalWrite(LED_VERDE, HIGH); delay(90);
  }
}

String   cacheJson = "{\"ok\":false,\"msg\":\"Esperando la primera consulta a Databricks...\"}";
uint32_t ultimaConsulta = 0;
bool     primeraVez = true;

const char* SQL_KPI =
  "SELECT "
  "(SELECT ROUND(SUM(ingresos_totales),2) FROM gold.gold_sales_summary),"
  "(SELECT SUM(total_compras) FROM gold.gold_sales_summary),"
  "(SELECT COUNT(DISTINCT user_id) FROM gold.gold_user_metrics),"
  "(SELECT COUNT(*) FROM bronze.eventos_raw WHERE source='ESP32'),"
  "(SELECT date_format(from_utc_timestamp(MAX(ingestion_time),'America/Bogota'),'yyyy-MM-dd HH:mm:ss') FROM bronze.eventos_raw WHERE source='ESP32'),"
  "(SELECT CAST(unix_timestamp() - unix_timestamp(MAX(ingestion_time)) AS INT) FROM bronze.eventos_raw WHERE source='ESP32')";

const char* SQL_MES =
  "SELECT CAST(mes AS STRING), region, categoria, ROUND(SUM(ingresos_totales),2), SUM(total_compras) "
  "FROM gold.gold_sales_summary GROUP BY 1,2,3 ORDER BY 1,2,3";

const char* SQL_ESP =
  "SELECT event_id, event_type, category, region, amount, quantity, "
  "date_format(from_utc_timestamp(ingestion_time,'America/Bogota'),'HH:mm:ss'), device_id "
  "FROM bronze.eventos_raw WHERE source='ESP32' ORDER BY ingestion_time DESC LIMIT 10";

// Ritmo de llegada de eventos IoT por minuto (últimos 30 minutos)
const char* SQL_RITMO =
  "SELECT date_format(date_trunc('minute', from_utc_timestamp(ingestion_time,'America/Bogota')),'HH:mm'), COUNT(*) "
  "FROM bronze.eventos_raw WHERE source='ESP32' "
  "AND ingestion_time >= current_timestamp() - INTERVAL 30 MINUTES GROUP BY 1 ORDER BY 1";

uint32_t latenciaMs = 0, consultasOk = 0, consultasError = 0;

// ---------- Página web (HTML + CSS + JS en memoria flash) ----------
const char PAGINA[] PROGMEM = R"rawliteral(
<!DOCTYPE html><html lang="es" data-theme="dark"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lakehouse IoT · Centro de mando</title>
<style>
:root{--bg:#070b16;--panel:rgba(255,255,255,.07);--panel2:rgba(255,255,255,.10);--line:rgba(255,255,255,.16);--tx:#f1f5fb;--mut:#a8b6d0;--acc:#4f8df7;--ok:#34d399;--warn:#fbbf24;--bad:#f87171;--blur:blur(18px) saturate(140%);--shadow:0 8px 32px rgba(0,0,0,.35)}
[data-theme=light]{--bg:#dbe7f5;--panel:rgba(255,255,255,.55);--panel2:rgba(255,255,255,.72);--line:rgba(255,255,255,.85);--tx:#0f172a;--mut:#475569;--shadow:0 8px 32px rgba(31,60,110,.18)}
*{box-sizing:border-box}html{background:var(--bg)}body{margin:0;min-height:100vh;font-family:Inter,system-ui,Segoe UI,Arial;color:var(--tx);font-size:14px}
body::before{content:"";position:fixed;inset:0;z-index:-2;background:radial-gradient(60% 50% at 15% 10%,rgba(59,130,246,.55),transparent 70%),radial-gradient(50% 45% at 85% 20%,rgba(168,85,247,.45),transparent 70%),radial-gradient(55% 50% at 70% 90%,rgba(20,184,166,.40),transparent 70%),radial-gradient(45% 40% at 10% 85%,rgba(244,114,182,.30),transparent 70%),var(--bg)}
body::after{content:"";position:fixed;width:46vmax;height:46vmax;left:-10vmax;top:30vh;z-index:-1;border-radius:50%;background:radial-gradient(circle,rgba(99,102,241,.38),transparent 65%);filter:blur(40px);animation:fl 28s ease-in-out infinite alternate}
@keyframes fl{to{transform:translate(70vw,-20vh) scale(1.15)}}
@media(prefers-reduced-motion:reduce){body::after{animation:none}}
header{display:flex;align-items:center;gap:14px;padding:12px 20px;border-bottom:1px solid var(--line);background:var(--panel);position:sticky;top:0;z-index:5}
header h1{font-size:17px;margin:0;font-weight:600}header .sub{color:var(--mut);font-size:12px}
.live{display:inline-flex;align-items:center;gap:6px;font-size:12px;font-weight:600;color:var(--ok);background:rgba(34,197,94,.12);padding:4px 10px;border-radius:99px}
.live i{width:8px;height:8px;border-radius:50%;background:var(--ok);animation:p 1.4s infinite}
.live.off{color:var(--bad);background:rgba(239,68,68,.12)}.live.off i{background:var(--bad);animation:none}
@keyframes p{0%{box-shadow:0 0 0 0 rgba(34,197,94,.7)}70%{box-shadow:0 0 0 8px rgba(34,197,94,0)}100%{box-shadow:0 0 0 0 rgba(34,197,94,0)}}
.sp{flex:1}.btn{background:var(--panel2);border:1px solid var(--line);color:var(--tx);border-radius:8px;padding:6px 12px;cursor:pointer;font-size:13px}
.btn:hover{border-color:var(--acc)}
main{padding:16px 20px;max-width:1400px;margin:auto;display:grid;gap:14px}
.pipe{display:flex;align-items:center;gap:6px;flex-wrap:wrap;background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:10px 14px}
.node{display:flex;align-items:center;gap:8px;padding:6px 12px;border-radius:8px;background:var(--panel2);border:1px solid var(--line);font-size:12px}
.node b{font-size:13px}.dot{width:9px;height:9px;border-radius:50%;background:var(--mut)}
.dot.ok{background:var(--ok)}.dot.warn{background:var(--warn)}.dot.bad{background:var(--bad)}
.arrow{color:var(--mut)}
.filters{display:flex;gap:16px;flex-wrap:wrap;align-items:center}
.chips{display:flex;gap:6px;flex-wrap:wrap;align-items:center}.chips span{color:var(--mut);font-size:12px;margin-right:4px}
.chip{border:1px solid var(--line);background:var(--panel);color:var(--tx);border-radius:99px;padding:5px 12px;font-size:12px;cursor:pointer}
.chip.on{background:var(--acc);border-color:var(--acc);color:#fff}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px;min-width:0}
.card h3{margin:0 0 8px;font-size:13px;font-weight:600;color:var(--mut);display:flex;justify-content:space-between}
.kpi .v{font-size:26px;font-weight:700;letter-spacing:-.5px}
.kpi .d{font-size:12px;margin-top:2px}.up{color:var(--ok)}.down{color:var(--bad)}.flat{color:var(--mut)}
.kpi canvas{height:38px;margin-top:6px}
.row{display:grid;grid-template-columns:2fr 1fr;gap:12px}
@media(max-width:900px){.row{grid-template-columns:1fr}}
canvas{width:100%;display:block}.ch{height:280px}.ch2{height:240px}
.legend{display:flex;flex-wrap:wrap;gap:10px;font-size:12px;color:var(--mut);margin-top:8px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:5px;vertical-align:-1px}
table{width:100%;border-collapse:collapse;font-size:12.5px}th{color:var(--mut);font-weight:600;text-align:left;padding:7px 6px;border-bottom:1px solid var(--line)}
td{padding:7px 6px;border-bottom:1px solid var(--line)}tr.new td{animation:hl 2.5s}
@keyframes hl{0%{background:rgba(59,130,246,.35)}100%{background:transparent}}
.tag{font-size:11px;padding:2px 8px;border-radius:99px;background:var(--panel2);border:1px solid var(--line)}
.tag.purchase{color:var(--ok);border-color:rgba(34,197,94,.4)}.tag.refund{color:var(--bad);border-color:rgba(239,68,68,.4)}
.sys{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}
.sys div{background:var(--panel2);border:1px solid var(--line);border-radius:10px;padding:10px}.sys span{display:block;color:var(--mut);font-size:11px}.sys b{font-size:16px}
#tip{position:fixed;pointer-events:none;background:#0f172a;color:#fff;border:1px solid #334155;border-radius:8px;padding:8px 10px;font-size:12px;display:none;z-index:9;box-shadow:0 6px 20px rgba(0,0,0,.35);white-space:nowrap}
.err{background:rgba(239,68,68,.12);border:1px solid rgba(239,68,68,.4);color:var(--bad);border-radius:10px;padding:10px 14px;display:none}
@media(max-width:640px){header{flex-wrap:wrap;padding:10px 12px}header .sub,#upd{display:none}main{padding:12px}.hm{display:none}.kpi .v{font-size:22px}}
footer{color:var(--mut);font-size:11px;text-align:center;padding:10px}
/* Efecto vidrio (glassmorphism): paneles translúcidos con desenfoque sobre el fondo */
header,.pipe,.card,.chip,.btn,.node,.sys div{backdrop-filter:var(--blur);-webkit-backdrop-filter:var(--blur)}
header{box-shadow:0 4px 24px rgba(0,0,0,.25)}
.pipe,.card{border-radius:16px;box-shadow:var(--shadow),inset 0 1px 0 rgba(255,255,255,.14)}
[data-theme=dark] .card,[data-theme=dark] .pipe{background:linear-gradient(145deg,rgba(255,255,255,.12),rgba(255,255,255,.04))}
.sys div,.node{border-radius:12px}
#tip{background:rgba(15,23,42,.82);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);border-color:rgba(255,255,255,.18)}
</style></head><body>
<header><div><h1>Lakehouse IoT · Centro de mando</h1><div class="sub">ESP32 → Databricks (Bronze · Silver · Gold) → ESP32</div></div>
<span id="live" class="live off"><i></i><span id="liveTx">CONECTANDO</span></span><div class="sp"></div>
<span class="sub" id="upd"></span><button class="btn" id="csv">Exportar CSV</button><button class="btn" id="theme">Tema</button></header>
<main>
<div class="err" id="err"></div>
<div class="pipe" id="pipe"></div>
<div class="filters"><div class="chips" id="fR"><span>Región</span></div><div class="chips" id="fC"><span>Categoría</span></div><button class="btn" id="reset">Limpiar filtros</button></div>
<div class="kpis">
 <div class="card kpi"><h3>Ingresos totales<span>Gold</span></h3><div class="v" id="k1">–</div><div class="d" id="d1"></div><canvas id="s1"></canvas></div>
 <div class="card kpi"><h3>Órdenes de compra<span>Gold</span></h3><div class="v" id="k2">–</div><div class="d" id="d2"></div><canvas id="s2"></canvas></div>
 <div class="card kpi"><h3>Ticket promedio<span>Gold</span></h3><div class="v" id="k3">–</div><div class="d" id="d3"></div><canvas id="s3"></canvas></div>
 <div class="card kpi"><h3>Clientes únicos<span>global</span></h3><div class="v" id="k4">–</div><div class="d flat">Tabla gold_user_metrics</div></div>
 <div class="card kpi"><h3>Eventos IoT recibidos<span>Bronze</span></h3><div class="v" id="k5">–</div><div class="d" id="d5"></div><canvas id="s5"></canvas></div>
</div>
<div class="row">
 <div class="card"><h3>Ingresos por región y categoría</h3><canvas id="cBar" class="ch"></canvas><div class="legend" id="lBar"></div></div>
 <div class="card"><h3>Participación por región</h3><canvas id="cDon" class="ch"></canvas><div class="legend" id="lDon"></div></div>
</div>
<div class="row">
 <div class="card"><h3>Evolución mensual de ventas por región</h3><canvas id="cLine" class="ch"></canvas><div class="legend" id="lLine"></div></div>
 <div class="card"><h3>Mapa de calor · región × categoría</h3><canvas id="cHeat" class="ch"></canvas></div>
</div>
<div class="row">
 <div class="card"><h3>Transmisión en vivo · ESP32 #1 → Bronze<span id="rate"></span></h3>
  <table><thead><tr><th>Hora</th><th class="hm">Dispositivo</th><th class="hm">Evento</th><th>Tipo</th><th>Categoría</th><th>Región</th><th style="text-align:right">Total</th></tr></thead><tbody id="feed"></tbody></table></div>
 <div class="card"><h3>Ranking de categorías</h3><canvas id="cRank" class="ch2"></canvas></div>
</div>
<div class="card"><h3>Salud del sistema · ESP32 #2</h3><div class="sys" id="sys"></div></div>
</main>
<footer>Proyecto Final Big Data en la Nube · Universidad Inpahu · Datos servidos por un ESP32 desde la capa Gold de Databricks</footer>
<div id="tip"></div>
<script>
const PAL=['#3b82f6','#22c55e','#f59e0b','#ef4444','#a855f7'];
const REG=['CENTRO','ESTE','NORTE','OESTE','SUR'],CAT=['DEPORTES','ELECTRONICA','HOGAR','JUGUETERIA','ROPA'];
const MES=['ene','feb','mar','abr','may','jun','jul','ago','sep','oct','nov','dic'];
const $=i=>document.getElementById(i);
let D=null,selR=new Set(),selC=new Set(),vistos=new Set(),hits={};
const money=n=>'$'+Math.round(n).toLocaleString('es-CO'),num=n=>Math.round(n).toLocaleString('es-CO');
const short=n=>n>=1e6?'$'+(n/1e6).toFixed(2)+'M':n>=1e3?'$'+(n/1e3).toFixed(1)+'k':'$'+n.toFixed(0);
const css=v=>getComputedStyle(document.documentElement).getPropertyValue(v).trim();
const mlabel=m=>MES[+m.slice(5,7)-1]+' '+m.slice(2,4);
try{const t=localStorage.getItem('tema');if(t)document.documentElement.dataset.theme=t}catch(e){}
$('theme').onclick=()=>{const h=document.documentElement;h.dataset.theme=h.dataset.theme=='dark'?'light':'dark';try{localStorage.setItem('tema',h.dataset.theme)}catch(e){}render()};
function chips(id,arr,set){const c=$(id);arr.forEach(v=>{const b=document.createElement('button');b.className='chip';b.textContent=v[0]+v.slice(1).toLowerCase();
 b.onclick=()=>{set.has(v)?set.delete(v):set.add(v);b.classList.toggle('on');render()};c.appendChild(b)})}
chips('fR',REG,selR);chips('fC',CAT,selC);
$('reset').onclick=()=>{selR.clear();selC.clear();document.querySelectorAll('.chip').forEach(b=>b.classList.remove('on'));render()};
async function cargar(){try{const r=await fetch('/api/data',{cache:'no-store'});D=await r.json();render()}catch(e){setLive(false,'SIN CONEXIÓN')}}
function setLive(on,t){$('live').className='live'+(on?'':' off');$('liveTx').textContent=t}
const F=()=>(D.m||[]).filter(x=>(!selR.size||selR.has(x[1]))&&(!selC.size||selC.has(x[2])));
const sum=(a,i)=>a.reduce((s,x)=>s+ +x[i],0);
function render(){
 if(!D)return;
 if(!D.ok){$('err').style.display='block';$('err').textContent=D.msg||'Error';setLive(false,'ERROR');return}
 $('err').style.display=D.err?'block':'none';$('err').textContent=D.err?'Última consulta falló: '+D.err+' (mostrando los últimos datos válidos)':'';
 setLive(!D.err,D.err?'DEGRADADO':'EN VIVO');$('upd').textContent='Datos de hace '+D.edad+' s';
 const f=F(),R=selR.size?REG.filter(r=>selR.has(r)):REG,C=selC.size?CAT.filter(c=>selC.has(c)):CAT;
 const meses=[...new Set((D.m||[]).map(x=>x[0].slice(0,7)))].sort();
 const porMes=meses.map(m=>{const g=f.filter(x=>x[0].startsWith(m));return[sum(g,3),sum(g,4)]});
 const ing=sum(f,3),comp=sum(f,4);
 $('k1').textContent=short(ing);$('k2').textContent=num(comp);$('k3').textContent=money(comp?ing/comp:0);$('k4').textContent=num(+D.k[2]);$('k5').textContent=num(+D.k[3]);
 const hoy=new Date(),cur=hoy.getFullYear()+'-'+String(hoy.getMonth()+1).padStart(2,'0');
 let li=meses.length-1;if(meses[li]==cur)li--;
 const delta=(a,b)=>b?((a-b)/b*100):0,dd=(el,v,lb)=>{$(el).className='d '+(v>0.5?'up':v<-0.5?'down':'flat');$(el).textContent=(v>0?'▲ ':v<0?'▼ ':'')+Math.abs(v).toFixed(1)+'% '+lb};
 if(li>0){const lb='vs '+mlabel(meses[li-1]);dd('d1',delta(porMes[li][0],porMes[li-1][0]),lb+' ('+mlabel(meses[li])+')');dd('d2',delta(porMes[li][1],porMes[li-1][1]),lb);
  const t=i=>porMes[i][1]?porMes[i][0]/porMes[i][1]:0;dd('d3',delta(t(li),t(li-1)),lb)}
 const pm=porMes.slice(0,li+1);spark($('s1'),pm.map(x=>x[0]),PAL[0]);spark($('s2'),pm.map(x=>x[1]),PAL[1]);spark($('s3'),pm.map(x=>x[1]?x[0]/x[1]:0),PAL[2]);
 const rt=(D.r||[]).map(x=>+x[1]),ult=rt.length?rt[rt.length-1]:0;spark($('s5'),rt.length?rt:[0,0],PAL[4]);
 $('d5').className='d '+(ult?'up':'flat');$('d5').textContent=ult?ult+' eventos en el último minuto':'Sin eventos en los últimos 30 min';
 $('rate').textContent=rt.length?(rt.reduce((a,b)=>a+b,0)/rt.length).toFixed(1)+' ev/min (30 min)':'';
 const sb=C.map(c=>({n:c,c:PAL[CAT.indexOf(c)],v:R.map(r=>sum(f.filter(x=>x[1]==r&&x[2]==c),3))}));
 barras($('cBar'),R,sb);leg('lBar',sb);
 const dv=R.map(r=>({n:r,c:PAL[REG.indexOf(r)],v:sum(f.filter(x=>x[1]==r),3)}));dona($('cDon'),dv,ing);leg('lDon',dv.map(x=>({n:x.n+' '+(ing?(x.v/ing*100).toFixed(1):0)+'%',c:x.c})));
 const sl=R.map(r=>({n:r,c:PAL[REG.indexOf(r)],v:meses.map(m=>sum(f.filter(x=>x[1]==r&&x[0].startsWith(m)),3))}));lineas($('cLine'),meses,sl,meses[meses.length-1]==cur);leg('lLine',sl);
 heat($('cHeat'),R,C,f);
 ranking($('cRank'),C.map(c=>({n:c,c:PAL[CAT.indexOf(c)],v:sum(f.filter(x=>x[2]==c),3)})).sort((a,b)=>b.v-a.v));
 pipeline();feed();sys();
}
function leg(id,s){$(id).innerHTML=s.map(x=>'<span><i style="background:'+x.c+'"></i>'+x.n+'</span>').join('')}
function ctx(cv){const r=devicePixelRatio||1,w=cv.clientWidth,h=cv.clientHeight;cv.width=w*r;cv.height=h*r;const g=cv.getContext('2d');g.scale(r,r);g.font='11px Inter,system-ui';hits[cv.id]=[];return{g,w,h}}
function hit(cv,x,y,w,h,html){hits[cv.id].push({x,y,w,h,html})}
function grid(g,w,h,max,L,B,T){g.strokeStyle=css('--line');g.fillStyle=css('--mut');g.textAlign='right';g.lineWidth=1;
 for(let i=0;i<=4;i++){const y=h-B-(h-B-T)*i/4;g.beginPath();g.moveTo(L,y+.5);g.lineTo(w,y+.5);g.stroke();g.fillText(short(max*i/4),L-6,y+4)}}
function nice(m){if(m<=0)return 1;const p=Math.pow(10,Math.floor(Math.log10(m))),n=m/p;return(n<=1?1:n<=2?2:n<=5?5:10)*p}
function spark(cv,v,c){const{g,w,h}=ctx(cv);if(v.length<2)return;const mx=Math.max(...v),mn=Math.min(...v),k=mx-mn||1;
 g.beginPath();v.forEach((y,i)=>{const X=i*(w-2)/(v.length-1)+1,Y=h-3-(y-mn)/k*(h-6);i?g.lineTo(X,Y):g.moveTo(X,Y)});g.strokeStyle=c;g.lineWidth=2;g.stroke();
 g.lineTo(w-1,h);g.lineTo(1,h);g.closePath();g.globalAlpha=.15;g.fillStyle=c;g.fill();g.globalAlpha=1}
function rr(g,x,y,w,h,r){r=Math.min(r,w/2,h/2);g.beginPath();g.moveTo(x,y+h);g.lineTo(x,y+r);g.quadraticCurveTo(x,y,x+r,y);g.lineTo(x+w-r,y);g.quadraticCurveTo(x+w,y,x+w,y+r);g.lineTo(x+w,y+h);g.closePath();g.fill()}
function barras(cv,lab,se){const{g,w,h}=ctx(cv),L=54,B=24,T=10,max=nice(Math.max(1,...se.flatMap(s=>s.v)));grid(g,w,h,max,L,B,T);
 const gw=(w-L)/lab.length,bw=Math.min(46,Math.max(3,gw*.78/se.length));
 lab.forEach((lb,i)=>{const x0=L+i*gw+(gw-bw*se.length)/2;se.forEach((s,j)=>{const v=s.v[i],bh=(h-B-T)*v/max,x=x0+j*bw;g.fillStyle=s.c;rr(g,x,h-B-bh,bw-2,bh,3);
  hit(cv,x,h-B-bh,bw-2,bh,'<b>'+lb+' · '+s.n+'</b><br>'+money(v))});
  g.fillStyle=css('--mut');g.textAlign='center';g.fillText(lb,L+i*gw+gw/2,h-7)})}
function dona(cv,d,tot){const{g,w,h}=ctx(cv),cx=w/2,cy=h/2,R=Math.min(w,h)/2-8,r=R*.62;let a=-Math.PI/2;
 d.forEach(s=>{const da=tot?s.v/tot*2*Math.PI:0;g.beginPath();g.arc(cx,cy,R,a,a+da);g.arc(cx,cy,r,a+da,a,true);g.closePath();g.fillStyle=s.c;g.fill();
  s.a0=a;s.a1=a+da;a+=da});g.fillStyle=css('--tx');g.textAlign='center';g.font='700 20px Inter,system-ui';g.fillText(short(tot),cx,cy+4);
 g.font='11px Inter,system-ui';g.fillStyle=css('--mut');g.fillText('ingresos',cx,cy+20);cv._don={cx,cy,R,r,d,tot}}
function lineas(cv,lab,se,parcial){const{g,w,h}=ctx(cv),L=54,B=24,T=10,max=nice(Math.max(1,...se.flatMap(s=>s.v)));grid(g,w,h,max,L,B,T);
 const n=lab.length,dx=n>1?(w-L-44)/(n-1):0,X=i=>L+12+i*dx,Y=v=>h-B-(h-B-T)*v/max,nf=parcial?n-1:n;
 se.forEach(s=>{g.strokeStyle=s.c;g.lineWidth=2.2;g.beginPath();s.v.slice(0,nf).forEach((v,i)=>i?g.lineTo(X(i),Y(v)):g.moveTo(X(i),Y(v)));g.stroke();
  if(parcial&&n>1){g.setLineDash([4,4]);g.beginPath();g.moveTo(X(n-2),Y(s.v[n-2]));g.lineTo(X(n-1),Y(s.v[n-1]));g.stroke();g.setLineDash([])}
  s.v.forEach((v,i)=>{g.fillStyle=s.c;g.beginPath();g.arc(X(i),Y(v),2.6,0,7);g.fill()})});g.lineWidth=1;
 g.fillStyle=css('--mut');g.textAlign='center';lab.forEach((m,i)=>{if(n<=10||i%2==0||i==n-1)g.fillText(mlabel(m)+(parcial&&i==n-1?'*':''),X(i),h-7)});
 if(parcial){g.textAlign='right';g.fillText('* mes en curso (parcial)',w-4,T+4)}
 lab.forEach((m,i)=>hit(cv,X(i)-dx/2,0,dx||w,h,'<b>'+mlabel(m)+'</b><br>'+se.map(s=>'<span style="color:'+s.c+'">●</span> '+s.n+': '+money(s.v[i])).join('<br>')));
 cv._line={X,lab,h,B,T}}
function heat(cv,R,C,f){const{g,w,h}=ctx(cv),L=78,T=20,cw=(w-L)/C.length,ch=Math.min(64,(h-T-6)/R.length);
 const v=R.map(r=>C.map(c=>sum(f.filter(x=>x[1]==r&&x[2]==c),3))),all=v.flat(),mx=Math.max(...all),mn=Math.min(...all),k=mx-mn||1;
 g.textAlign='center';C.forEach((c,j)=>{g.fillStyle=css('--mut');g.fillText(c.slice(0,6),L+j*cw+cw/2,12)});
 R.forEach((r,i)=>{g.fillStyle=css('--mut');g.textAlign='right';g.fillText(r,L-8,T+i*ch+ch/2+4);
  C.forEach((c,j)=>{const t=(v[i][j]-mn)/k,x=L+j*cw+2,y=T+i*ch+2;g.fillStyle='rgba(59,130,246,'+(0.12+t*0.85).toFixed(2)+')';g.fillRect(x,y,cw-4,ch-4);
   g.fillStyle=t>0.55?'#fff':css('--tx');g.textAlign='center';g.fillText(short(v[i][j]),x+(cw-4)/2,y+(ch-4)/2+4);
   hit(cv,x,y,cw-4,ch-4,'<b>'+r+' · '+c+'</b><br>'+money(v[i][j]))})})}
function ranking(cv,d){const{g,w,h}=ctx(cv),L=96,bh=Math.min(30,(h-10)/d.length-8),mx=Math.max(1,...d.map(x=>x.v));
 d.forEach((s,i)=>{const y=6+i*(bh+8),bw=(w-L-70)*s.v/mx;g.fillStyle=css('--mut');g.textAlign='right';g.fillText((i+1)+'. '+s.n,L-8,y+bh/2+4);
  g.fillStyle=s.c;rr(g,L,y,bw,bh,4);g.fillStyle=css('--tx');g.textAlign='left';g.fillText(short(s.v),L+bw+6,y+bh/2+4);hit(cv,L,y,bw,bh,'<b>'+s.n+'</b><br>'+money(s.v))})}
function age(t){if(!t)return 1e9;const d=new Date(t.replace(' ','T'));return isNaN(d)?1e9:(Date.now()-d)/1000}
function pipeline(){const a=D.k[5]!=null?+D.k[5]:age(D.k[4]),esp=a<120?'ok':a<900?'warn':'bad',db=D.err?'bad':'ok',fresh=D.edad<120?'ok':'warn';
 const n=[['ESP32 #1','Productor',esp],['Bronze','eventos_raw',esp=='bad'?'warn':'ok'],['Silver','silver_events',db],['Gold','sales · users',db],['ESP32 #2','Este panel',fresh]];
 $('pipe').innerHTML=n.map((x,i)=>'<div class="node"><span class="dot '+x[2]+'"></span><div><b>'+x[0]+'</b><br><span style="color:var(--mut)">'+x[1]+'</span></div></div>'+(i<4?'<span class="arrow">→</span>':'')).join('')+
  '<div class="sp"></div><span class="sub">Último evento IoT: '+(D.k[4]||'—')+'</span>'}
function feed(){const rows=(D.e||[]).filter(x=>(!selR.size||selR.has(x[3]))&&(!selC.size||selC.has(x[2])));
 $('feed').innerHTML=rows.map(x=>{const nw=vistos.size&&!vistos.has(x[0]);return'<tr class="'+(nw?'new':'')+'"><td>'+x[6]+'</td><td class="hm">'+(x[7]||'')+'</td><td class="hm">'+x[0]+'</td><td><span class="tag '+x[1]+'">'+x[1]+'</span></td><td>'+x[2]+'</td><td>'+x[3]+'</td><td style="text-align:right">$'+(+x[4]*+x[5]).toFixed(2)+'</td></tr>'}).join('')||'<tr><td colspan=7 style="color:var(--mut)">Esperando eventos del ESP32 #1…</td></tr>';
 (D.e||[]).forEach(x=>vistos.add(x[0]))}
function sys(){const s=D.sys||{},up=s.up||0;const q=s.rssi>-60?'excelente':s.rssi>-70?'buena':s.rssi>-80?'regular':'débil';
 const it=[['Señal Wi-Fi',(s.rssi||'–')+' dBm · '+q],['Tiempo encendido',Math.floor(up/3600)+'h '+Math.floor(up%3600/60)+'m'],['Memoria libre',((s.heap||0)/1024).toFixed(0)+' KB'],
  ['Latencia Databricks',(s.lat||0)+' ms'],['Consultas OK / error',(s.ok||0)+' / '+(s.ko||0)],['Filas Gold recibidas',num((D.m||[]).length)]];
 $('sys').innerHTML=it.map(x=>'<div><span>'+x[0]+'</span><b>'+x[1]+'</b></div>').join('')}
const tip=$('tip');
document.addEventListener('mousemove',e=>{const cv=e.target;if(cv.tagName!='CANVAS'||!hits[cv.id]){tip.style.display='none';return}
 const b=cv.getBoundingClientRect(),x=e.clientX-b.left,y=e.clientY-b.top;let html=null;
 if(cv._don){const o=cv._don,dx=x-o.cx,dy=y-o.cy,dist=Math.hypot(dx,dy);if(dist>o.r&&dist<o.R){let a=Math.atan2(dy,dx);if(a<-Math.PI/2)a+=2*Math.PI;
  const s=o.d.find(s=>a>=s.a0&&a<s.a1);if(s)html='<b>'+s.n+'</b><br>'+money(s.v)+' · '+(s.v/o.tot*100).toFixed(1)+'%'}}
 if(!html){const hh=hits[cv.id].find(r=>x>=r.x&&x<=r.x+r.w&&y>=r.y&&y<=r.y+r.h);if(hh)html=hh.html}
 if(html){tip.innerHTML=html;tip.style.display='block';const tw=tip.offsetWidth;tip.style.left=Math.min(e.clientX+14,innerWidth-tw-8)+'px';tip.style.top=(e.clientY+14)+'px'}else tip.style.display='none'});
$('csv').onclick=()=>{if(!D)return;const rows=[['mes','region','categoria','ingresos','compras']].concat(F());
 const b=new Blob(['﻿'+rows.map(r=>r.join(';')).join('\n')],{type:'text/csv'}),a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='gold_ventas_filtrado.csv';a.click()};
addEventListener('resize',render);cargar();setInterval(cargar,10000);
</script></body></html>

)rawliteral";

// ---------- Databricks ----------
// Ejecuta una consulta y copia result.data_array en 'destino'. Devuelve "" si todo salió bien, o el error.
String consultar(const char* sql, JsonDocument& destino) {
  WiFiClientSecure cliente;
  cliente.setInsecure();
  HTTPClient https;
  https.setTimeout(45000);
  String url = String("https://") + DATABRICKS_HOST + "/api/2.0/sql/statements";
  if (!https.begin(cliente, url)) return "sin conexión HTTPS";
  https.addHeader("Authorization", String("Bearer ") + DATABRICKS_TOKEN);
  https.addHeader("Content-Type", "application/json");

  JsonDocument req;
  req["warehouse_id"] = WAREHOUSE_ID;
  req["catalog"]      = CATALOG;
  req["statement"]    = sql;
  req["wait_timeout"] = "50s";
  req["on_wait_timeout"] = "CANCEL";
  String cuerpo;
  serializeJson(req, cuerpo);

  int codigo = https.POST(cuerpo);
  String respuesta = https.getString();
  https.end();
  if (codigo != 200) return "HTTP " + String(codigo) + " " + respuesta.substring(0, 120);

  JsonDocument res;
  DeserializationError e = deserializeJson(res, respuesta);
  if (e) return String("JSON: ") + e.c_str();
  const char* estado = res["status"]["state"] | "?";
  if (strcmp(estado, "SUCCEEDED") != 0) {
    const char* msg = res["status"]["error"]["message"] | estado;
    return String(msg).substring(0, 150);
  }
  destino.set(res["result"]["data_array"]);
  return "";
}

void actualizarDatos() {
  amarillo(true);                         // AMARILLO: consultando Databricks
  JsonDocument kpi, mes, esp, ritmo;
  uint32_t t0 = millis();
  String err = consultar(SQL_KPI, kpi);
  if (err == "") err = consultar(SQL_MES, mes);
  if (err == "") err = consultar(SQL_ESP, esp);
  if (err == "") err = consultar(SQL_RITMO, ritmo);
  latenciaMs = millis() - t0;
  if (err == "") consultasOk++; else consultasError++;
  amarillo(false);

  if (err != "") {
    Serial.println("Error consultando Databricks: " + err);
    digitalWrite(LED_ROJO, HIGH);         // ROJO: consulta fallida
    digitalWrite(LED_VERDE, LOW);
    if (primeraVez) {
      JsonDocument e;
      e["ok"] = false;
      e["msg"] = "Databricks: " + err;
      cacheJson = "";
      serializeJson(e, cacheJson);
    } else {
      // conserva los últimos datos buenos y solo marca el error
      JsonDocument previo;
      deserializeJson(previo, cacheJson);
      previo["err"] = err;
      cacheJson = "";
      serializeJson(previo, cacheJson);
    }
    return;
  }

  JsonDocument out;
  out["ok"] = true;
  out["k"]  = kpi[0];   // [ingresos, compras, usuarios, eventos_esp32, ultimo_evento (hora Bogotá), segundos_desde_ultimo]
  out["m"]  = mes;      // [[mes, region, categoria, ingresos, compras], ...]
  out["e"]  = esp;      // últimos 10 eventos del ESP32 #1
  out["r"]  = ritmo;    // [[HH:mm, eventos], ...] últimos 30 minutos
  out["t"]  = millis();
  cacheJson = "";
  serializeJson(out, cacheJson);
  primeraVez = false;
  digitalWrite(LED_ROJO, LOW);
  digitalWrite(LED_VERDE, HIGH);          // VERDE: datos al día

  long eventosEsp = String((const char*)(kpi[0][3] | "0")).toInt();
  if (eventosEspPrevios >= 0 && eventosEsp > eventosEspPrevios) {
    Serial.printf("Nuevos eventos del ESP32 #1: +%ld\n", eventosEsp - eventosEspPrevios);
    destellosVerde();                     // 3 destellos: llegó información del otro ESP32
  }
  eventosEspPrevios = eventosEsp;
  Serial.printf("Datos actualizados (%u bytes, %u filas mensuales)\n", (unsigned)cacheJson.length(), (unsigned)mes.size());
}

// ---------- Servidor web ----------
void handleRaiz() { server.send_P(200, "text/html; charset=utf-8", PAGINA); }

void handleData() {
  // inserta la "edad" de los datos (segundos desde la última consulta exitosa)
  JsonDocument doc;
  if (!deserializeJson(doc, cacheJson) && doc["t"].is<uint32_t>()) {
    doc["edad"] = (millis() - doc["t"].as<uint32_t>()) / 1000;
    JsonObject sys = doc["sys"].to<JsonObject>();   // salud del sistema en tiempo real
    sys["rssi"] = WiFi.RSSI();
    sys["up"]   = millis() / 1000;
    sys["heap"] = ESP.getFreeHeap();
    sys["lat"]  = latenciaMs;
    sys["ok"]   = consultasOk;
    sys["ko"]   = consultasError;
    String s; serializeJson(doc, s);
    server.send(200, "application/json", s);
  } else {
    server.send(200, "application/json", cacheJson);
  }
}

void setup() {
  Serial.begin(115200);
  pinMode(PIN_LED, OUTPUT);
  pinMode(LED_VERDE, OUTPUT);
  pinMode(LED_AMARILLO, OUTPUT);
  pinMode(LED_ROJO, OUTPUT);
  autodiagnostico();
  Serial.println("\n=== ESP32 #2 - Dashboard web ===");

  WiFi.mode(WIFI_AP_STA);   // STA: sale a internet (Databricks) | AP: red propia para ver el dashboard
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  Serial.print("Conectando Wi-Fi");
  while (WiFi.status() != WL_CONNECTED) {
    amarillo(true); delay(250); amarillo(false); delay(250);   // conectando
    Serial.print(".");
  }
  Serial.printf("\nWi-Fi OK. Abre en el navegador: http://%s\n", WiFi.localIP().toString().c_str());

  // Se crea el punto de acceso después de conectar, para que use el mismo canal que la red del hotspot
  if (WiFi.softAP(AP_SSID, AP_PASS)) {
    Serial.printf("Red propia '%s' (clave %s). Conéctate a ella y abre: http://%s\n",
                  AP_SSID, AP_PASS, WiFi.softAPIP().toString().c_str());
  } else {
    Serial.println("No se pudo crear la red propia del ESP32 #2.");
  }

  if (MDNS.begin("dashboard-esp32")) {
    MDNS.addService("http", "tcp", 80);
    Serial.println("También disponible en: http://dashboard-esp32.local");
  }

  server.on("/", handleRaiz);
  server.on("/api/data", handleData);
  server.begin();

  actualizarDatos();
  ultimaConsulta = millis();
}

void loop() {
  server.handleClient();
  if (millis() - ultimaConsulta >= CONSULTA_CADA_SEG * 1000UL) {
    ultimaConsulta = millis();
    if (WiFi.status() != WL_CONNECTED) {
      digitalWrite(LED_VERDE, LOW);
      digitalWrite(LED_ROJO, HIGH);       // ROJO: sin Wi-Fi
      WiFi.reconnect();
    }
    else actualizarDatos();
  }
}
