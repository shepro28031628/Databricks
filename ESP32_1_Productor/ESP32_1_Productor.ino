/*
  ESP32 #1 - PRODUCTOR DE EVENTOS (punto de venta IoT)
  ----------------------------------------------------
  Genera eventos de venta con el mismo esquema del taller y los inserta por lotes
  en la tabla Bronze de Databricks usando la API REST "SQL Statement Execution".

  Hardware (semáforo de estado, cada LED con resistencia de 220-330 ohm a GND):
    - LED VERDE    (GPIO 25): fijo = Wi-Fi OK y enviando | parpadea = en pausa | apagado = sin Wi-Fi.
    - LED AMARILLO (GPIO 26): encendido mientras transmite un lote a Databricks (también parpadea al conectar Wi-Fi).
    - LED ROJO     (GPIO 27): el último lote falló (se apaga con el siguiente envío exitoso).
    - LED integrado (GPIO 2): espejo del amarillo.
    - Al encender: autodiagnóstico (verde -> amarillo -> rojo).
    - Botón BOOT (GPIO 0): pausa / reanuda el envío (para no gastar la cuota de Free Edition).
    - (Opcional) Potenciómetro en GPIO 34: controla cada cuántos segundos se envía un lote (5 a 60 s).

  Librerías (Arduino IDE > Gestor de librerías): ArduinoJson (v7) de Benoit Blanchon.
  Placa: "ESP32 Dev Module" (paquete esp32 de Espressif).
*/

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <ArduinoJson.h>

// ===================== CONFIGURACIÓN (EDITAR) =====================
const char* WIFI_SSID        = "EDI";
const char* WIFI_PASS        = "12345678";
const char* DATABRICKS_HOST = "dbc-2e9d45b0-5357.cloud.databricks.com";  // sin https://
const char* DATABRICKS_TOKEN = "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXX";       // token personal (no lo publiques)
const char* WAREHOUSE_ID    = "415fe1a16c83e7ea";                         // ID del SQL Warehouse
const char* CATALOG         = "proyecto_nota3";                           // o "workspace" (lo indica la celda 00)

const int  EVENTOS_POR_LOTE   = 50;    // filas por INSERT (no subir de ~60: el texto SQL se copia varias veces en la RAM del ESP32)
const int  LOTES_POR_CICLO    = 4;     // INSERT por ciclo -> 4 x 50 = 200 eventos por ciclo
const int  INTERVALO_SEG      = 5;     // segundos mínimos entre ciclos (si no hay potenciómetro). Un ciclo tarda ~12 s, así que el envío es casi continuo
#define    USAR_POTENCIOMETRO   0      // 1 = leer GPIO 34 para ajustar el intervalo
// ===================================================================

const int PIN_LED      = 2;    // LED integrado (espejo del amarillo)
const int LED_VERDE    = 25;
const int LED_AMARILLO = 26;
const int LED_ROJO     = 27;
const int PIN_BOTON    = 0;
const int PIN_POT      = 34;

const char* TIPOS[]      = {"purchase", "purchase", "purchase", "click", "view", "cart", "refund"};  // más compras
const char* CATEGORIAS[] = {"ELECTRONICA", "HOGAR", "ROPA", "DEPORTES", "JUGUETERIA"};
const char* REGIONES[]   = {"NORTE", "SUR", "ESTE", "OESTE", "CENTRO"};

String   deviceId;
uint32_t sesion;           // identificador aleatorio por arranque (evita choques de event_id)
uint32_t contador = 0;
bool     enviando = true;
uint32_t ultimoEnvio = 0;
uint32_t lotesOk = 0, lotesError = 0;

void amarillo(bool on) {
  digitalWrite(LED_AMARILLO, on);
  digitalWrite(PIN_LED, on);
}

void parpadear(int pin, int veces, int ms) {
  for (int i = 0; i < veces; i++) {
    digitalWrite(pin, HIGH); delay(ms);
    digitalWrite(pin, LOW);  delay(ms);
  }
}

void autodiagnostico() {
  int leds[] = {LED_VERDE, LED_AMARILLO, LED_ROJO};
  for (int i = 0; i < 3; i++) { digitalWrite(leds[i], HIGH); delay(250); }
  delay(400);
  for (int i = 0; i < 3; i++) digitalWrite(leds[i], LOW);
}

// Verde: fijo = enviando, parpadeo 1 Hz = pausa, apagado = sin Wi-Fi (no bloqueante)
void actualizarVerde() {
  if (WiFi.status() != WL_CONNECTED) digitalWrite(LED_VERDE, LOW);
  else if (enviando)                  digitalWrite(LED_VERDE, HIGH);
  else                                digitalWrite(LED_VERDE, (millis() / 500) % 2);
}

void conectarWiFi() {
  if (WiFi.status() == WL_CONNECTED) return;
  Serial.printf("Conectando a %s", WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  uint32_t t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < 20000) {
    amarillo(true); delay(250); amarillo(false); delay(250);   // amarillo parpadeando = conectando
    Serial.print(".");
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.printf("\nWi-Fi OK. IP: %s\n", WiFi.localIP().toString().c_str());
  } else {
    Serial.println("\nNo se pudo conectar al Wi-Fi (se reintentará).");
  }
}

// Construye un INSERT con EVENTOS_POR_LOTE filas
String construirInsert() {
  String sql;
  sql.reserve(EVENTOS_POR_LOTE * 190 + 250);
  sql = "INSERT INTO bronze.eventos_raw "
        "(event_id, user_id, event_type, category, amount, quantity, region, "
        "event_timestamp, source, ingestion_time, device_id) VALUES ";

  char fila[220];
  for (int i = 0; i < EVENTOS_POR_LOTE; i++) {
    contador++;
    int    usuario  = random(1, 5001);
    const char* tipo = TIPOS[random(0, 7)];
    const char* cat  = CATEGORIAS[random(0, 5)];
    const char* reg  = REGIONES[random(0, 5)];
    float  monto    = random(1000, 46001) / 100.0;   // 10.00 a 460.00
    int    cantidad = random(1, 6);

    snprintf(fila, sizeof(fila),
             "%s('ESP-%08lX-%06lu','USR-%05d','%s','%s',%.2f,%d,'%s',current_timestamp(),'ESP32',current_timestamp(),'%s')",
             i == 0 ? "" : ",",
             (unsigned long)sesion, (unsigned long)contador, usuario, tipo, cat, monto, cantidad, reg,
             deviceId.c_str());
    sql += fila;
  }
  return sql;
}

// Envía una sentencia SQL al SQL Warehouse. Devuelve el estado (SUCCEEDED, PENDING, FAILED...).
String ejecutarSQL(const String& sql) {
  WiFiClientSecure cliente;
  cliente.setInsecure();   // simplifica el TLS para el proyecto académico (no valida el certificado)

  HTTPClient https;
  https.setTimeout(45000);
  String url = String("https://") + DATABRICKS_HOST + "/api/2.0/sql/statements";
  if (!https.begin(cliente, url)) return "ERROR_CONEXION";

  https.addHeader("Authorization", String("Bearer ") + DATABRICKS_TOKEN);
  https.addHeader("Content-Type", "application/json");

  JsonDocument req;
  req["warehouse_id"] = WAREHOUSE_ID;
  req["catalog"]      = CATALOG;
  req["statement"]    = sql;
  req["wait_timeout"] = "30s";   // si el warehouse está "frío", la sentencia sigue corriendo en segundo plano
  String cuerpo;
  serializeJson(req, cuerpo);

  int codigo = https.POST(cuerpo);
  String respuesta = https.getString();
  https.end();

  if (codigo != 200) {
    Serial.printf("HTTP %d: %s\n", codigo, respuesta.substring(0, 300).c_str());
    return "HTTP_" + String(codigo);
  }

  JsonDocument res;
  if (deserializeJson(res, respuesta)) return "JSON_INVALIDO";
  String estado = res["status"]["state"] | "DESCONOCIDO";
  if (estado == "FAILED") {
    Serial.printf("Error SQL: %s\n", (const char*)(res["status"]["error"]["message"] | ""));
  }
  return estado;
}

void setup() {
  Serial.begin(115200);
  pinMode(PIN_LED, OUTPUT);
  pinMode(LED_VERDE, OUTPUT);
  pinMode(LED_AMARILLO, OUTPUT);
  pinMode(LED_ROJO, OUTPUT);
  pinMode(PIN_BOTON, INPUT_PULLUP);
  autodiagnostico();
  randomSeed(esp_random());

  uint64_t mac = ESP.getEfuseMac();
  char id[24];
  snprintf(id, sizeof(id), "ESP32-%04X", (uint16_t)(mac >> 32));
  deviceId = id;
  sesion = esp_random();

  Serial.println("\n=== ESP32 #1 - Productor de eventos ===");
  Serial.printf("device_id: %s | sesión: %08lX\n", deviceId.c_str(), (unsigned long)sesion);
  conectarWiFi();
  Serial.println("Botón BOOT = pausar / reanudar.");
}

void loop() {
  // Botón BOOT: alterna pausa / envío
  static bool botonAnterior = HIGH;
  bool boton = digitalRead(PIN_BOTON);
  if (botonAnterior == HIGH && boton == LOW) {
    enviando = !enviando;
    Serial.println(enviando ? ">> Envío REANUDADO" : ">> Envío EN PAUSA");
    delay(250);   // antirrebote
  }
  botonAnterior = boton;

#if USAR_POTENCIOMETRO
  uint32_t intervalo = map(analogRead(PIN_POT), 0, 4095, 5, 60) * 1000UL;
#else
  uint32_t intervalo = INTERVALO_SEG * 1000UL;
#endif

  if (enviando && millis() - ultimoEnvio >= intervalo) {
    ultimoEnvio = millis();
    conectarWiFi();
    if (WiFi.status() != WL_CONNECTED) { digitalWrite(LED_ROJO, HIGH); return; }

    uint32_t t0 = millis();
    int insertOk = 0;
    String estado = "";
    for (int k = 0; k < LOTES_POR_CICLO; k++) {
      String sql = construirInsert();     // se libera al terminar cada vuelta (cuida la RAM)
      amarillo(true);                     // AMARILLO: transmitiendo a Databricks
      estado = ejecutarSQL(sql);
      amarillo(false);
      if (!(estado == "SUCCEEDED" || estado == "PENDING" || estado == "RUNNING")) break;
      insertOk++;
    }
    uint32_t ms = millis() - t0;

    if (insertOk == LOTES_POR_CICLO) {
      lotesOk++;
      Serial.printf("Ciclo #%lu enviado (%d eventos en %d INSERT, total %lu) -> %s en %lu ms\n",
                    (unsigned long)lotesOk, EVENTOS_POR_LOTE * LOTES_POR_CICLO, LOTES_POR_CICLO,
                    (unsigned long)contador, estado.c_str(), (unsigned long)ms);
      digitalWrite(LED_ROJO, LOW);        // ROJO apagado: último ciclo OK
    } else {
      lotesError++;
      Serial.printf("Falló el ciclo (%s) tras %d de %d INSERT. Errores acumulados: %lu\n",
                    estado.c_str(), insertOk, LOTES_POR_CICLO, (unsigned long)lotesError);
      digitalWrite(LED_ROJO, HIGH);       // ROJO encendido: último lote falló
      parpadear(LED_ROJO, 3, 80);
      digitalWrite(LED_ROJO, HIGH);
    }
  }
  actualizarVerde();
  delay(20);
}
