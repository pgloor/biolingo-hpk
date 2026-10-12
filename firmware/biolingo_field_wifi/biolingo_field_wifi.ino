/*
 * biolingo_field_wifi.ino  —  HPK field firmware (Stefan's Biolingo v22 PCB)
 * ==================================================================
 * ESP32-S3-WROOM-1 + AD8232 (GPIO4) + SSD1306 OLED (SDA13/SCL12).
 * Records the plant bioelectric signal continuously and pushes complete
 * WAV chunks to hpk.biolingo.org over a phone hotspot. No BLE to the phone,
 * no Raspberry Pi gateway.
 *
 * Acquisition chain: 400 Hz sample, LP ~21 Hz, MAINS NOTCH (8-sample moving
 * average = exactly one 50 Hz period -> zero gain at 50/100/150/200 Hz),
 * HP ~0.1 Hz (computed, not stored), decimate 4x -> 100 Hz output, int16 mV*10.
 *
 * Sept 2026: the notch was added after Moritz's field tests. Without it ~23 %
 * of the mains hum (LP -9 dB, 4-pt average -4 dB) survived into the 100 Hz
 * stream, where 50 Hz sits exactly on Nyquist. Its beat against the sample
 * clock (grid drifts +/-0.05 Hz) produced the 20-60 s "oscillation" Moritz saw,
 * strongest on a person/near chargers and absent outdoors.
 *
 * Transport: HTTP multipart POST, shared token. Heartbeat every 30 s carries
 * status AND reads back server instructions: record (standby switch) and
 * flush (close the current partial chunk now, e.g. on End session so the
 * session tail is never lost to a power-off before the 5-minute boundary).
 *
 * Chunks buffer in a PSRAM queue for store-and-forward if the link drops.
 * ==================================================================
 */

#include <WiFi.h>
#include <WiFiClient.h>
#include <HTTPClient.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <time.h>
#include "esp_timer.h"

// =============== USER CONFIG ===============

// Phone hotspot (Moritz's phone). Confirmed working 18 July 2026.
#include "secrets.h"            // WIFI_SSID, WIFI_PASS, DEVICE_TOKEN - copy secrets.h.example, not in git
const char* ssid     = WIFI_SSID;
const char* password = WIFI_PASS;

#define SERVER_HOST "188.245.247.156"   // biolingo web host (hpk.biolingo.org)
#define SERVER_PORT 8080
#define UPLOAD_PATH "/upload"
#define STATUS_PATH "/api/heartbeat"

// Shared secret - must match BIOLINGO_TOKEN in the server's systemd unit.
// DEVICE_TOKEN comes from secrets.h

// Per-node identity.
//   NODE_NUM 0  -> the node names itself from its own chip ID, e.g.
//                  "biolingo-3f2a1c". EVERY BOARD IS THEN UNIQUE WITH NO EDIT.
//   NODE_NUM 1..99 -> fixed name "biolingo-field-01" etc. If you use this you
//                  MUST change the number on every board before uploading, or
//                  they all report as the same device and overwrite each other.
#define NODE_NUM      1                 // 0 = auto from chip ID (recommended)
#define ROLE          "demo_HPK"           // "plant" or "control" (labels the plant)

// A workshop session is a fixed 5-minute protocol (1 silence + 1 eurythmy +
// 1 silence + 2 cooldown), so one 5-minute chunk == one session.
#define CHUNK_SECONDS  300
#define QUEUE_DEPTH   120               // slots; actual count set by free memory

// -- Electrode check (advisory only) --
// Two earlier attempts to infer "no electrode" from the waveform (amplitude,
// then rail saturation) both mis-fired on good plant recordings. Only a
// completely FLAT signal is flagged now; saturation is reported, not judged.
#define REQUIRE_ELECTRODE  0            // 0 = always record (default)
#define SAT_MARGIN          2           // ADC counts from 0/4095 counted as pinned (reporting only)
#define P2P_MIN_MV         0.2          // essentially no movement = dead / shorted
#define IDLE_AFTER_S       60           // sustained flag before pausing (only if REQUIRE_ELECTRODE)
#define GATE_WINDOW_MS   2000           // measurement window
#define ELECTRODE_LO_PLUS   -1          // AD8232 LO+ GPIO, -1 = not wired
#define ELECTRODE_LO_MINUS  -1          // AD8232 LO- GPIO, -1 = not wired

// =========== ACQUISITION ===========
// 380/100 in C integer arithmetic is 3, not 4 (Stefan's "~4" comment was
// wrong), giving 126.7 Hz while headers claimed 100 Hz. 400/4 = 100 exactly
// and 1000000/400 = 2500 us exactly, so no rounding either.
const int   SAMPLE_RATE        = 400;
const int   OUTPUT_RATE        = 100;
const int   SAMPLE_INTERVAL_US = 1000000 / SAMPLE_RATE;       // 2500 exactly
const int   DECIMATION_FACTOR  = SAMPLE_RATE / OUTPUT_RATE;   // 4 exactly
static_assert(SAMPLE_RATE % OUTPUT_RATE == 0,
              "SAMPLE_RATE must be an exact multiple of OUTPUT_RATE");
static_assert(1000000 % SAMPLE_RATE == 0,
              "1000000/SAMPLE_RATE must be a whole number of microseconds");

const int   ADC_PIN            = 4;     // Stefan S3
const int   ADC_RESOLUTION     = 12;
const float ADC_VOLTAGE_REF    = 3.3;

const float LP_ALPHA = 0.25;            // ~21 Hz
const float HP_ALPHA = 0.998;           // ~0.1 Hz
float lpFilterState = 0.0, hpFilterState = 0.0, hpPrevInput = 0.0;

// -- Mains notch: moving average over exactly one mains period --
// A box filter of length SAMPLE_RATE/MAINS_HZ has nulls at every multiple of
// MAINS_HZ, so 50 Hz and all its harmonics are removed BEFORE decimation and
// cannot alias onto Nyquist. Passband: -1 dB at 10 Hz, -4 dB at 20 Hz - far
// above anything the plant produces. Costs one add, one subtract, one multiply.
const int   MAINS_HZ  = 50;
const int   NOTCH_N   = SAMPLE_RATE / MAINS_HZ;                 // 8
static_assert(SAMPLE_RATE % MAINS_HZ == 0,
              "SAMPLE_RATE must be an exact multiple of MAINS_HZ for the notch");
static float notchBuf[NOTCH_N];
static int   notchIdx = 0;
static float notchSum = 0.0;
static float applyMainsNotch(float x) {
  notchSum += x - notchBuf[notchIdx];
  notchBuf[notchIdx] = x;
  if (++notchIdx >= NOTCH_N) notchIdx = 0;
  return notchSum / NOTCH_N;
}
int   decimationCounter = 0;
float decimationAccumulator = 0.0;

// =============== OLED ===============
#define SCREEN_WIDTH 128
#define SCREEN_HEIGHT 64
#define SCREEN_ADDRESS 0x3C
Adafruit_SSD1306 display(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, -1);

// =============== chunk queue ===============
static const size_t   WAV_HDR   = 44;
static const uint32_t FRAMES    = (uint32_t)OUTPUT_RATE * CHUNK_SECONDS;
static const uint32_t PCM_BYTES = FRAMES * 2;
static const size_t   CHUNK_SZ  = WAV_HDR + PCM_BYTES;

struct Chunk { uint8_t* buf; uint32_t startMillis; bool full; uint32_t partialFrames; };
static Chunk    qbuf[QUEUE_DEPTH];
static int      qDepth = 0;
static volatile int qCount = 0, qHead = 0, qTail = 0;
static volatile uint32_t frameCount = 0;
static volatile bool overflowed = false;
static portMUX_TYPE qSpin = portMUX_INITIALIZER_UNLOCKED;

// live telemetry for the field GUI / OLED
static volatile float lastMv = 0.0, winMin = 1e9, winMax = -1e9, p2p = 0.0;

// electrode check state
static volatile bool  recording = true;     // actually archiving right now
static volatile bool  armed     = true;     // what the server last told us to do
static volatile bool  flushReq  = false;    // server asked us to close the partial chunk now
static volatile bool  suspect   = false;    // advisory: signal looks flat
static volatile float gateMin = 1e9, gateMax = -1e9, gateP2p = 0.0, satFrac = 0.0;
static volatile uint32_t satCount = 0, sampCount = 0;
static uint32_t badSince = 0;

// live preview trace (separate from the WAV chunks - only for the GUI)
static const int LIVE_CAP = 40;
static int16_t liveBuf[LIVE_CAP];
static volatile int liveCount = 0, liveDecCnt = 0;

static String   devId;
static uint32_t chunksSent = 0;
static String   wifiState = "Connecting";

// NTP offset: epoch = ntpEpoch + (millis() - ntpMillis)/1000
static time_t   ntpEpoch  = 0;
static uint32_t ntpMillis = 0;
static bool     timeValid = false;

// =============== helpers ===============
static float adcToVoltage(int adc) { return (adc / 4095.0) * ADC_VOLTAGE_REF * 1000.0; }
static float applyLowpass(float x)  { lpFilterState = LP_ALPHA * x + (1.0 - LP_ALPHA) * lpFilterState; return lpFilterState; }
static float applyHighpass(float x) { float o = HP_ALPHA * (hpFilterState + x - hpPrevInput); hpFilterState = o; hpPrevInput = x; return o; }

static time_t epochFor(uint32_t ms) {
  if (!timeValid) return 0;
  return ntpEpoch + (time_t)((int32_t)(ms - ntpMillis) / 1000);
}

// Header for a chunk that is only partially filled (used on flush): the data
// size must match the frames actually recorded, not the full 5-minute size.
static void writeWavHeaderN(uint8_t* b, uint32_t frames) {
  uint32_t pcm = frames * 2;
  uint32_t byteRate = (uint32_t)OUTPUT_RATE * 2, riff = 36 + pcm, sub1 = 16, sr = OUTPUT_RATE;
  uint16_t fmt = 1, ch = 1, align = 2, bps = 16;
  memcpy(b, "RIFF", 4);      memcpy(b + 4, &riff, 4);   memcpy(b + 8, "WAVE", 4);
  memcpy(b + 12, "fmt ", 4); memcpy(b + 16, &sub1, 4);  memcpy(b + 20, &fmt, 2);
  memcpy(b + 22, &ch, 2);    memcpy(b + 24, &sr, 4);    memcpy(b + 28, &byteRate, 4);
  memcpy(b + 32, &align, 2); memcpy(b + 34, &bps, 2);
  memcpy(b + 36, "data", 4); memcpy(b + 40, &pcm, 4);
}

static void writeWavHeader(uint8_t* b) { writeWavHeaderN(b, FRAMES); }

// =============== sampling - esp_timer ===============
static esp_timer_handle_t sampleTimer = nullptr;

static void sampleCb(void*) {
  int   rawAdc = analogRead(ADC_PIN);
  if (rawAdc <= SAT_MARGIN || rawAdc >= 4095 - SAT_MARGIN) satCount++;
  sampCount++;
  float rawMv = adcToVoltage(rawAdc);
  float lpMv  = applyMainsNotch(applyLowpass(rawMv));   // LP, then kill 50 Hz + harmonics
  float hp    = applyHighpass(lpMv); (void)hp;    // computed, not output (as Stefan)

  lastMv = lpMv;
  if (lpMv < winMin) winMin = lpMv;
  if (lpMv > winMax) winMax = lpMv;
  if (lpMv < gateMin) gateMin = lpMv;
  if (lpMv > gateMax) gateMax = lpMv;

  decimationAccumulator += lpMv;
  if (++decimationCounter < DECIMATION_FACTOR) return;

  float dec = decimationAccumulator / DECIMATION_FACTOR;
  decimationAccumulator = 0.0; decimationCounter = 0;

  int32_t v = (int32_t)roundf(dec * 10.0);
  if (v >  32767) v =  32767;
  if (v < -32768) v = -32768;
  int16_t sm = (int16_t)v;

  portENTER_CRITICAL_ISR(&qSpin);
  Chunk& c = qbuf[qTail];
  if (recording && c.buf && !c.full) {
    if (frameCount == 0) c.startMillis = millis();
    uint8_t* d = c.buf + WAV_HDR + (size_t)frameCount * 2;
    d[0] = (uint8_t)(sm & 0xFF); d[1] = (uint8_t)((sm >> 8) & 0xFF);
    if (++frameCount >= FRAMES) {
      writeWavHeader(c.buf);
      c.full = true; c.partialFrames = FRAMES; qCount++; frameCount = 0;
      int nxt = (qTail + 1) % qDepth;
      if (qbuf[nxt].full || !qbuf[nxt].buf) overflowed = true;
      else qTail = nxt;
    }
  }
  if (++liveDecCnt >= 8) {                 // 100 Hz / 8 = 12.5 Hz preview
    liveDecCnt = 0;
    if (liveCount < LIVE_CAP) liveBuf[liveCount++] = sm;
  }
  portEXIT_CRITICAL_ISR(&qSpin);
}

// =========== WiFi - non-blocking ===========
static uint32_t lastWifiCheck = 0;
static uint32_t disconnectedSince = 0;
const uint32_t  WIFI_CHECK_INTERVAL   = 5000;
const uint32_t  WIFI_FORCE_RETRY_MS   = 30000;

static void startWiFi() {
  Serial.printf("[wifi] connecting to '%s' (non-blocking)\n", ssid);
  wifiState = "Connecting";
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.begin(ssid, password);
  WiFi.setSleep(false);                  // Phaenomena lesson
}

static void checkWiFiAndReconnect() {
  wl_status_t st = WiFi.status();
  if (st == WL_CONNECTED) {
    if (wifiState != "Connected") {
      wifiState = "Connected";
      Serial.printf("[wifi] up - IP %s  RSSI %d dBm\n",
                    WiFi.localIP().toString().c_str(), WiFi.RSSI());
    }
    disconnectedSince = 0;
    return;
  }
  if (disconnectedSince == 0) {
    disconnectedSince = millis();
    Serial.println("[wifi] link down - letting the stack retry");
  }
  wifiState = "Reconnecting";
  // Only force a fresh association if auto-reconnect has been getting nowhere;
  // hammering WiFi.begin() every check restarts an in-flight attempt and can
  // stop it ever completing on a slow phone hotspot.
  if (millis() - disconnectedSince > WIFI_FORCE_RETRY_MS) {
    Serial.println("[wifi] forcing fresh association");
    WiFi.disconnect(true);
    delay(200);
    WiFi.begin(ssid, password);
    disconnectedSince = millis();
  }
}

static void trySyncTime() {
  if (timeValid || WiFi.status() != WL_CONNECTED) return;
  configTime(0, 0, "pool.ntp.org", "time.google.com");
  time_t t = 0; time(&t);
  if (t > 1700000000) {
    ntpEpoch = t; ntpMillis = millis(); timeValid = true;
    Serial.printf("[ntp] locked, epoch %ld\n", (long)t);
  }
}

// =============== upload ===============
static bool uploadChunk(uint8_t* wav, size_t wavLen, time_t startEpoch) {
  if (WiFi.status() != WL_CONNECTED) return false;
  const char* bnd = "----biolingoBoundary7MA4YWxk";
  char fname[96]; snprintf(fname, sizeof(fname), "%s_%ld.wav", devId.c_str(), (long)startEpoch);

  String head = String("--") + bnd + "\r\nContent-Disposition: form-data; name=\"device\"\r\n\r\n" + devId + "\r\n"
    "--" + bnd + "\r\nContent-Disposition: form-data; name=\"role\"\r\n\r\n" + ROLE + "\r\n"
    "--" + bnd + "\r\nContent-Disposition: form-data; name=\"sample_rate\"\r\n\r\n" + String(OUTPUT_RATE) + "\r\n"
    "--" + bnd + "\r\nContent-Disposition: form-data; name=\"channels\"\r\n\r\n1\r\n"
    "--" + bnd + "\r\nContent-Disposition: form-data; name=\"units\"\r\n\r\nmV_x10\r\n"
    "--" + bnd + "\r\nContent-Disposition: form-data; name=\"start_epoch\"\r\n\r\n" + String((long)startEpoch) + "\r\n"
    "--" + bnd + "\r\nContent-Disposition: form-data; name=\"token\"\r\n\r\n" + DEVICE_TOKEN + "\r\n"
    "--" + bnd + "\r\nContent-Disposition: form-data; name=\"file\"; filename=\"" + fname + "\"\r\n"
    "Content-Type: audio/wav\r\n\r\n";
  String tail = String("\r\n--") + bnd + "--\r\n";

  WiFiClient client;
  client.setTimeout(15000);
  if (!client.connect(SERVER_HOST, SERVER_PORT)) return false;

  client.printf("POST %s HTTP/1.1\r\nHost: %s\r\n", UPLOAD_PATH, SERVER_HOST);
  client.printf("Content-Type: multipart/form-data; boundary=%s\r\n", bnd);
  client.printf("Content-Length: %u\r\nConnection: close\r\n\r\n",
                (unsigned)(head.length() + wavLen + tail.length()));
  client.print(head);
  size_t sent = 0;
  while (sent < wavLen) {
    size_t n = min((size_t)1460, wavLen - sent);
    if (client.write(wav + sent, n) != n) { client.stop(); return false; }
    sent += n;
  }
  client.print(tail);

  bool ok = false; uint32_t t0 = millis();
  while (millis() - t0 < 15000) {
    if (client.available()) {
      String line = client.readStringUntil('\n');
      ok = (line.indexOf(" 200 ") > 0 || line.indexOf(" 201 ") > 0);
      break;
    }
    delay(10);
  }
  client.stop();
  return ok;
}

// The server answers each heartbeat with {"record":..,"flush":..}. record is
// the human standby switch; flush asks us to close the current partial chunk.
static void sendHeartbeat() {
  if (WiFi.status() != WL_CONNECTED) return;
  p2p = (winMax > winMin) ? (winMax - winMin) : 0.0;
  winMin = 1e9; winMax = -1e9;                    // reset window

  HTTPClient http; char url[280];
  snprintf(url, sizeof(url),
           "http://%s:%d%s?device=%s&role=%s&rate=%d&ch=1&queue=%d&rssi=%d&mv=%.1f&p2p=%.1f&fill=%lu&state=%s&sat=%.1f&token=%s",
           SERVER_HOST, SERVER_PORT, STATUS_PATH, devId.c_str(), ROLE, OUTPUT_RATE,
           qCount, WiFi.RSSI(), lastMv, p2p, (unsigned long)(100UL * frameCount / FRAMES),
           recording ? (suspect ? "flat" : "rec") : "idle",
           satFrac * 100.0, DEVICE_TOKEN);
  http.begin(url);
  int code = http.GET();
  if (code == 200) {
    String body = http.getString();
    bool want = (body.indexOf("\"record\":true") >= 0);
    if (want != armed) {
      armed = want;
      portENTER_CRITICAL(&qSpin);
      recording = want;
      frameCount = 0;                    // drop the partial chunk either way
      portEXIT_CRITICAL(&qSpin);
      Serial.printf("[server] %s storing\n", want ? "START" : "STOP");
    }
    if (body.indexOf("\"flush\":true") >= 0) flushReq = true;
  }
  http.end();
}

// Close the chunk currently being filled (however short), seal it as a valid
// WAV, and hand it to the upload queue. Called when the server sets flush
// (e.g. End session) so the session tail does not wait for the 5-minute
// boundary or vanish on power-off.
static void flushPartialChunk() {
  portENTER_CRITICAL(&qSpin);
  uint32_t n = frameCount;
  Chunk& c = qbuf[qTail];
  bool ok = (c.buf && !c.full && n > 0);
  if (ok) {
    writeWavHeaderN(c.buf, n);
    c.partialFrames = n;
    c.full = true; qCount++; frameCount = 0;
    int nxt = (qTail + 1) % qDepth;
    if (qbuf[nxt].full || !qbuf[nxt].buf) overflowed = true;
    else qTail = nxt;
  }
  portEXIT_CRITICAL(&qSpin);
  if (ok) Serial.printf("[flush] sealed partial chunk: %lu frames (%.1fs)\n",
                        (unsigned long)n, n / (float)OUTPUT_RATE);
}

static void sendLive() {
  if (WiFi.status() != WL_CONNECTED || liveCount == 0) return;
  int16_t tmp[LIVE_CAP]; int n;
  portENTER_CRITICAL(&qSpin);
  n = liveCount; memcpy(tmp, liveBuf, n * sizeof(int16_t)); liveCount = 0;
  portEXIT_CRITICAL(&qSpin);

  String csv; csv.reserve(n * 7);
  for (int i = 0; i < n; i++) { if (i) csv += ','; csv += String(tmp[i]); }

  String body = "device=" + devId + "&token=" + DEVICE_TOKEN +
                "&t_end=" + String((long)epochFor(millis())) + "&samples=" + csv;

  HTTPClient http; char url[128];
  snprintf(url, sizeof(url), "http://%s:%d/api/live_push", SERVER_HOST, SERVER_PORT);
  http.begin(url);
  http.addHeader("Content-Type", "application/x-www-form-urlencoded");
  http.setTimeout(3000);
  http.POST(body);
  http.end();
}

// =============== electrode check (advisory) ===============
static void updateGate() {
  float lo, hi; uint32_t sat, tot;
  portENTER_CRITICAL(&qSpin);
  lo = gateMin; hi = gateMax; sat = satCount; tot = sampCount;
  gateMin = 1e9; gateMax = -1e9; satCount = 0; sampCount = 0;
  portEXIT_CRITICAL(&qSpin);
  if (hi < lo || tot == 0) return;

  gateP2p = hi - lo;
  satFrac = (float)sat / (float)tot;

  bool leadOff = false;
#if ELECTRODE_LO_PLUS >= 0
  leadOff |= (digitalRead(ELECTRODE_LO_PLUS) == HIGH);
#endif
#if ELECTRODE_LO_MINUS >= 0
  leadOff |= (digitalRead(ELECTRODE_LO_MINUS) == HIGH);
#endif

  // Flat means flat: no movement at all. Saturation is reported but not used to
  // judge - a real signal can touch the rails and a disconnected one may not.
  bool bad = leadOff || gateP2p < P2P_MIN_MV;
  suspect = bad;

  uint32_t now = millis();
  if (!bad) {
    badSince = 0;
    if (!recording && armed) {
      portENTER_CRITICAL(&qSpin);
      frameCount = 0; recording = true;
      portEXIT_CRITICAL(&qSpin);
      Serial.println("[check] signal present -> RECORDING");
    }
    return;
  }
  if (badSince == 0) {
    badSince = now;
    Serial.printf("[check] FLAT signal: p2p %.1f mV (sat %.0f%%)%s\n",
                  gateP2p, satFrac * 100.0, leadOff ? ", lead-off" : "");
  }
#if REQUIRE_ELECTRODE
  if (recording && (now - badSince) > (uint32_t)IDLE_AFTER_S * 1000UL) {
    portENTER_CRITICAL(&qSpin);
    recording = false; frameCount = 0;
    portEXIT_CRITICAL(&qSpin);
    Serial.println("[check] pausing archive (REQUIRE_ELECTRODE=1)");
  }
#endif
}

// =============== OLED ===============
static void updateDisplay() {
  display.clearDisplay(); display.setTextSize(1); display.setTextColor(SSD1306_WHITE);
  display.setCursor(0, 0);  display.println(devId);
  display.setCursor(0, 12);
  if (WiFi.status() == WL_CONNECTED) { display.print("IP:"); display.println(WiFi.localIP().toString()); }
  else                               { display.print("WiFi: "); display.println(wifiState); }
  display.setCursor(0, 24); display.printf("Sent:%lu Q:%d%s", chunksSent, qCount, overflowed ? "!" : "");
  display.setCursor(0, 36);
  if (!armed)          display.print("STANDBY not storing");
  else if (!recording) display.print("PAUSED");
  else if (suspect)    display.printf("REC %lu%%  FLAT!", (unsigned long)(100UL * frameCount / FRAMES));
  else                 display.printf("REC  %lu%%", (unsigned long)(100UL * frameCount / FRAMES));
  display.setCursor(0, 48); display.printf("V: %.1f mV", lastMv);
  display.setCursor(0, 56); display.printf("p2p: %.1f %s", gateP2p, timeValid ? "" : "NOTIME");
  display.display();
}

// =============== setup ===============
void setup() {
  Serial.begin(115200); delay(2000);
  char idbuf[40];
#if NODE_NUM == 0
  uint64_t chip = ESP.getEfuseMac();               // unique per ESP32, from the factory
  snprintf(idbuf, sizeof(idbuf), "biolingo-demo_HPK-%06x", (unsigned)(chip & 0xFFFFFF));
#else
  snprintf(idbuf, sizeof(idbuf), "biolingo-demo_HPK-%02d", NODE_NUM);
#endif
  devId = String(idbuf);
  Serial.printf("\n[boot] %s (%s)  chunk %d s @ %d Hz = %u B\n",
                devId.c_str(), ROLE, CHUNK_SECONDS, OUTPUT_RATE, (unsigned)CHUNK_SZ);

  Wire.begin(13, 12);                                   // Stefan's OLED pins
  if (!display.begin(SSD1306_SWITCHCAPVCC, SCREEN_ADDRESS)) Serial.println("[oled] not found");
  display.clearDisplay(); display.setTextColor(SSD1306_WHITE); display.setTextSize(1);
  display.setCursor(0, 0); display.println("Biolingo field"); display.println("booting..."); display.display();

  analogReadResolution(ADC_RESOLUTION);
  analogSetAttenuation(ADC_11db);
  pinMode(ADC_PIN, INPUT);
#if ELECTRODE_LO_PLUS >= 0
  pinMode(ELECTRODE_LO_PLUS, INPUT);
#endif
#if ELECTRODE_LO_MINUS >= 0
  pinMode(ELECTRODE_LO_MINUS, INPUT);
#endif

  // Fill as many queue slots as memory allows, keeping a reserve for WiFi/HTTP.
  size_t psram = ESP.getPsramSize();
  const size_t RESERVE = psram ? 65536 : 90000;
  Serial.printf("[mem] PSRAM %u bytes, heap %u bytes, chunk %u bytes\n",
                (unsigned)psram, (unsigned)ESP.getFreeHeap(), (unsigned)CHUNK_SZ);

  for (int i = 0; i < QUEUE_DEPTH; i++) { qbuf[i].buf = nullptr; qbuf[i].full = false; qbuf[i].partialFrames = 0; }
  qDepth = 0;
  for (int i = 0; i < QUEUE_DEPTH; i++) {
    size_t avail = psram ? ESP.getFreePsram() : ESP.getFreeHeap();
    if (avail < CHUNK_SZ + RESERVE) break;
    uint8_t* b = (uint8_t*)(psram ? ps_malloc(CHUNK_SZ) : malloc(CHUNK_SZ));
    if (!b) break;
    qbuf[i].buf = b; qDepth = i + 1;
  }
  if (!qbuf[0].buf || qDepth < 1) {
    Serial.println("FATAL: buffer alloc failed. Reduce CHUNK_SECONDS or enable PSRAM.");
    display.clearDisplay(); display.setCursor(0, 0);
    display.println("FATAL"); display.println("no RAM for buffer");
    display.printf("need %uKB\n", (unsigned)(CHUNK_SZ / 1024));
    display.printf("free %uKB\n", (unsigned)(ESP.getFreeHeap() / 1024));
    display.display();
    while (true) delay(1000);
  }
  Serial.printf("[mem] queue %d chunks = %d min offline backlog\n",
                qDepth, qDepth * CHUNK_SECONDS / 60);

  startWiFi();                       // non-blocking: sampling starts regardless

  float v0 = adcToVoltage(analogRead(ADC_PIN));
  lpFilterState = v0; hpPrevInput = v0; hpFilterState = 0.0;
  for (int i = 0; i < NOTCH_N; i++) notchBuf[i] = v0;   // prime the notch so it starts settled
  notchSum = v0 * NOTCH_N; notchIdx = 0;

  lastWifiCheck = millis();
  const esp_timer_create_args_t targs = {
    .callback = &sampleCb, .arg = nullptr,
    .dispatch_method = ESP_TIMER_TASK, .name = "sample"
  };
  esp_timer_create(&targs, &sampleTimer);
  esp_timer_start_periodic(sampleTimer, SAMPLE_INTERVAL_US);
  Serial.printf("[boot] sampling started at %d Hz (%d us)\n", SAMPLE_RATE, SAMPLE_INTERVAL_US);
}

// =============== loop ===============
void loop() {
  static uint32_t lastHb = 0, lastDisp = 0, lastLive = 0, lastGate = 0;

  if (millis() - lastWifiCheck >= WIFI_CHECK_INTERVAL) {
    lastWifiCheck = millis();
    checkWiFiAndReconnect();
    trySyncTime();
  }
  if (millis() - lastGate > GATE_WINDOW_MS) { updateGate(); lastGate = millis(); }
  // 10 s (was 30 s): the heartbeat is how the server tells the box to flush its
  // partial chunk on End session, so a shorter interval means the session tail
  // uploads within ~10 s instead of up to 30 s. A heartbeat is a few hundred
  // bytes — trivial for the phone hotspot, and the boxes only run during use.
  if (millis() - lastHb   > 10000) { sendHeartbeat();  lastHb   = millis(); }
  if (millis() - lastLive > 2000 && timeValid) { sendLive(); lastLive = millis(); }
  if (millis() - lastDisp > 1000)  { updateDisplay();  lastDisp = millis(); }

  // server asked us to save the partial chunk now (End session / manual flush)
  if (flushReq) { flushReq = false; flushPartialChunk(); }

  // flush queue oldest-first; only once we can stamp a real wall-clock time
  if (qCount > 0 && timeValid && WiFi.status() == WL_CONNECTED) {
    Chunk& c = qbuf[qHead];
    if (c.full) {
      time_t ep = epochFor(c.startMillis);
      size_t wavLen = WAV_HDR + (size_t)c.partialFrames * 2;   // real length (full or partial)
      Serial.printf("[tx] chunk epoch %ld (%lu frames, queue %d)\n",
                    (long)ep, (unsigned long)c.partialFrames, qCount);
      if (uploadChunk(c.buf, wavLen, ep)) {
        portENTER_CRITICAL(&qSpin);
        c.full = false; qCount--; qHead = (qHead + 1) % qDepth;
        if (overflowed) { qTail = (qTail + 1) % qDepth; overflowed = false; }
        portEXIT_CRITICAL(&qSpin);
        chunksSent++;
      } else {
        Serial.println("[tx] failed - will retry");
        delay(3000);
      }
    }
  }
  delay(20);
}
