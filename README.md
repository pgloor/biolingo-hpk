# Biolingo HPK — field firmware and ingest server

Plant bioelectric recording with the Biolingo v22 board (ESP32-S3 + AD8232, Stefan Rentsch design),
uploading directly over WiFi to a small FastAPI server with a live view. Built for the
Herz-Pflanzen-Kommunikation (HPK) eurythmy field work; also used for the Aarau office experiments.

## Layout

- `firmware/biolingo_field_wifi/` — Arduino sketch for the node. Samples the AD8232 at 400 Hz,
  8-sample moving average as 50 Hz notch (exactly one mains period, so 50/100/150 Hz are nulled),
  and stores the full 400 Hz (int16 = mV × 10) — no low-pass, no decimation, so nothing is
  thrown away before analysis (~70 MB/day per node). Files before 23 Oct 2026 are 100 Hz with
  an ~18 Hz low-pass; `sample_rate` in the WAV header says which. Writes 5-minute WAV chunks,
  uploads them by HTTP POST, pushes a
  12.5 Hz live preview every 2 s, heartbeat every 10 s with electrode diagnostics
  (`mv`, `p2p`, `sat` = fraction of raw ADC samples at a rail). PSRAM store-and-forward queue,
  so nothing is lost while WiFi or the server is down. Per-node `NODE_NUM` and `ROLE`.
- `server/server.py` — single-file FastAPI ingest + GUI: `/upload`, `/api/heartbeat`,
  `/api/live_push` (device side), `/api/live`, `/api/status`, sessions, Polar H10 HR,
  `/browse` file listing. Data in `BIOLINGO_DATA` (`<device>/<device>_<UTC>_<epoch>.wav`
  plus `biolingo.sqlite`).
- `tools/sat.py` — electrode health from the server's status endpoint.

## Node setup

1. `cp firmware/biolingo_field_wifi/secrets.h.example firmware/biolingo_field_wifi/secrets.h`
   and fill in a **2.4 GHz** SSID, password and the shared token.
2. Set `NODE_NUM` (1..99, unique per board, or 0 = from chip ID) and `ROLE` in the sketch.
3. Arduino IDE, board ESP32S3 Dev Module; libraries Adafruit GFX + SSD1306. Upload.
4. Serial shows `[wifi] connected`, `[ntp] locked`; the node appears in the GUI within 10 s.

## Server setup (Ubuntu)

    sudo useradd -r -m -d /opt/hpk hpk
    sudo -u hpk python3 -m venv /opt/hpk/venv
    sudo -u hpk /opt/hpk/venv/bin/pip install -r server/requirements.txt
    sudo cp server/server.py /opt/hpk/
    sudo cp server/hpk.service.example /etc/systemd/system/hpk.service   # set BIOLINGO_TOKEN
    sudo systemctl enable --now hpk

The device endpoints listen on 0.0.0.0:8080 (plain HTTP, token-protected); put nginx with TLS and
basic auth in front for the GUI (`/`, `/api/live`, `/api/status`, `/browse`), leave
`/upload`, `/api/heartbeat` and `/api/live_push` reachable on 8080.

## Electrode rules learned the hard way

- The AD8232 reference (third lead) must be on the **same object** as the two inputs:
  three pads gel-to-gel for a dummy, a third Ag/AgCl pad on the plant (stem base / petiole) for a plant.
  Reference open → output rails (0 or 3300 mV). Reference on building earth with inputs on a
  floating leaf → full-scale 50 Hz. Iron nail in soil → 0.5 V galvanic offset against Ag/AgCl pads.
- Two identical, fresh pads from the same pack. Mixed pad types = DC offset = rail.
- One box per plant. Two driven-reference boards on one plant fight each other (slow oscillation).
- `sat` in the heartbeat is the number to watch while placing electrodes: it must be ~0 %.
  A clean-looking filtered trace can hide a saturated ADC.
