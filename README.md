# Biolingo HPK — field firmware and ingest server

Plant bioelectric recording with the Biolingo v22 board (ESP32-S3 + AD8232, Stefan Rentsch design),
uploading directly over WiFi to a small FastAPI server with a live view. Built for the
Herz-Pflanzen-Kommunikation (HPK) eurythmy field work; also used for the Aarau office experiments.

## Layout

- `firmware/biolingo_field_wifi/` — Arduino sketch for the node. Samples the AD8232 at 400 Hz
  and stores the **raw ADC readings** (int16 = mV × 10, 400 Hz): no notch, no low-pass, no
  decimation, so a railed reading is visible in the file as 0 or 3300 mV and all filtering is
  done offline where it can be undone (~70 MB/day per node). The 50 Hz notch is an 8-sample
  moving average (exactly one mains period at 400 Hz); apply it in analysis and keep a mask of
  outputs that include a railed reading. Files before 23 Oct 2026 are 100 Hz, notched and
  low-passed on the device; `sample_rate` in the WAV header says which. Writes 5-minute WAV chunks,
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

## BDAS classroom view (bdas.biolingo.org)

A simplified live page for teaching (Pflanzenkommunikation course, Biodynamische Ausbildung
Schweiz, Rheinau, Nov 2026). Shows only boxes named `biolingo-bdas-*`, one tile per sensor.

- `firmware/biolingo_bdas_wifi/` — the field firmware with two WiFi networks tried in order
  (`WIFI_SSID_1`/`_2` in `secrets.h`, see `secrets.h.example`). A box moves to the next network
  after 20 s without association, or after 3 failed uploads/heartbeats in a row (captive portal,
  no internet). The OLED shows the network in use. Flash six times with `NODE_NUM` 1..6
  (`biolingo-bdas-01` … `-06`); the plant-free dummy gets `ROLE "control"`.
- `server/bdas.html` — the page (English). Per sensor: live trace with mV axis, a **Stimulus**
  button that logs a marker (`/api/event`) and draws it on the trace, **Save** (upload the
  unfinished chunk now and switch storing on) and **Download** (ZIP of that sensor's last 12 h
  plus `markers.csv` and a README). Status badge and `rail %` = share of raw ADC samples at
  0/3300 mV (`sat` from the heartbeat; > 5 % shows "check electrode"). `?box=3` = one sensor
  alone, for a phone at the group's table.
- `server/server.py` — the "BDAS classroom view" block adds `/bdas`, `/api/events_recent`,
  `/api/bdas/files`, `/api/bdas/file/{id}`, `/api/bdas/zip`, `/api/bdas/save_now`. The
  `/api/bdas/*` routes only ever serve `biolingo-bdas-*` boxes.
- `server/update_bdas.py` — replaces (or inserts) that block in a deployed server script,
  with a timestamped backup: `sudo python3 update_bdas.py /opt/hpk/server_hpk.py block.py`.
- `server/nginx-bdas.conf.example` — vhost exposing only the page and the endpoints it needs;
  everything else stays behind the HPK basic auth.

Deploy: copy `bdas.html` next to the server script (read on every request, no restart needed
for page changes), restart `hpk` after server changes, then nginx + `certbot --nginx -d bdas.biolingo.org`.

## Electrode rules learned the hard way

- The AD8232 reference (third lead) must be on the **same object** as the two inputs:
  three pads gel-to-gel for a dummy, a third Ag/AgCl pad on the plant (stem base / petiole) for a plant.
  Reference open → output rails (0 or 3300 mV). Reference on building earth with inputs on a
  floating leaf → full-scale 50 Hz. Iron nail in soil → 0.5 V galvanic offset against Ag/AgCl pads.
- Two identical, fresh pads from the same pack. Mixed pad types = DC offset = rail.
- One box per plant. Two driven-reference boards on one plant fight each other (slow oscillation).
- `sat` in the heartbeat is the number to watch while placing electrodes: it must be ~0 %.
  A clean-looking filtered trace can hide a saturated ADC.
