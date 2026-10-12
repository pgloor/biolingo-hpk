#!/usr/bin/env python3
"""
Biolingo field ingest server (HPK / eurythmy experiments)
=========================================================
Single-file FastAPI app. Receives 3-5 min WAV chunks from the ESP32-S3
field nodes (over Moritz's phone hotspot), logs eurythmy gesture markers
and Polar H10 HR data, and serves a mobile GUI Moritz uses in the field.

Everything is timestamped against server wall-clock (NTP-synced), so the
plant WAV, the gesture events and the HR stream all share one timebase.

Run:
    pip install fastapi "uvicorn[standard]" python-multipart
    python server.py            # listens on 0.0.0.0:8080

Storage:
    ./biolingo_data/<device>/<file>.wav      raw WAV chunks
    ./biolingo_data/biolingo.sqlite          metadata / sessions / events
"""

import os, io, csv, json, time, wave, sqlite3, contextlib
from datetime import datetime, timezone
from pathlib import Path

from fastapi import (FastAPI, UploadFile, File, Form, Request, WebSocket,
                     WebSocketDisconnect, HTTPException)
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, PlainTextResponse

# ---------------- config ----------------
HOST = "0.0.0.0"
PORT = 8080
DATA_DIR = Path(os.environ.get("BIOLINGO_DATA", "./biolingo_data")).resolve()
# Shared secret for the ESP32 device endpoints (/upload, /api/heartbeat).
# Set BIOLINGO_TOKEN in the systemd unit. Empty = no auth (local testing only).
DEVICE_TOKEN = os.environ.get("BIOLINGO_TOKEN", "")
DB_PATH = DATA_DIR / "biolingo.sqlite"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Seed gestures for the field pad. These are the ones Moritz has used;
# anything else is added in the GUI and stored in the database, so it is
# still there after a page refresh or a phone restart.
DEFAULT_GESTURES = ["A", "G", "D"]

# ---------------- db ----------------
def db():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

# Columns added after the first deployment. CREATE TABLE IF NOT EXISTS does
# nothing to a table that already exists, so a database created by an older
# version keeps the old shape and every INSERT naming a new column fails.
# These are applied with ALTER TABLE on every start — idempotent and cheap.
LATER_COLUMNS = {
    "files":      {"role": "TEXT DEFAULT ''", "units": "TEXT DEFAULT ''"},
    "heartbeats": {"queue": "INTEGER DEFAULT 0", "rssi": "INTEGER DEFAULT 0",
                   "role": "TEXT DEFAULT ''", "mv": "REAL DEFAULT 0",
                   "p2p": "REAL DEFAULT 0", "fill": "INTEGER DEFAULT 0",
                   "state": "TEXT DEFAULT 'rec'", "sat": "REAL DEFAULT 0", "armed": "INTEGER DEFAULT 1",
                   "flush_req": "INTEGER DEFAULT 0"},
}


def migrate(c):
    for table, cols in LATER_COLUMNS.items():
        try:
            have = {r[1] for r in c.execute(f"PRAGMA table_info({table})")}
        except sqlite3.Error:
            continue
        if not have:
            continue                      # table not created yet; schema will make it
        for name, decl in cols.items():
            if name not in have:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
                print(f"[migrate] {table}.{name} added")


def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS files(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device TEXT, path TEXT, start_epoch REAL, sample_rate INTEGER,
            channels INTEGER, duration REAL, bytes INTEGER,
            session_id INTEGER, received_epoch REAL, role TEXT DEFAULT '', units TEXT DEFAULT '');
        CREATE TABLE IF NOT EXISTS sessions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT, device TEXT, start_epoch REAL, stop_epoch REAL);
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER, device TEXT, gesture TEXT, rep INTEGER,
            phase TEXT, server_epoch REAL, client_epoch REAL, note TEXT);
        CREATE TABLE IF NOT EXISTS hr(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER, device TEXT, epoch REAL, hr REAL, rr REAL);
        CREATE TABLE IF NOT EXISTS gestures(
            name TEXT PRIMARY KEY, added_epoch REAL);
        CREATE TABLE IF NOT EXISTS heartbeats(
            device TEXT PRIMARY KEY, last_seen REAL, sample_rate INTEGER, channels INTEGER,
            queue INTEGER DEFAULT 0, rssi INTEGER DEFAULT 0, role TEXT DEFAULT '',
            mv REAL DEFAULT 0, p2p REAL DEFAULT 0, fill INTEGER DEFAULT 0,
            state TEXT DEFAULT 'rec', sat REAL DEFAULT 0, armed INTEGER DEFAULT 1,
            flush_req INTEGER DEFAULT 0);
        """)
        migrate(c)
        if not c.execute("SELECT 1 FROM gestures LIMIT 1").fetchone():
            for g in DEFAULT_GESTURES:
                c.execute("INSERT OR IGNORE INTO gestures(name,added_epoch) VALUES(?,?)",
                          (g, time.time()))

init_db()

# ── live preview ring buffers (RAM only; the WAV chunks remain the real data) ──
from collections import deque, defaultdict
LIVE_HZ = 12.5                       # preview rate after extra decimation on the ESP
LIVE_MAX = int(LIVE_HZ * 180)        # keep ~3 minutes per device
LIVE = defaultdict(lambda: deque(maxlen=LIVE_MAX))   # device -> deque[(epoch, mV)]
LIVE_HR = {}                                         # {"last": (epoch, bpm)}
app = FastAPI(title="Biolingo Field Ingest")

def check_token(tok: str):
    """Device endpoints are reachable from the open internet. Reject anything
    without the shared token so nobody can fill the disk with junk uploads."""
    if DEVICE_TOKEN and tok != DEVICE_TOKEN:
        raise HTTPException(status_code=403, detail="bad token")

def now_epoch():
    return time.time()

OVERLAP_SQL = """
    SELECT {cols} FROM files
     WHERE start_epoch < ?
       AND (start_epoch + COALESCE(duration, 300)) > ?
     ORDER BY device, start_epoch
"""

def session_files(c, sess, cols="*"):
    """Chunks whose recorded span overlaps the session window.

    Chunks are 5 minutes long and upload only once complete, so the session
    may already be closed by the time a chunk that CONTAINS the session
    arrives. Matching on the session_id stamped at upload therefore misses
    exactly the data you want. Overlap is computed here instead, which also
    fixes sessions recorded before this change.
    """
    t0 = sess["start_epoch"]
    t1 = sess["stop_epoch"] or now_epoch()
    return [dict(r) for r in c.execute(OVERLAP_SQL.format(cols=cols), (t1, t0))]


def active_session(device=None):
    with db() as c:
        q = "SELECT * FROM sessions WHERE stop_epoch IS NULL"
        args = ()
        if device:
            q += " AND device=?"; args = (device,)
        r = c.execute(q + " ORDER BY id DESC LIMIT 1", args).fetchone()
        return dict(r) if r else None

# ---------------- ingest ----------------
@app.post("/upload")
async def upload(device: str = Form(...), sample_rate: int = Form(100),
                 channels: int = Form(1), start_epoch: float = Form(...),
                 role: str = Form(""), units: str = Form("mV_x10"),
                 token: str = Form(""), file: UploadFile = File(...)):
    check_token(token)
    raw = await file.read()
    import shutil
    free_mb = shutil.disk_usage(DATA_DIR).free / 1e6
    if free_mb < 500:
        raise HTTPException(status_code=507, detail=f"low disk: {free_mb:.0f} MB free")

    ddir = DATA_DIR / device
    ddir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromtimestamp(start_epoch, timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fname = f"{device}_{stamp}_{int(start_epoch)}.wav"
    fpath = ddir / fname
    fpath.write_bytes(raw)

    # duration from WAV header if possible
    duration = None
    try:
        with contextlib.closing(wave.open(str(fpath), "rb")) as w:
            duration = w.getnframes() / float(w.getframerate())
    except Exception:
        pass

    sess = active_session(device)
    with db() as c:
        c.execute("""INSERT INTO files(device,path,start_epoch,sample_rate,channels,
                     duration,bytes,session_id,received_epoch,role,units)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                  (device, str(fpath), start_epoch, sample_rate, channels,
                   duration, len(raw), sess["id"] if sess else None, now_epoch(), role, units))
        c.execute("""INSERT INTO heartbeats(device,last_seen,sample_rate,channels)
                     VALUES(?,?,?,?) ON CONFLICT(device) DO UPDATE SET
                     last_seen=excluded.last_seen""",
                  (device, now_epoch(), sample_rate, channels))
    return {"ok": True, "file": fname, "duration": duration, "session": sess["id"] if sess else None}

@app.post("/api/live_push")
async def live_push(device: str = Form(...), token: str = Form(""),
                    samples: str = Form(""), t_end: float = Form(0.0)):
    """ESP32s push a short burst of decimated samples every ~2 s.
    Separate path from the GET so nginx can leave this one un-passworded."""
    check_token(token)
    try:
        vals = [int(x) / 10.0 for x in samples.split(",") if x.strip()]   # mV x10 -> mV
    except ValueError:
        raise HTTPException(status_code=400, detail="bad samples")
    if not vals:
        return {"ok": True, "n": 0}
    end = t_end if t_end > 1e9 else now_epoch()
    dt = 1.0 / LIVE_HZ
    start = end - dt * (len(vals) - 1)
    buf = LIVE[device]
    for i, v in enumerate(vals):
        buf.append((start + i * dt, v))
    return {"ok": True, "n": len(vals)}


@app.get("/api/live")
def live(seconds: float = 60.0):
    """Recent trace per device for the GUI. Read-only, sits behind basic auth."""
    cutoff = now_epoch() - seconds
    out = {}
    with db() as c:
        roles = {r["device"]: r["role"] for r in c.execute("SELECT device,role FROM heartbeats")}
    for dev, buf in LIVE.items():
        pts = [(round(t, 2), round(v, 2)) for t, v in buf if t >= cutoff]
        if pts:
            out[dev] = {"role": roles.get(dev, ""), "points": pts,
                        "hz": LIVE_HZ, "units": "mV"}
    hr = LIVE_HR.get("last")
    return {"now": now_epoch(), "devices": out,
            "hr": {"bpm": hr[1], "age": now_epoch() - hr[0]} if hr else None}


@app.get("/api/heartbeat")
def heartbeat(device: str, rate: int = 0, ch: int = 0, queue: int = 0,
              rssi: int = 0, role: str = "", mv: float = 0.0,
              p2p: float = 0.0, fill: int = 0, state: str = "rec",
              sat: float = 0.0, token: str = ""):
    check_token(token)
    with db() as c:
        c.execute("""INSERT INTO heartbeats(device,last_seen,sample_rate,channels,queue,
                     rssi,role,mv,p2p,fill,state,sat)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(device) DO UPDATE SET
                     last_seen=excluded.last_seen, sample_rate=excluded.sample_rate,
                     channels=excluded.channels, queue=excluded.queue,
                     rssi=excluded.rssi, role=excluded.role, mv=excluded.mv,
                     p2p=excluded.p2p, fill=excluded.fill, state=excluded.state,
                     sat=excluded.sat""",
                  (device, now_epoch(), rate, ch, queue, rssi, role, mv, p2p, fill, state, sat))
        row = c.execute("SELECT armed, flush_req FROM heartbeats WHERE device=?",
                        (device,)).fetchone()
        flush = bool(row["flush_req"]) if row else False
        if flush:
            c.execute("UPDATE heartbeats SET flush_req=0 WHERE device=?", (device,))
    # record: start/stop archiving (human standby switch).
    # flush:  close and upload the current partial chunk now, so the tail of a
    #         session is never lost to a power-off before the chunk fills.
    return {"ok": True, "record": bool(row["armed"]) if row else True, "flush": flush}

# optional WebSocket path (kept minimal; HTTP POST is the primary/robust path)
@app.websocket("/ws")
async def ws(sock: WebSocket):
    await sock.accept()
    device = "ws-device"; buf = bytearray(); meta = {}
    try:
        while True:
            msg = await sock.receive()
            if "text" in msg and msg["text"]:
                meta = json.loads(msg["text"])          # {device, start_epoch, sample_rate, channels}
                device = meta.get("device", device); buf = bytearray()
            elif "bytes" in msg and msg["bytes"] is not None:
                buf += msg["bytes"]
                if meta.get("final"):
                    ddir = DATA_DIR / device; ddir.mkdir(parents=True, exist_ok=True)
                    (ddir / f"{device}_{int(meta.get('start_epoch', now_epoch()))}.wav").write_bytes(bytes(buf))
                    await sock.send_text(json.dumps({"ok": True}))
                    buf = bytearray()
    except WebSocketDisconnect:
        pass

# ---------------- sessions ----------------
@app.post("/api/session/start")
async def session_start(req: Request):
    d = await req.json()
    ts = now_epoch()
    with db() as c:
        cur = c.execute("INSERT INTO sessions(label,device,start_epoch) VALUES(?,?,?)",
                        (d.get("label", ""), d.get("device", ""), ts))
        # Starting a session guarantees storing is on for every box, so nobody
        # has to remember the arm switch. (We deliberately do NOT disarm on stop
        # — continuous recording between sessions is wanted, for baseline.)
        c.execute("UPDATE heartbeats SET armed=1")
        return {"ok": True, "session_id": cur.lastrowid, "start_epoch": ts}

@app.post("/api/session/stop")
async def session_stop(req: Request):
    d = await req.json()
    with db() as c:
        c.execute("UPDATE sessions SET stop_epoch=? WHERE id=?", (now_epoch(), d["session_id"]))
        # ask every node to flush its current partial chunk, so the final
        # seconds of the session upload immediately instead of waiting for the
        # 5-minute boundary (or being lost if the boxes are switched off).
        c.execute("UPDATE heartbeats SET flush_req=1")
    return {"ok": True}


@app.post("/api/flush")
async def flush_all(req: Request):
    """Manual 'save now' - flush every node's partial chunk."""
    with db() as c:
        c.execute("UPDATE heartbeats SET flush_req=1")
    return {"ok": True}

# ---------------- gesture markers ----------------
@app.post("/api/event")
async def event(req: Request):
    d = await req.json()
    with db() as c:
        cur = c.execute("""INSERT INTO events(session_id,device,gesture,rep,phase,
                           server_epoch,client_epoch,note) VALUES(?,?,?,?,?,?,?,?)""",
                        (d.get("session_id"), d.get("device", ""), d.get("gesture", ""),
                         d.get("rep"), d.get("phase", ""), now_epoch(),
                         d.get("client_epoch"), d.get("note", "")))
        return {"ok": True, "event_id": cur.lastrowid, "server_epoch": now_epoch()}

# ---------------- Polar H10 HR CSV upload ----------------
@app.post("/api/hr_push")
async def hr_push(req: Request):
    """Live HR/RR streamed from the browser via Web Bluetooth.

    The phone talks to the Polar H10 directly from the GUI page, so there is
    no separate logger app to pair with and no CSV to remember to export.
    Rows arrive as [epoch, hr, rr]; rr is seconds or null.
    """
    d = await req.json()
    sid = d.get("session_id")
    dev = d.get("device") or "polar-h10"
    role = d.get("role") or ""
    rows = d.get("rows") or []
    n = 0
    with db() as c:
        for r in rows:
            try:
                ep = float(r[0])
                hr = float(r[1]) if r[1] is not None else None
                rr = float(r[2]) if len(r) > 2 and r[2] is not None else None
            except (TypeError, ValueError, IndexError):
                continue
            c.execute("INSERT INTO hr(session_id,device,epoch,hr,rr) VALUES(?,?,?,?,?)",
                      (sid, dev if not role else f"{dev} ({role})", ep, hr, rr))
            n += 1
    if rows:
        LIVE_HR["last"] = (now_epoch(), rows[-1][1])
    return {"ok": True, "n": n}


@app.post("/api/hr/upload")
async def hr_upload(session_id: int = Form(...), device: str = Form(""),
                    file: UploadFile = File(...)):
    """
    Accepts a CSV exported from a Polar-logging phone app.
    Auto-detects columns: needs an epoch/timestamp column and hr and/or rr.
    """
    text = (await file.read()).decode("utf-8", "replace")
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        return {"ok": False, "error": "empty"}
    header = [h.strip().lower() for h in rows[0]]

    def col(*names):
        for n in names:
            if n in header:
                return header.index(n)
        return None
    ci = col("epoch", "timestamp", "time", "unix")
    hi = col("hr", "heartrate", "heart_rate", "bpm")
    ri = col("rr", "rri", "rr_interval", "rr_ms")
    n = 0
    with db() as c:
        for row in rows[1:]:
            if not row or ci is None:
                continue
            try:
                ep = float(row[ci])
                if ep > 1e12:  # ms epoch
                    ep /= 1000.0
                hr = float(row[hi]) if hi is not None and row[hi] else None
                rr = float(row[ri]) if ri is not None and row[ri] else None
                c.execute("INSERT INTO hr(session_id,device,epoch,hr,rr) VALUES(?,?,?,?,?)",
                          (session_id, device, ep, hr, rr))
                n += 1
            except (ValueError, IndexError):
                continue
    return {"ok": True, "rows": n}

# ---------------- read APIs ----------------
@app.post("/api/gesture")
async def add_gesture(req: Request):
    d = await req.json()
    name = (d.get("name") or "").strip()[:24]
    if not name:
        raise HTTPException(status_code=400, detail="empty name")
    with db() as c:
        c.execute("INSERT OR IGNORE INTO gestures(name,added_epoch) VALUES(?,?)",
                  (name, now_epoch()))
    return {"ok": True, "name": name}


@app.delete("/api/gesture/{name}")
def del_gesture(name: str):
    with db() as c:
        c.execute("DELETE FROM gestures WHERE name=?", (name,))
    return {"ok": True}


@app.post("/api/device/{device}/arm")
async def arm_device(device: str, req: Request):
    d = await req.json()
    armed = 1 if d.get("armed") else 0
    with db() as c:
        c.execute("UPDATE heartbeats SET armed=? WHERE device=?", (armed, device))
    return {"ok": True, "armed": bool(armed)}


@app.get("/api/status")
def status():
    with db() as c:
        hbs = [dict(r) for r in c.execute("SELECT * FROM heartbeats ORDER BY device")]
        files = [dict(r) for r in c.execute(
            "SELECT id,device,start_epoch,duration,bytes,session_id,received_epoch "
            "FROM files ORDER BY id DESC LIMIT 15")]
        sess = active_session()
        gestures = [r["name"] for r in
                    c.execute("SELECT name FROM gestures ORDER BY added_epoch, name")]
    now = now_epoch()
    for h in hbs:
        h["online"] = (now - h["last_seen"]) < 120
        h["ago_s"] = round(now - h["last_seen"], 1)
        # Hide boxes that have been silent a long time (spare/put-away units), so
        # the roster stays uncluttered like the live panel. Recently-dropped
        # boxes stay visible with NO CONTACT for a while, so a mid-session
        # dropout (how we caught 02/05 dying) is still obvious at a glance.
        h["hidden"] = (now - h["last_seen"]) > 1800   # 30 min
    return {"now": now, "devices": hbs, "recent_files": files,
            "active_session": sess, "gestures": gestures}

@app.get("/api/session/{sid}")
def session_detail(sid: int):
    with db() as c:
        s = c.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
        ev = [dict(r) for r in c.execute(
            "SELECT * FROM events WHERE session_id=? ORDER BY server_epoch", (sid,))]
        fs = session_files(c, s, "id,device,start_epoch,duration") if s else []
    return {"session": dict(s) if s else None, "events": ev, "files": fs}

@app.get("/api/waveform/{file_id}")
def waveform(file_id: int, points: int = 1200):
    with db() as c:
        r = c.execute("SELECT path FROM files WHERE id=?", (file_id,)).fetchone()
    if not r:
        return JSONResponse({"error": "not found"}, 404)
    try:
        with contextlib.closing(wave.open(r["path"], "rb")) as w:
            n = w.getnframes(); ch = w.getnchannels(); sr = w.getframerate()
            import array
            frames = w.readframes(n)
            a = array.array("h"); a.frombytes(frames)
            mono = a[0::ch] if ch > 1 else a
            step = max(1, len(mono) // points)
            ds = [mono[i] for i in range(0, len(mono), step)]
    except Exception as e:
        return JSONResponse({"error": str(e)}, 500)
    return {"sample_rate": sr, "channels": ch, "points": ds}

@app.get("/api/export/{sid}")
def export_session(sid: int):
    """Everything needed to analyse a session, in one JSON.
    The Mac-side pull script consumes this."""
    with db() as c:
        s = c.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
        if not s:
            return JSONResponse({"error": "no such session"}, 404)
        files = session_files(
            c, s, "id,device,role,path,start_epoch,sample_rate,duration,bytes")
        events = [dict(r) for r in c.execute(
            "SELECT gesture,rep,phase,server_epoch,note FROM events "
            "WHERE session_id=? ORDER BY server_epoch", (sid,))]
        t0 = s["start_epoch"]; t1 = s["stop_epoch"] or now_epoch()
        hrows = [dict(r) for r in c.execute(
            "SELECT epoch,hr,rr FROM hr WHERE (session_id=? OR (epoch>=? AND epoch<=?)) "
            "GROUP BY epoch,hr,rr ORDER BY epoch", (sid, t0, t1))]
    for f in files:
        f["filename"] = Path(f.pop("path")).name
        f["url"] = f"/files/{f['id']}"
    return {"session": dict(s), "files": files, "events": events, "hr": hrows,
            "units": "int16 mV x10 (divide by 10 for mV)"}


@app.get("/api/sessions")
def list_sessions():
    with db() as c:
        rows = [dict(r) for r in c.execute(
            "SELECT s.*, (SELECT COUNT(*) FROM events e WHERE e.session_id=s.id) "
            "AS n_events FROM sessions s ORDER BY s.id DESC")]
        for r in rows:
            r["n_files"] = len(session_files(c, r, "id"))
    return {"sessions": rows}


@app.get("/browse", response_class=HTMLResponse)
@app.get("/browse/{subpath:path}", response_class=HTMLResponse)
def browse(subpath: str = ""):
    """Plain directory listing of the stored WAV files, click to download.
    Confined to DATA_DIR; symlinks and .. cannot escape it."""
    base = DATA_DIR
    target = (base / subpath).resolve()
    if not str(target).startswith(str(base)) or not target.exists():
        return HTMLResponse("<p>not found</p>", status_code=404)

    rows = []
    up = "/".join(subpath.split("/")[:-1])
    if subpath:
        rows.append(f'<tr><td colspan="3"><a href="/browse/{up}">&#8593; up</a></td></tr>')

    entries = sorted((p for p in target.iterdir() if not p.name.startswith("_")),
                     key=lambda p: (p.is_file(), p.name))
    for p in entries:
        rel = str(p.relative_to(base))
        if p.is_dir():
            n = sum(1 for _ in p.iterdir())
            rows.append(f'<tr><td>&#128193; <a href="/browse/{rel}">{p.name}/</a></td>'
                        f'<td>{n} items</td><td></td></tr>')
        else:
            kb = p.stat().st_size / 1024
            mt = datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            rows.append(f'<tr><td>&#127925; {p.name}</td><td>{kb:.0f} KB</td>'
                        f'<td>{mt} &nbsp; <a href="/raw/{rel}" download>download</a></td></tr>')

    crumb = " / ".join(f'<a href="/browse/{"/".join(subpath.split("/")[:i+1])}">{part}</a>'
                       for i, part in enumerate(subpath.split("/")) if part)
    return f"""<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>HPK files</title>
<style>body{{font-family:system-ui,sans-serif;background:#0e1420;color:#e7eefb;padding:16px;max-width:900px;margin:auto}}
a{{color:#43d9a3;text-decoration:none}}a:hover{{text-decoration:underline}}
table{{width:100%;border-collapse:collapse;margin-top:12px}}
td{{padding:7px 10px;border-bottom:1px solid #26344c;font-size:14px}}
h2{{font-size:18px}}.mut{{color:#8ba0c4;font-size:13px}}</style>
<h2>HPK stored files</h2>
<div class=mut>/ {crumb}</div>
<table>{"".join(rows)}</table>
<p class=mut><a href="/">&#8592; back to the field GUI</a></p>"""


@app.get("/raw/{subpath:path}")
def raw_file(subpath: str):
    """Serve a stored file for download (used by /browse)."""
    base = DATA_DIR
    target = (base / subpath).resolve()
    if not str(target).startswith(str(base)) or not target.is_file():
        return PlainTextResponse("not found", 404)
    return FileResponse(target, filename=target.name)


@app.get("/files/{file_id}")
def download(file_id: int):
    with db() as c:
        r = c.execute("SELECT path FROM files WHERE id=?", (file_id,)).fetchone()
    if not r:
        return PlainTextResponse("not found", 404)
    return FileResponse(r["path"], media_type="audio/wav", filename=Path(r["path"]).name)

# ---------------- GUI ----------------
@app.get("/", response_class=HTMLResponse)
def gui():
    return HTML

HTML = """<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>Biolingo Field</title>
<style>
:root{--bg:#0e1420;--panel:#182234;--line:#26344c;--tx:#e7eefb;--mut:#8ba0c4;--acc:#43d9a3;--warn:#e8b04b;--rec:#ff5a6a}
*{box-sizing:border-box}body{margin:0;font-family:system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--tx);padding:12px;max-width:640px;margin:0 auto}
h1{font-size:17px;margin:4px 0 12px;letter-spacing:.3px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:14px;margin-bottom:12px}
.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.dot{width:10px;height:10px;border-radius:50%;display:inline-block;margin-right:6px}
.on{background:var(--acc)}.off{background:var(--rec)}
.mut{color:var(--mut);font-size:13px}
button{font-size:16px;font-weight:600;border:none;border-radius:12px;padding:14px 16px;color:#06121e;background:var(--acc);cursor:pointer}
button.sec{background:var(--line);color:var(--tx)}button.rec{background:var(--rec);color:#fff}
button:disabled{opacity:.4}
.gestgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(72px,1fr));gap:8px;margin:8px 0}
.gestgrid button{padding:16px 0;font-size:18px;background:var(--line);color:var(--tx)}
.gestgrid button.act{background:var(--acc);color:#06121e}
.big{width:100%;padding:20px;font-size:20px;margin-top:6px}
input{background:#0c1220;border:1px solid var(--line);color:var(--tx);border-radius:10px;padding:10px;font-size:15px}
canvas{width:100%;height:90px;background:#0b1120;border-radius:10px;margin-top:8px}
.log{font-size:12px;max-height:150px;overflow:auto;margin-top:8px}
.log div{padding:3px 0;border-bottom:1px solid var(--line)}
.pill{font-size:11px;background:var(--line);border-radius:20px;padding:2px 8px}
a{color:var(--acc)}
</style></head><body>
<h1>Biolingo Field · eurythmy</h1>

<div class="card" id="devices"><div class="mut">devices…</div></div>

<div class="card">
  <div class="row" style="margin-top:2px">
    <input id="labelInput" placeholder="session label (optional)" style="flex:1">
  </div>
  <div class="row" style="margin-top:10px">
    <button id="startS" class="big" style="flex:1">Start session</button>
    <button id="stopS" class="big sec" style="flex:1" disabled>End session</button>
  </div>
</div>

<!-- Full-width stage signal: tells both people what to do right now -->
<div id="stage" style="display:none;border-radius:16px;padding:26px 18px;margin-bottom:12px;
     text-align:center;transition:background .4s">
  <div id="stageName" style="font-size:34px;font-weight:800;letter-spacing:.5px;color:#06121e"></div>
  <div id="stageInstr" style="font-size:17px;font-weight:600;margin-top:4px;color:#06121e;opacity:.8"></div>
  <div id="stageNext" style="font-size:14px;margin-top:12px;color:#06121e;opacity:.65"></div>
  <!-- Wide progress bar: grows evenly across the whole 5-minute session and is
       now the ONLY timing cue (no running number to distract the eurythmist).
       Tall and high-contrast so it reads clearly from 1-2 m. -->
  <div style="margin-top:20px;height:40px;border-radius:20px;background:rgba(6,18,30,.18);overflow:hidden">
    <div id="stageBar" style="height:100%;width:0%;border-radius:20px;background:#06121e;opacity:.55;transition:width 1s linear"></div>
  </div>
</div>

<!-- After End session: tells the operator whether the last chunk has uploaded,
     so they know when it is safe to power the boxes down (no guessing at a clock). -->
<div id="saveBanner" style="display:none;border-radius:12px;padding:12px 14px;margin-bottom:12px;
     font-weight:700;text-align:center"></div>

<div class="card">
  <div class="row" style="justify-content:space-between">
    <b>Live signal</b>
    <span class="row" style="gap:6px">
      <button class="sec" id="win30" style="padding:6px 10px;font-size:13px">30s</button>
      <button class="sec" id="win60" style="padding:6px 10px;font-size:13px">60s</button>
      <button class="sec" id="win180" style="padding:6px 10px;font-size:13px">3m</button>
    </span>
  </div>
  <div id="scopes"><div class="mut">waiting for data…</div></div>
</div>


<div class="card">
  <b>Polar H10 · two straps</b>
  <div class="mut">Connect each person's strap. Connect both before starting.</div>

  <div class="row" style="margin-top:10px;gap:8px">
    <div style="flex:1">
      <button id="polarBtnEur" style="width:100%">Connect eurythmist</button>
      <div class="mut" id="polarMsgEur" style="margin-top:4px">not connected</div>
    </div>
    <div style="flex:1">
      <button id="polarBtnCtl" class="sec" style="width:100%">Connect control</button>
      <div class="mut" id="polarMsgCtl" style="margin-top:4px">not connected</div>
    </div>
  </div>
  <div class="row" style="margin-top:8px">
    <span class="pill">eurythmist: <b id="hrEur">—</b></span>
    <span class="pill">control: <b id="hrCtl">—</b></span>
  </div>

  <div id="polarWarn" style="display:none;margin-top:8px;padding:8px 10px;border-radius:8px;
       background:#e8b04b;color:#06121e;font-size:13px;font-weight:600">
    Session running with a strap not connected
  </div>
  <div id="polarStall" style="display:none;margin-top:8px;padding:8px 10px;border-radius:8px;
       background:#ff5a6a;color:#fff;font-size:13px;font-weight:600">
    A strap says connected but no beats are arriving — reconnect
  </div>
  <div class="mut" id="wakeMsg" style="margin-top:6px"></div>
</div>

<div class="card">
  <div class="row" style="justify-content:space-between">
    <b>Recent files</b><a href="/browse" style="font-size:13px">browse all &#8599;</a>
  </div>
  <div class="log" id="files"></div>
</div>

<script>
let SESSION=null, DEVICE="";
const $=id=>document.getElementById(id);

async function api(path,opts){const r=await fetch(path,opts);return r.json();}
function fmtT(ep){return new Date(ep*1000).toLocaleTimeString();}
function fmtAgo(s){
  if(s<90) return Math.round(s)+' s';
  if(s<5400) return Math.round(s/60)+' min';
  return (s/3600).toFixed(1)+' h';
}

async function refresh(){
  const s=await api('/api/status');

  // Drop long-idle boxes from the roster (server marks them hidden); recently
  // dropped ones stay so a mid-session dropout is still visible.
  const shown=s.devices.filter(d=>!d.hidden);
  DEVICE=(shown[0]||{}).device||DEVICE;
  $('devices').innerHTML = shown.length? shown.map(d=>
    (d.online
      ? `<div style="padding:6px 0;border-bottom:1px solid var(--line)">
          <div class="row"><span class="dot on"></span><b>${d.device}</b>
           ${d.role==='control'?'<span class="pill">control</span>':''}
           ${d.queue>0?`<span class="pill" style="background:#e8b04b;color:#06121e">backlog ${d.queue}</span>`:''}
           ${d.state==='idle'?'<span class="pill" style="background:#ff5a6a;color:#fff">PAUSED — no electrode</span>':''}
           ${d.state==='flat'?'<span class="pill" style="background:#ff5a6a;color:#fff">FLAT — no signal at all</span>':''}
           ${!d.armed?'<span class="pill" style="background:#8ba0c4;color:#06121e">STANDBY — not storing</span>':''}
           <button class="sec armbtn" data-dev="${d.device}" data-armed="${d.armed?1:0}"
             style="padding:4px 10px;font-size:12px;margin-left:auto">${d.armed?'stop storing':'start storing'}</button>
          </div>
          <div class="mut">online · ${d.rssi}dBm · ${d.mv.toFixed(1)} mV ·
           p2p ${d.p2p.toFixed(1)} · ${d.state==='idle'?'idle':'recording '+d.fill+'%'}</div>
         </div>`
      // Offline: show ONLY how long it has been silent. The last readings are
      // stale and printing them next to a red dot reads as live data.
      : `<div style="padding:6px 0;border-bottom:1px solid var(--line);opacity:.55">
          <div class="row"><span class="dot off"></span><b>${d.device}</b>
           <span class="pill" style="background:#ff5a6a;color:#fff">NO CONTACT</span></div>
          <div class="mut">silent for ${fmtAgo(d.ago_s)} — last readings hidden (stale)</div>
         </div>`)
  ).join('') : '<div class="mut">no devices yet — power on the ESP32</div>';

  // Adopt the server's running session. If a staged panel is already running
  // locally we leave it alone; otherwise (e.g. right after a page reload) we
  // REJOIN it — runStages recomputes the current stage from the session's
  // start_epoch, so colour and countdown come back exactly where they should be.
  if(!STAGE_RUN){
    if(s.active_session){
      runStages(s.active_session.id, s.active_session.start_epoch);
    }else{SESSION=null;$('startS').disabled=false;$('stopS').disabled=true;}
  }

  $('files').innerHTML=s.recent_files.map(f=>
    `<div><span class="mut">${fmtT(f.start_epoch)}</span> ${f.device}
     ${f.duration?f.duration.toFixed(0)+'s':''} · ${(f.bytes/1024).toFixed(0)}KB
     <a href="/files/${f.id}">wav</a></div>`).join('');
  document.querySelectorAll('.armbtn').forEach(b=>{
    b.onclick=async()=>{
      const on=b.dataset.armed==='1';
      await fetch('/api/device/'+encodeURIComponent(b.dataset.dev)+'/arm',
        {method:'POST',headers:{'Content-Type':'application/json'},
         body:JSON.stringify({armed:!on})});
      refresh();
    };
  });
}

// ───────── staged workshop session ─────────
// Fixed 5-minute protocol. The big coloured panel tells both people (the
// eurythmist and the control person) what to do; each stage boundary is logged
// as an event so the phases can be cut out of the recording afterwards.
//
// The schedule is deterministic and anchored to the session's server-side
// start time, so which stage we are in is a pure function of elapsed wall-clock
// seconds. That means a page reload (or a phone that froze the tab) can rejoin
// a running session and land on exactly the right stage — no local state is
// load-bearing. STAGE_START is the epoch (seconds) the running session began.
const STAGES=[
  {name:'SILENCE',  instr:'both stand still', secs:60, bg:'#4a90d9'},
  {name:'EURYTHMY', instr:'perform  I',       secs:60, bg:'#e8c33b'},
  {name:'SILENCE',  instr:'both stand still', secs:60, bg:'#4a90d9'},
  {name:'COOLDOWN', instr:'relax',            secs:120,bg:'#e05a5a'},
];
const STAGE_TOTAL=STAGES.reduce((a,s)=>a+s.secs,0);   // 300 s
let STAGE_RUN=false, stageTimer=null, STAGE_START=null, lastStageIdx=-1;

async function logEvent(gesture,phase){
  if(!SESSION) return;
  await api('/api/event',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({session_id:SESSION,device:DEVICE,gesture:gesture,rep:1,
      phase:phase,client_epoch:Date.now()/1000})});
}

// Which stage are we in at `elapsed` seconds, and how many seconds remain in it?
function stageAt(elapsed){
  let acc=0;
  for(let i=0;i<STAGES.length;i++){
    if(elapsed < acc+STAGES[i].secs) return {idx:i, remain:Math.ceil(acc+STAGES[i].secs-elapsed)};
    acc+=STAGES[i].secs;
  }
  return {idx:STAGES.length, remain:0};   // finished
}

function showStage(i,remain){
  const st=STAGES[i], el=$('stage');
  el.style.display='block'; el.style.background=st.bg;
  $('stageName').textContent=st.name;
  $('stageInstr').textContent=st.instr;
  const nxt=STAGES[i+1];
  $('stageNext').textContent=nxt?('next: '+nxt.name+' · '+nxt.instr):'last stage';
  // Smooth progress bar across the WHOLE session (grows evenly 0->100%), wide
  // enough to read from 1-2 m while doing eurythmy without twitching digits.
  let done=0; for(let k=0;k<i;k++) done+=STAGES[k].secs;
  done += (STAGES[i].secs - remain);
  const pct=Math.max(0,Math.min(100, 100*done/STAGE_TOTAL));
  const bar=$('stageBar'); if(bar) bar.style.width=pct.toFixed(1)+'%';
}

function endStages(finished){
  clearInterval(stageTimer); stageTimer=null; STAGE_RUN=false; STAGE_START=null; lastStageIdx=-1;
  const el=$('stage');
  const bar=$('stageBar'); if(bar) bar.style.width='0%';
  if(finished){ el.style.background='#2e7d55'; $('stageName').textContent='DONE';
    $('stageInstr').textContent='session complete';
    $('stageNext').textContent=''; setTimeout(()=>{el.style.display='none';},6000); }
  else el.style.display='none';
  $('startS').disabled=false; $('stopS').disabled=true;
}

// The one timer that drives the panel. Reads elapsed time from STAGE_START each
// tick, so it is self-correcting: if the tab was frozen for 20 s, the next tick
// simply shows the correct current stage rather than drifting behind.
function stageTick(){
  const elapsed=(Date.now()/1000)-STAGE_START;
  const {idx,remain}=stageAt(elapsed);
  if(idx>=STAGES.length){                       // reached the end
    if(lastStageIdx>=0 && lastStageIdx<STAGES.length) logEvent(STAGES[lastStageIdx].name,'stop');
    finishSession(); return;
  }
  if(idx!==lastStageIdx){                        // crossed a boundary
    if(lastStageIdx>=0) logEvent(STAGES[lastStageIdx].name,'stop');
    logEvent(STAGES[idx].name+':'+STAGES[idx].instr,'start');
    lastStageIdx=idx;
  }
  showStage(idx,remain);
}

async function finishSession(){
  clearInterval(stageTimer); stageTimer=null; STAGE_RUN=false; STAGE_START=null;
  if(SESSION){
    await api('/api/session/stop',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({session_id:SESSION})});
    await flushHR();
  }
  releaseWake(); endStages(true); SESSION=null;
  watchForSave();
}

// After a session ends the boxes flush their partial chunks on the next
// heartbeat (~10 s) and upload them. This shows "saving..." and flips to
// "saved - safe to power down" once a new file actually arrives, so nobody has
// to guess how long to wait. Gives up after ~40 s with a soft note.
let saveWatchTimer=null;
async function watchForSave(){
  const b=$('saveBanner');
  if(saveWatchTimer){clearInterval(saveWatchTimer); saveWatchTimer=null;}
  // newest file id right now — a genuinely new upload will exceed this
  let baseline=-1;
  try{ const s=await api('/api/status');
       baseline=(s.recent_files&&s.recent_files[0])?s.recent_files[0].id:-1; }catch(e){}
  b.style.display='block'; b.style.background='#e8b04b'; b.style.color='#06121e';
  b.textContent='saving last recording… please keep the boxes powered';
  let waited=0;
  saveWatchTimer=setInterval(async ()=>{
    waited+=3;
    try{
      const s=await api('/api/status');
      const top=(s.recent_files&&s.recent_files[0])?s.recent_files[0].id:-1;
      if(top>baseline){
        clearInterval(saveWatchTimer); saveWatchTimer=null;
        b.style.background='#2e7d55'; b.style.color='#eafff5';
        b.textContent='✓ saved — safe to power down the boxes';
        setTimeout(()=>{b.style.display='none';},12000);
        return;
      }
    }catch(e){}
    if(waited>=40){
      clearInterval(saveWatchTimer); saveWatchTimer=null;
      b.style.background='#8ba0c4'; b.style.color='#06121e';
      b.textContent='last chunk not seen yet — give it a moment, then check Recent files before powering down';
      setTimeout(()=>{b.style.display='none';},12000);
    }
  },3000);
}

// Begin (or, on reload, rejoin) the staged panel for session `sid` that started
// at epoch `startEpoch`. Idempotent: calling it again while running is a no-op.
function runStages(sid, startEpoch){
  if(STAGE_RUN) return;
  SESSION=sid; STAGE_START=startEpoch; STAGE_RUN=true; lastStageIdx=-1;
  acquireWake();
  $('startS').disabled=true; $('stopS').disabled=false;
  stageTick();                                   // paint immediately
  stageTimer=setInterval(stageTick,1000);
}

async function startSession(){
  const now=new Date();
  const r=await api('/api/session/start',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({label:$('labelInput').value.trim() ||
      now.toLocaleDateString()+' '+now.toTimeString().slice(0,5), device:DEVICE})});
  // Anchor to the server's start_epoch when it gives one, so every client that
  // views this session agrees on the schedule; fall back to local clock.
  runStages(r.session_id, (r.start_epoch!=null)? r.start_epoch : Date.now()/1000);
}

async function endSessionEarly(){
  if(SESSION){
    if(lastStageIdx>=0 && lastStageIdx<STAGES.length) await logEvent(STAGES[lastStageIdx].name,'stop');
    await logEvent('ABORTED','stop');
    await api('/api/session/stop',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({session_id:SESSION})});
    await flushHR(); releaseWake();
  }
  endStages(false); SESSION=null; refresh();
  watchForSave();
}

$('startS').onclick=startSession;
$('stopS').onclick=endSessionEarly;

// ───────── live scopes ─────────
let WIN=60;
['win30','win60','win180'].forEach((id,i)=>{
  $(id).onclick=()=>{WIN=[30,60,180][i];liveTick();};
});

function ensureScope(dev,role){
  let el=document.getElementById('sc_'+dev);
  if(el) return el;
  const wrap=document.createElement('div');
  wrap.style.marginBottom='10px';
  wrap.innerHTML=`<div class="row" style="justify-content:space-between">
      <b style="font-size:13px">${dev}${role==='control'?' <span class="pill">control</span>':''}</b>
      <span class="mut" id="sm_${dev}"></span></div>
      <canvas id="sc_${dev}" width="600" height="70"></canvas>`;
  $('scopes').appendChild(wrap);
  return document.getElementById('sc_'+dev);
}

async function liveTick(){
  let d;
  try{ d=await api('/api/live?seconds='+WIN); }catch(e){ return; }
  const devs=Object.keys(d.devices).sort();
  if(!devs.length){ return; }
  if($('scopes').firstChild && $('scopes').firstChild.className==='mut') $('scopes').innerHTML='';

  devs.forEach(dev=>{
    const info=d.devices[dev], pts=info.points;
    const cv=ensureScope(dev,info.role);
    const x=cv.getContext('2d'),W=cv.width,H=cv.height;
    x.clearRect(0,0,W,H);
    if(pts.length<2) return;

    const vs=pts.map(p=>p[1]);
    let lo=Math.min(...vs), hi=Math.max(...vs);
    const span=Math.max(hi-lo,1.0), pad=span*0.12;
    lo-=pad; hi+=pad;
    const t1=d.now, t0=t1-WIN;
    const px=t=>(t-t0)/WIN*W, py=v=>H-((v-lo)/(hi-lo))*H;

    // trace — break the line across gaps so a dropout can't look like signal
    x.strokeStyle='#43d9a3'; x.lineWidth=1.4; x.beginPath();
    let pen=false;
    for(let i=0;i<pts.length;i++){
      const [t,v]=pts[i];
      if(i>0 && t-pts[i-1][0] > 1.0){ x.stroke(); x.beginPath(); pen=false; }
      const X=px(t),Y=py(v);
      pen?x.lineTo(X,Y):x.moveTo(X,Y); pen=true;
    }
    x.stroke();
    document.getElementById('sm_'+dev).textContent=
      `${vs[vs.length-1].toFixed(1)} mV · range ${(hi-lo-2*pad).toFixed(1)}`;
  });
}

// ───────── Polar H10 over Web Bluetooth — two straps ─────────
// The H10 exposes the standard BLE Heart Rate Service, so the browser reads HR
// and RR directly. Two people wear straps: the eurythmist and the control. Each
// connection is tagged with its role so the two streams stay separate on the
// server. Requires HTTPS + Chrome on Android.
const POLAR = {
  eur: {dev:null, on:false, last:null, hr:null, buf:[], btn:'polarBtnEur', msg:'polarMsgEur', show:'hrEur', label:'eurythmist'},
  ctl: {dev:null, on:false, last:null, hr:null, buf:[], btn:'polarBtnCtl', msg:'polarMsgCtl', show:'hrCtl', label:'control'},
};

async function connectPolar(role){
  const P=POLAR[role];
  if(!navigator.bluetooth){ $(P.msg).textContent='need Chrome on Android'; return; }
  try{
    $(P.msg).textContent='choose the '+P.label+"'s strap…";
    const dev=await navigator.bluetooth.requestDevice({filters:[{services:['heart_rate']}]});
    dev.addEventListener('gattserverdisconnected',()=>{
      P.on=false; $(P.msg).textContent='disconnected — tap to reconnect';
      $(P.btn).textContent='Connect '+P.label;
    });
    const srv=await dev.gatt.connect();
    const svc=await srv.getPrimaryService('heart_rate');
    const ch=await svc.getCharacteristic('heart_rate_measurement');
    await ch.startNotifications();
    ch.addEventListener('characteristicvaluechanged', ev=>onHR(role,ev));
    P.dev=dev; P.on=true;
    $(P.btn).textContent=P.label+' connected';
    $(P.msg).textContent='connected: '+(dev.name||'Polar H10');
  }catch(e){
    $(P.msg).textContent = e.name==='NotFoundError'
      ? 'no strap chosen — make sure it is worn and damp' : ('failed: '+e.message);
  }
}

function onHR(role,ev){
  const P=POLAR[role], v=ev.target.value, flags=v.getUint8(0);
  let i=1, hr;
  if(flags&0x01){ hr=v.getUint16(1,true); i=3; } else { hr=v.getUint8(1); i=2; }
  if(flags&0x08) i+=2;
  const rrs=[];
  if(flags&0x10){ for(;i+1<v.byteLength;i+=2) rrs.push(v.getUint16(i,true)/1024); }
  const t=Date.now()/1000;
  P.last=t; P.hr=hr;
  if(rrs.length) rrs.forEach(rr=>P.buf.push([t,hr,rr])); else P.buf.push([t,hr,null]);
  $(P.show).textContent=hr+' bpm';
}

async function flushHR(){
  for(const role of ['eur','ctl']){
    const P=POLAR[role];
    if(!P.buf.length) continue;
    const rows=P.buf.splice(0,P.buf.length);
    try{
      const r=await fetch('/api/hr_push',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({session_id:SESSION, device:'polar-'+role, role:P.label, rows:rows})});
      if(!r.ok) P.buf=rows.concat(P.buf);
    }catch(e){ P.buf=rows.concat(P.buf); }
  }
}
const anyPolarOn=()=>POLAR.eur.on||POLAR.ctl.on;
const bothPolarOn=()=>POLAR.eur.on&&POLAR.ctl.on;

$('polarBtnEur').onclick=()=>connectPolar('eur');
$('polarBtnCtl').onclick=()=>connectPolar('ctl');
setInterval(flushHR,5000);

// ── keep the screen awake ──────────────────────────────────────────────
// Mobile browsers suspend a backgrounded or screen-locked tab. That stops the
// Polar notifications AND this page's timers, so heart rate silently stops
// being recorded while the card still shows the last value it saw. A screen
// wake lock keeps the session alive; it is released when the session stops.
let wakeLock=null;
async function acquireWake(){
  if(!('wakeLock' in navigator)) { $('wakeMsg').textContent=
      'This browser cannot keep the screen awake — set the phone screen timeout to never.'; return; }
  try{
    wakeLock=await navigator.wakeLock.request('screen');
    wakeLock.addEventListener('release',()=>{ $('wakeMsg').textContent='screen lock released'; });
    $('wakeMsg').textContent='screen kept awake while the session runs';
  }catch(e){ $('wakeMsg').textContent='could not keep screen awake: '+e.message; }
}
async function releaseWake(){
  try{ if(wakeLock){ await wakeLock.release(); wakeLock=null; $('wakeMsg').textContent=''; } }catch(e){}
}
document.addEventListener('visibilitychange',()=>{
  if(document.visibilityState==='visible' && SESSION && !wakeLock) acquireWake();
});

// A session recording plant data with no HR is a gap you only notice when you
// come to analyse it. Say so now, while it can still be fixed.
setInterval(()=>{
  // during a session, both straps should be connected AND sending
  $('polarWarn').style.display = (SESSION && !bothPolarOn()) ? 'block' : 'none';
  const now=Date.now()/1000;
  let stalled=false;
  for(const role of ['eur','ctl']){
    const P=POLAR[role];
    if(P.on && P.last && (now-P.last)>15) stalled=true;
    if(P.on && P.last){
      const age=Math.round(now-P.last);
      $(P.show).textContent = (age>5? P.hr+' bpm ('+age+'s ago)' : P.hr+' bpm');
    }
  }
  $('polarStall').style.display = stalled ? 'block' : 'none';
}, 2000);

refresh();setInterval(refresh,5000);
liveTick();setInterval(liveTick,1000);
</script></body></html>"""

if __name__ == "__main__":
    import uvicorn
    print(f"Biolingo field ingest on http://{HOST}:{PORT}  data -> {DATA_DIR}")
    uvicorn.run(app, host=HOST, port=PORT)
