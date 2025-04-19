# SourceMapStats

**SourceMapStats** is an end‑to‑end statistics dashboard for Valve Source / GoldSource games.  
It periodically queries public game servers, stores per‑map player counts in a CSV file, and serves an interactive Chart.js dashboard so you can explore which maps are really being played over time.

![screenshot](docs/example.png) <!--‑‑ add your own screenshot -->

---

## Table of Contents
- [SourceMapStats](#sourcemapstats)
  - [Table of Contents](#tableofcontents)
  - [Why?](#why)
  - [Live demo(http://176.57.188.166:5000)](#live-demohttp176571881665000)
  - [Quick start](#quickstart)
  - [Local vs public mode](#localvspublicmode)
    - [How to expose the service](#how-to-expose-the-service)
  - [Configuration](#configuration)
  - [REST API](#restapi)

---

## Why?

I built SourceMapStats for a personal project: I wanted concrete data about which **Team Fortress 2** maps people really play.  
The scanner, however, is game‑agnostic—just change the `Game` parameter and it will happily crawl any Source engine title that’s listed on the Steam Master Server.

---

## Live demo(http://176.57.188.166:5000)

> **Note:** By default the server binds **only to localhost** for safety.  
> If you enable public mode (see below) you can replace `127.0.0.1` with your machine’s LAN or public IP.

---

## Quick start

> Requires **Python 3.9+** (needed by Waitress ≥ 3.0) and **git**.

### 1. Clone & install

```bash
git clone https://github.com/Ultikynnys/SourceMapStats.git
cd SourceMapStats
chmod +x run_app.sh
```

### 2. Set your own API key

`config_keys.json` ships with a **placeholder** key. Replace it with your own randomly‑generated token **before** starting the server:

```bash
cp config_keys.json.example config_keys.json
nano config_keys.json
```

```json
{
  "accepted_keys": [
    "CHANGEME‑PUT‑YOUR‑RANDOM‑KEY‑HERE"
  ]
}
```

Use at least 20–30 alphanumeric characters; dashes and underscores are allowed.

### 3. Run the server

```bash
./run_app.sh            # creates venv, installs deps, starts server
```

The script autodetects your local IP and launches the Flask app through **Waitress**.  
Open `http://localhost:5000` (same machine) or `http://<server‑ip>:5000` if you have enabled **public mode**.

### 4. Start the crawler from the UI

1. Paste your API key into the **“API Key”** field in the right‑hand sidebar.  
2. Click **Start Scanning**.  
3. Watch the status panel update and the chart populate.

![Start scanning button](docs/Start.png)

> **Tip:** Let the crawler run continuously for **at least one month** to gather enough samples for reliable map‑popularity trends. Shorter runs can be skewed by daily fluctuations and special events.

---

## Local vs public mode

For security SourceMapStats ships **private‑first**.  
A single constant at the top of **`app.py`** controls where Waitress binds:

```python
################################################
# --------------[ Bind Mode Toggle ]-----------#
################################################
PUBLIC_MODE: bool = False  # ⟵ default (local‑only)
```

| Setting | Effect | Reachability |
|---------|--------|--------------|
| `False` (default) | Waitress binds to `127.0.0.1` | Requests are accepted **only** from the same machine. |
| `True` | Waitress binds to `0.0.0.0` | The API can be reached from any interface/IP where the port is open. |

### How to expose the service

1. Edit **`app.py`** and flip the flag:

   ```python
   PUBLIC_MODE = True
   ```

2. Restart the server (`Ctrl‑C` + re‑run `./run_app.sh`).

3. Ensure port `5000` is open (firewall / Docker `-p 5000:5000` / cloud security group).

4. Visit:

   ```
   http://<server‑ip>:5000
   ```

---

## Configuration

Runtime configuration is stored in‑memory and can be tweaked on the fly via the sidebar or `POST /api/update_params`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Game` | str | `"tf"` | Short game dir (tf, csgo, etc.). |
| `MapsToShow` | int | `15` | How many top maps to graph. |
| `Start_Date` / `End_Date` | `YYYY‑MM‑DD` | wide range | Date window used when aggregating CSV rows. |
| `AverageDays` | int | `1` | Number of days per bucket for averaging. |
| `FastWriteDelay` | int (min) | `10` | Idle delay between “fast” scans. |
| … | | | see the `config` dict in `app.py`. |

Changes persist **until the process restarts** (no disk write yet).

---

## REST API

All endpoints rate‑limit anonymous callers to **3 req/s**.  
Supply a header `X‑API‑KEY: <your‑token>` to bypass this limit.

| Method | Path | Description |
|--------|------|-------------|
| **GET** | `/api/heartbeat` | Lightweight health‑check. |
| **POST** | `/api/start_scan` | Begin continuous scanning thread (requires API key). |
| **POST** | `/api/stop_scan` | Graceful stop. |
| **GET** | `/api/status` | JSON payload with current scanner state. |
| **GET** | `/api/data` | Chart‑ready JSON (labels, datasets, stats). |
| **POST** | `/api/update_params` | Hot‑patch configuration. |
| **GET** | `/api/params` | Current config. |
| **GET** | `/api/csv_status` | `{ exists: bool, empty: bool }`. |
| **GET** | `/api/connections` | Last 50 requests per IP (admin only). |

Example:

```bash
curl -H "X-API-KEY: YOURTOKEN" -X POST http://localhost:5000/api/start_scan
```

---
