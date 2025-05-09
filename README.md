# SourceMapStats

**SourceMapStats** is an end-to-end statistics dashboard for Valve Source / GoldSource games.  
It periodically queries public game servers, stores per-map player counts in a CSV file, and serves an interactive Chart.js dashboard so you can explore which maps are really being played over time.

![screenshot](docs/example.png) 

---

## Table of Contents
- [SourceMapStats](#sourcemapstats)
  - [Table of Contents](#table-of-contents)
  - [Why?](#why)
  - [Live Demo](#live-demo)
  - [Quick start](#quick-start)
    - [1. Clone \& install](#1-clone--install)
    - [2. Set your own API key](#2-set-your-own-api-key)
    - [3. Run the server](#3-run-the-server)
    - [4. Start the crawler from the UI](#4-start-the-crawler-from-the-ui)
    - [5. (Optional) Set up as a system service](#5-optional-set-up-as-a-system-service)
  - [Local vs public mode](#local-vs-public-mode)
    - [How to expose the service](#how-to-expose-the-service)
  - [Configuration](#configuration)
  - [Security Recommendations](#security-recommendations)
  - [REST API](#rest-api)

---

## Why?

I built SourceMapStats for a personal project: I wanted concrete data about which **Team Fortress 2** maps people really play.  
The scanner, however, is game-agnostic—just change the `Game` parameter and it will happily crawl any Source engine title that’s listed on the Steam Master Server.

---

## Live Demo

Visit the [live demo](http://176.57.188.166:5000/) to see SourceMapStats in action.

> **Note:** By default the server binds **only to localhost** for safety.  
> If you enable public mode (see below) you can replace `127.0.0.1` with your machine’s LAN or public IP.

---

## Quick start

> Requires **Python 3.9+** (needed by Waitress ≥ 3.0) and **git**.

### 1. Clone & install

```bash
git clone https://github.com/Ultikynnys/SourceMapStats.git
cd SourceMapStats
chmod +x run_app.sh
```

### 2. Set your own API key

`config_keys.json` ships with a **placeholder** key. Replace it with your own randomly-generated token **before** starting the server:

```bash
cp config_keys.json.example config_keys.json
nano config_keys.json
```

```json
{
  "accepted_keys": [
    "CHANGEME-PUT-YOUR-RANDOM-KEY-HERE"
  ]
}
```

Use at least 20–30 alphanumeric characters; dashes and underscores are allowed.

### 3. Run the server

```bash
./run_app.sh            # creates venv, installs deps, starts server
```

The script autodetects your local IP and launches the Flask app through **Waitress**.  
Open `http://localhost:5000` (same machine) or `http://<server-ip>:5000` if you have enabled **public mode**.

### 4. Start the crawler from the UI

1. Paste your API key into the **“API Key”** field in the right-hand sidebar.  
2. Click **Start Scanning**.  
3. Watch the status panel update and the chart populate.

![Start scanning button](docs/Start.png)

> **Tip:** Let the crawler run continuously for **at least one month** to gather enough samples for reliable map-popularity trends. Shorter runs can be skewed by daily fluctuations and special events.

### 5. (Optional) Set up as a system service

To run SourceMapStats as a background service:

```bash
sudo cp sourcemapstats.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable sourcemapstats
sudo systemctl start sourcemapstats
```

---

## Local vs public mode

For security SourceMapStats ships **private-first**.  
A single constant at the top of **`app.py`** controls where Waitress binds:

```python
################################################
# --------------[ Bind Mode Toggle ]-----------#
################################################
PUBLIC_MODE: bool = False  # ⟵ default (local-only)
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

2. Restart the server (`Ctrl-C` + re-run `./run_app.sh`).  
3. Ensure port `5000` is open (firewall / Docker `-p 5000:5000` / cloud security group).  
4. Visit:

   ```
   http://<server-ip>:5000
   ```

---

## Configuration

You can tweak most parameters on-the-fly via the sidebar or `POST /api/update_params`.  
**New in vX.Y:** you can also adjust how much days with more snapshots “count” by setting **BiasExponent** (default 1). Rankings and averages are computed using per-day weight = `snapshot_count^BiasExponent`.

| Key               | Type    | Default    | Description                                                                                       |
|-------------------|---------|------------|---------------------------------------------------------------------------------------------------|
| `Game`            | str     | `"tf"`     | Short game dir (tf, csgo, etc.).                                                                  |
| `MapsToShow`      | int     | `15`       | How many top maps to graph.                                                                       |
| `Start_Date`      | date    | wide range | Date window used when aggregating CSV rows (`YYYY-MM-DD`).                                        |
| `End_Date`        | date    | wide range |                                                                                                   |
| `OnlyMapsContaining` | str   | `"dr_"`    | Only include maps whose names contain this substring.                                             |
| `FastWriteDelay`  | int     | `10`       | Minutes between “fast” scans.                                                                     |
| `RuntimeMinutes`  | int     | `60`       | How long (minutes) each continuous scanning session runs before resetting.                        |
| `ColorIntensity`  | int     | `3`        | Controls color cycling intensity in Chart.js lines.                                               |
| `BiasExponent`    | int     | `1`        | Exponent applied to `snapshot_count` when weighting days: >1 amplifies bias toward well-sampled days; <1 softens it. |

Changes persist **until the process restarts**. Consider using environment variables or editing `app.py` for permanent defaults.

---

## Security Recommendations

1. Always change the default API key before first run  
2. Use a strong firewall configuration if exposing publicly  
3. Consider putting the service behind a reverse proxy  
4. Monitor the `/api/status` and `/api/rate_limit` endpoints for abuse  
5. Rate limit aggressive IPs at your network level  

---

## REST API

All endpoints rate-limit anonymous callers to **30 req/15s** per IP.  
Supply a header `X-API-KEY: <your-token>` to bypass this limit.

| Method | Path             | Description                                                  |
|--------|------------------|--------------------------------------------------------------|
| **GET**    | `/api/heartbeat`    | Lightweight health-check.                                      |
| **POST**   | `/api/start_scan`   | Begin continuous scanning thread (requires API key).          |
| **POST**   | `/api/stop_scan`    | Graceful stop.                                                |
| **GET**    | `/api/status`       | JSON payload with current scanner state.                      |
| **GET**    | `/api/data`         | Chart-ready JSON (labels, datasets, stats).                   |
| **POST**   | `/api/update_params`| Hot-patch configuration.                                      |
| **GET**    | `/api/csv_status`   | `{ exists: bool, empty: bool }`.                              |

Example:

```bash
curl -H "X-API-KEY: YOURTOKEN" -X POST http://localhost:5000/api/start_scan
```
