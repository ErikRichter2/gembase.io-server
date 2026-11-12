# Gembase — server

Backend for **Gembase**, a platform that helped game studios evaluate game concepts *before* building an MVP.
A studio described a concept (title, description, tags for core loop, art style, setting…), and Gembase found
the closest competitors on Google Play and scored the concept's threats and opportunities, using:

- **Market data** — ~40,000 Google Play apps, scraped and tagged with an LLM so concepts and real games share the same tag space
- **Player surveys** — answers from thousands of players about their preferences, the games they play and whether they'd play specific concepts

> Gembase was a two-person startup (2022–2024). My partner owned the product vision, design and evaluation
> methodology; I built the whole technical side — this server, the [Vue client](../gembase.io-client), the scraping
> pipeline and the LLM integration. The product was shut down; the code is published as a portfolio piece and
> is not actively maintained.

## Tech stack

| Area | Technology |
|---|---|
| API | Python 3.10, Flask, Gunicorn, Flask-CORS |
| Database | MySQL (via PyMySQL) |
| Data / calculations | pandas, NumPy, SciPy, PyArrow |
| LLM tagging | OpenAI API |
| Scraping | google-play-scraper (customized), Selenium + PyVirtualDisplay, BeautifulSoup |
| Auth & payments | JWT (PyJWT), Stripe |
| ID obfuscation | Obfuskey |

## Architecture

```
            ┌───────────────┐   POST /commands/<group>   ┌──────────────────────┐
 Vue client │ gembase.io-   │ ─────────────────────────▶ │ Flask command API    │
            │ client        │ ◀───────────────────────── │ (src/routes, server_ │
            └───────────────┘   {state, payload}         │  core/commands)      │
                                                         └─────────┬────────────┘
                                                                   │ queue request
                                                                   ▼
                                       ┌───────────────────────────────────────────┐
                                       │ MySQL                                     │
                                       │  gb_app · gb_platform · gb_calc · ...     │
                                       └───────────────┬───────────────────────────┘
                                                       │ poll queue / write results
                                                       ▼
                         ┌──────────────────────────────────────────────────────┐
                         │ Background processes (run_process.py)                │
                         │  • platform values calc service (evaluations)        │
                         │  • cron jobs (trending games, backups, scraping)     │
                         └──────────────────────────────────────────────────────┘
```

### Command-based API
Instead of many REST routes, the server exposes a few endpoints grouped by session type
(`/commands/public`, `/commands/users`, `/commands/portal`, `/commands/calc`, `/commands/billing`,
`/commands/studies`, survey endpoints). Each request carries a command id and a payload:

```json
{ "data": { "id": "get_competitors", "payload": { "...": "..." } } }
```

Handlers register with a decorator (`@handler.register("command_id")`) in
`server_core/commands/commands_handler.py`, which also takes care of the session, the transaction
(commit / rollback), per-user rate limiting and ID obfuscation for every command in one place.

### Background calculation service
Evaluations are too heavy for a request/response cycle. The API only writes a request into
`gb_calc.requests_queue`; `src/processes/platform_values_calc_service.py` runs as a separate process,
picks requests up, runs each calculation in its own thread and reports progress back to the DB, so the
client can poll it. The service is woken through a shared-memory flag, de-duplicates identical requests by
hash and cancels a user's outdated requests when they change inputs.

Every background job is started through `run_process.py`, which registers the process in
`gb_server_processes.processes`, keeps a heartbeat (CPU / memory / disk) and lets an admin force-kill it.

## Engineering highlights

- **Fast tag matching at scale** — ~40,000 apps × hundreds of tags. Tags are encoded as binary data so
  matching becomes bitwise operations in MySQL; for the heaviest queries the execution plan is forced
  where the optimizer chose badly.
- **Precomputation and caching** — results are calculated ahead of time in worker processes, so
  the UI doesn't wait on heavy analysis.
- **Structured output from LLMs** — iterated prompts until tagging thousands of apps returned
  consistently parseable data (see the prompt notebooks in `data/jupyter/`).
- **Per-client ID obfuscation** — `src/models/user_obfuscator.py` transforms every app / developer / tag
  id into a value unique to each client (on the way out *and* back in), so the dataset can't be
  enumerated or cross-referenced between accounts.
- **Scraping behind anti-bot protection** — google-play-scraper extended, plus a real Chrome via
  Selenium in a virtual display (Xvfb) for pages it couldn't handle.

## Project structure

```
app.py                  Imports env + Flask app (src.app.app_init)
run_process.py          Launcher for background processes (heartbeat, kill switch)
server_core/            Framework-level code: commands, DB connection, JWT, logging, OpenAI wrapper
src/
  app/                  Flask app init, error responses
  routes/web_server/    Command endpoints
  db/                   DB models (games, users, surveys, billing...)
    calc/               Evaluation logic: competitors, audience angles, gaps & opportunities
  processes/            Calc service, cron, process arguments
  models/               Obfuscator, emails, scraper, web search
  session/              Public / user sessions
  servers/webserver/    Gunicorn entry point
data/
  jupyter/              Notebooks used for scraping, LLM tagging, survey processing, data fixes
  docs/                 Server / MySQL / nginx setup notes
```

## Running locally

> The MySQL schema and data are not part of this repository, so the server won't do much
> without a database dump.

```bash
python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file in the project root (it is git-ignored):

```ini
APP_ENV=dev                 # dev | test | prod
SERVER_URL=http://localhost:5000
CLIENT_URL=http://localhost:5173
FLASK_SECRET_KEY=change-me
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=gembase
DB_PASS=change-me
EMAIL_SENDER_ADDRESS=
EMAIL_SENDER_PASSWORD=
GOOGLE_SEARCH_CREDENTIALS=
IS_DEBUG=1
```

Start the API:

```bash
flask --app src.servers.webserver.server run --debug                 # development
gunicorn -w 4 -b 127.0.0.1:8000 src.servers.webserver.server:server  # production, behind nginx
```

Run a background process:

```bash
python run_process.py -s <script_path> [-u <user_id>] [-e <env>] [-k] [-x] [-d]
```

## License

MIT — see [LICENSE](LICENSE).
