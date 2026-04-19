# Nier Save Editor

Web-based editor for the SQLite save database used by the local `lunar-tear` server project.

It is designed for browsing and editing player-state tables with extra quality-of-life features:

- live schema-driven table browsing
- user-scoped filtering
- inline ID annotations
- `nierrein`-style English lookup resolution for common game entities
- quick editing for save rows in the browser

## Features

- Reads the live SQLite schema directly from the target save DB
- Lists users with summary stats
- Lets you inspect and edit every table
- Filters `user_*` tables to the selected player
- Resolves many IDs into English labels for:
  - characters
  - costumes
  - weapons
  - companions
  - memoirs / parts
  - debris / thoughts
  - materials
  - consumables
  - important items
- Provides character dropdown selection for `character_id` fields

## Requirements

- Go 1.26+
- `sqlite3` available in your `PATH`
- Access to a `lunar-tear` checkout or equivalent dumped asset directory containing:
  - `assets/master_data`
  - `assets/revisions`

## Repository Setup

### 1. Clone or create the repo

If you are starting from this folder locally:

```bash
cd "/home/brodywelch/cage/Nier Save Editor"
git init
git add .
git commit -m "Initial commit"
```

Then create a new empty GitHub repository and connect it:

```bash
git remote add origin git@github.com:YOUR_USERNAME/nier-save-editor.git
git branch -M main
git push -u origin main
```

If you prefer HTTPS:

```bash
git remote add origin https://github.com/YOUR_USERNAME/nier-save-editor.git
git branch -M main
git push -u origin main
```

## Local Development

### 1. Move into the project

```bash
cd "Nier Save Editor"
```

### 2. Run the editor

Using the local schema-only sample DB in this folder:

```bash
go run . --db ./game.db --addr :8081 --master-data ../lunar-tear/server/assets/master_data
```

Using the live `lunar-tear` player DB:

```bash
go run . \
  --db ../lunar-tear/server/db/game.db \
  --addr :8081 \
  --master-data ../lunar-tear/server/assets/master_data
```

Then open:

```text
http://127.0.0.1:8081
```

## Testing

```bash
GOCACHE=/tmp/go-cache go test ./...
```

## Recommended Project Layout

This editor expects a layout similar to:

```text
workspace/
├── lunar-tear/
│   └── server/
│       ├── db/game.db
│       └── assets/
│           ├── master_data/
│           └── revisions/
└── Nier Save Editor/
    ├── main.go
    ├── lookup.go
    ├── web/
    └── theming/
```

## Notes

- The bundled `game.db` in this repo is useful for schema work, but it may not contain active player data.
- The editor uses `master_data` plus English text asset bundles under `assets/revisions` to resolve IDs into English labels.
- Some localization sources are cleaner than others. When a shipped asset bundle does not decode cleanly, the editor falls back to a structural label.
- SQLite still only allows one writer at a time. Editing a live DB while the server is running can still hit contention.

## Publishing Tips

- Do not commit your real populated save DB unless you explicitly want it public.
- Keep `game.db`, built binaries, and editor-specific local files out of the repo. The included `.gitignore` already covers the common cases.
- If you want, add screenshots to a `docs/` folder and link them here once the repo is public.
