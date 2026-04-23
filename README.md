# Nier Save Editor

Web-based editor for the SQLite save database used by the local `lunar-tear` server project.

It is designed for browsing and editing player-state tables with extra quality-of-life features:

- live schema-driven table browsing
- user-scoped filtering
- inline ID annotations
- `nierrein`-style English lookup resolution for common game entities
- quick editing for save rows in the browser
- Python 3 backend with dropdown-backed lookups sourced from `Engels/example-output`

## External Dependencies

This repository is not fully standalone. At runtime it expects:

- A SQLite save database such as `game.db`
- Extracted lookup JSON under `../Engels/example-output`, or a custom path passed with `--extraction-output`
- `EntityMMomBannerTable.json` from the game asset dump, or a custom path passed with `--mom-banner-table`

The following assets are already bundled in this repo and do not need to be downloaded separately:

- `web/` frontend files
- `images/` preview and reference assets used by the editor UI

No third-party Python packages are required for basic table browsing and save editing.
Optional master-data patching features additionally require:

- `msgpack`
- `lz4`
- `pycryptodome`

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
- Provides dropdowns for extracted entity IDs and user-owned UUID references
- Derives dynamic deck lookups from the selected user's current `user_deck_characters`, `user_deck_sub_weapons`, and `user_deck_parts`

## Requirements

- Python 3.11+
- Access to a `lunar-tear` checkout or equivalent dumped asset directory containing:
  - `server/db/game.db`
  - `assets/master_data`
  - `assets/revisions`
- Generated extractor outputs under `../Engels/example-output`

## Repository Setup

### 1. Clone or create the repo

If you are starting from this folder locally:

```bash
cd "Nier Save Editor"
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

### 2. Refresh extractor data

```bash
python3 ../Engels/extract_possessions.py
```

If you want the optional master-data patching features enabled:

```bash
python3 -m pip install -r requirements-optional.txt
```

### 3. Run the editor

With the default layout, the editor will automatically use the first DB it finds from:

- `./game.db`
- `../lunar-tear/server/db/game.db`

So the common startup command is just:

```bash
python3 app.py
```

If you want to target a specific DB explicitly, use:

```bash
python3 app.py \
  --db ../lunar-tear/server/db/game.db \
  --addr 127.0.0.1:8081 \
  --extraction-output ../Engels/example-output
```

Then open:

```text
http://127.0.0.1:8081
```

## Testing

```bash
python3 -m py_compile app.py
node --check web/app.js
```

## Recommended Project Layout

This editor expects a layout similar to:

```text
workspace/
├── Engels/
│   ├── extract_possessions.py
│   └── example-output/
├── lunar-tear/
│   └── server/
│       ├── db/game.db
│       └── assets/
│           ├── master_data/
│           └── revisions/
└── Nier Save Editor/
    ├── app.py
    ├── images/
    ├── web/
    └── README.md
```

## Notes

- The editor now reads prebuilt lookup JSON from `../Engels/example-output`, which in turn is generated from `master_data` plus English text asset bundles under `assets/revisions`.
- Some localization sources are cleaner than others. When a shipped asset bundle does not decode cleanly, the editor falls back to a structural label.
- SQLite still only allows one writer at a time. Editing a live DB while the server is running can still hit contention.
- `deck_characters` are not treated as a static catalog. Their dropdowns and annotations are built dynamically from the selected user's owned costume, weapon, companion, thought, sub-weapon, and parts UUIDs.
- By default the app looks for sibling `Engels/` and `lunar-tear/` folders, but every external path can be overridden with CLI flags.

## Publishing Tips

- Do not commit your real populated save DB unless you explicitly want it public.
- Keep `game.db`, built binaries, and editor-specific local files out of the repo. The included `.gitignore` already covers the common cases.
- If you publish this separately from `Engels`, make it clear that lookup JSON is generated content and not bundled by default.
- If you want, add screenshots to a `docs/` folder and link them here once the repo is public.
