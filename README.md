# Mama's Toolbox

Web-based editor for the SQLite save database used by the local `lunar-tear` server project.

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
- Includes bundled `web/` UI files and `images/` preview assets used by the editor

## Prerequisites

- Python 3.11+
- Access to a `lunar-tear` checkout or equivalent dumped asset directory containing:
  - `server/db/game.db`
  - `assets/master_data`
  - `assets/revisions`
- Generated extractor outputs under `../Engels/output`
- [`lucky0wl-sketch/Engels`](https://github.com/lucky0wl-sketch/Engels) checked out as a sibling folder, or an equivalent custom path passed with `--extraction-output`

No third-party Python packages are required for basic table browsing and save editing.
Optional master-data patching features additionally require:

- `msgpack`
- `lz4`
- `pycryptodome`

## Run

```bash
cd "Mama's Toolbox"
```

Refresh extractor data:

```bash
git clone https://github.com/lucky0wl-sketch/Engels.git ../Engels
python3 ../Engels/extract_possessions.py
```

If you want the optional master-data patching features enabled:

```bash
python3 -m pip install -r requirements-optional.txt
```

Run with defaults:

```bash
python3 app.py
```

Run with explicit paths:

```bash
python3 app.py \
  --db ../lunar-tear/server/db/game.db \
  --addr 127.0.0.1:8081 \
  --extraction-output ../Engels/output
```

Then open:

```text
http://127.0.0.1:8081
```

## Layout

```text
workspace/
├── Engels/
│   ├── extract_possessions.py
│   └── output/
├── lunar-tear/
│   └── server/
│       ├── db/game.db
│       └── assets/
│           ├── master_data/
│           └── revisions/
└── Mama's Toolbox/
    ├── app.py
    ├── images/
    ├── web/
    └── README.md
```

## External Dependencies And Credits

- Lookup JSON comes from [`lucky0wl-sketch/Engels`](https://github.com/lucky0wl-sketch/Engels)
- Runtime game data comes from a local `lunar-tear` checkout or equivalent asset dump
- `EntityMMomBannerTable.json` is required for banner and event selector features unless overridden with `--mom-banner-table`
- Basic operation uses only Python's standard library
- Optional master-data patching uses `msgpack`, `lz4`, and `pycryptodome`

## Notes

- By default the app looks for sibling `Engels/` and `lunar-tear/` folders, but every external path can be overridden with CLI flags.
- SQLite still only allows one writer at a time. Editing a live DB while the server is running can still hit contention.
- When the editor patches quest-related master-memory `.bin.e` data, it creates both a persistent `*.full-backup` file and a timestamped `*.bak-*` backup before writing.
