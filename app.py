#!/usr/bin/env python3
"""Python 3 backend for Mama's Toolbox, a NieR Re[in] save database editor."""

from __future__ import annotations

import argparse
import json
import mimetypes
import re
import sqlite3
import uuid
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_DB_CANDIDATES = (
    SCRIPT_DIR / "game.db",
    SCRIPT_DIR.parent / "lunar-tear" / "server" / "db" / "game.db",
)
DEFAULT_DB_HELP = "./game.db, ../lunar-tear/server/db/game.db"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR.parent / "Engels" / "output"
DEFAULT_MOM_BANNER_PATH = SCRIPT_DIR.parent / "lunar-tear" / "server" / "assets" / "master_data" / "EntityMMomBannerTable.json"
PRESETS_PATH = SCRIPT_DIR / "presets.json"
WEB_DIR = SCRIPT_DIR / "web"
THEME_DIR = SCRIPT_DIR / "theming"
IMAGES_DIR = SCRIPT_DIR / "images"
HOME_PLACEHOLDER = "~"


def default_db_path() -> Path:
    for candidate in DEFAULT_DB_CANDIDATES:
        resolved = candidate.resolve()
        if resolved.is_file():
            try:
                if resolved.stat().st_size <= 0:
                    continue
                with resolved.open("rb") as handle:
                    if handle.read(16) != b"SQLite format 3\x00":
                        continue
            except OSError:
                continue
            return resolved
    return DEFAULT_DB_CANDIDATES[0].resolve()


def resolve_cli_path(path: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (SCRIPT_DIR / path).resolve()


def load_json_file(path: Path) -> Any:
    # Accept UTF-8 files with or without a BOM to avoid platform/editor-specific decode failures.
    return json.loads(path.read_text(encoding="utf-8-sig"))


def display_path(path: Path) -> str:
    try:
        resolved = path.resolve()
        relative = resolved.relative_to(Path.home())
        relative_text = relative.as_posix()
        return HOME_PLACEHOLDER if relative_text == "." else f"{HOME_PLACEHOLDER}/{relative_text}"
    except ValueError:
        return str(path)
    except OSError:
        return str(path)


LOOKUP_FILE_MAP: dict[str, str] = {
    "material_id": "materials.json",
    "consumable_item_id": "consumables.json",
    "weapon_id": "weapons.json",
    "character_id": "characters.json",
    "costume_id": "costumes.json",
    "companion_id": "companions.json",
    "thought_id": "thoughts.json",
    "parts_id": "parts.json",
    "ability_id": "abilities.json",
    "skill_id": "skills.json",
    "important_item_id": "important_items.json",
    "premium_item_id": "premium_items.json",
    "mission_id": "missions.json",
    "quest_id": "quests.json",
    "quest_mission_id": "quest_missions.json",
    "tutorial_type": "tutorials.json",
    "shop_id": "shops.json",
    "shop_item_id": "shop_items.json",
    "gacha_medal_id": "gacha_medals.json",
    "gacha_id": "gacha_banners.json",
    "gift_text_id": "gift_texts.json",
    "character_board_id": "character_boards.json",
    "character_board_ability_id": "character_board_abilities.json",
    "character_board_status_up_id": "character_board_status_ups.json",
    "weapon_awaken_id": "weapon_awakens.json",
    "costume_awaken_ability_id": "costume_awaken_abilities.json",
    "main_quest_chapter_id": "main_quests.json",
    "event_quest_chapter_id": "event_quests.json",
    "extra_quest_id": "extra_quests.json",
    "side_story_quest_id": "side_story_quests.json",
    "cage_ornament_id": "cage_ornament_rewards.json",
}

OPTION_FILE_OVERRIDES: dict[str, str] = {
    "character_id": "playable_characters.json",
    "costume_id": "playable_costumes.json",
}

LOOKUP_ALIASES: dict[str, str] = {
    "favorite_costume_id": "costume_id",
    "dressup_costume_id": "costume_id",
    "description_gift_text_id": "gift_text_id",
}


@dataclass
class ColumnInfo:
    name: str
    type: str
    not_null: bool
    default_sql: str
    is_primary: bool


@dataclass
class TableSchema:
    name: str
    columns: list[ColumnInfo]
    primary_key: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "columns": [
                {
                    "name": column.name,
                    "type": column.type,
                    "notNull": column.not_null,
                    "defaultSql": column.default_sql,
                    "isPrimary": column.is_primary,
                }
                for column in self.columns
            ],
            "primaryKey": self.primary_key,
        }


@dataclass
class TableGroup:
    key: str
    label: str
    tables: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {"key": self.key, "label": self.label, "tables": self.tables}


@dataclass
class LookupEntry:
    label: str
    detail: str = ""
    group: str = ""

    def to_dict(self) -> dict[str, str]:
        payload = {"label": self.label}
        if self.detail:
            payload["detail"] = self.detail
        if self.group:
            payload["group"] = self.group
        return payload


@dataclass
class OwnedEntityRef:
    entry: LookupEntry
    entity_id: str = ""
    limit_break: int = 0
    ability_slots: dict[int, LookupEntry] | None = None
    skill_slots: dict[int, LookupEntry] | None = None
    active_skill_by_limit: dict[int, LookupEntry] | None = None


class LookupRegistry:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.columns: dict[str, dict[str, LookupEntry]] = {}
        self.option_columns: dict[str, dict[str, LookupEntry]] = {}
        self.weapon_skill_slots: dict[str, dict[int, LookupEntry]] = {}
        self.weapon_ability_slots: dict[str, dict[int, LookupEntry]] = {}
        self.costume_active_skills: dict[str, dict[int, LookupEntry]] = {}
        self.summary = {
            "enabled": False,
            "sourcePath": display_path(output_dir),
            "entryCount": 0,
            "kinds": [],
        }
        self._load()

    def _load(self) -> None:
        if not self.output_dir.is_dir():
            return

        for column, file_name in LOOKUP_FILE_MAP.items():
            if column == "gacha_id":
                self.columns[column] = self._load_gacha_lookup_file(file_name)
            else:
                self.columns[column] = self._load_lookup_file(file_name)

        for column, file_name in OPTION_FILE_OVERRIDES.items():
            option_entries = self._load_lookup_file(file_name)
            if option_entries:
                self.option_columns[column] = option_entries

        for column, entries in self.columns.items():
            if column not in self.option_columns:
                self.option_columns[column] = entries

        self.weapon_skill_slots = self._load_slot_lookup("weapon_skills.json", "weapon_id")
        self.weapon_ability_slots = self._load_slot_lookup("weapon_abilities.json", "weapon_id")
        self.costume_active_skills = self._load_slot_lookup(
            "costume_active_skills.json",
            "costume_id",
            limit_field="limit_break_count_lower_limit",
        )

        kinds = sorted(column for column, entries in self.columns.items() if entries)
        self.summary["enabled"] = bool(kinds)
        self.summary["kinds"] = kinds
        self.summary["entryCount"] = sum(len(entries) for entries in self.columns.values())

    def _load_lookup_file(self, file_name: str) -> dict[str, LookupEntry]:
        path = self.output_dir / file_name
        if not path.is_file():
            return {}
        payload = load_json_file(path)
        records = payload.get("records", [])
        entries: dict[str, LookupEntry] = {}
        for record in records:
            record_id = stringify(record.get("id"))
            if not record_id:
                continue
            entries[record_id] = LookupEntry(
                label=record.get("name", record_id),
                detail=detail_from_record(record),
                group=lookup_group_for_record(file_name, record),
            )
        return entries

    def _load_slot_lookup(
        self,
        file_name: str,
        owner_field: str,
        *,
        limit_field: str | None = None,
    ) -> dict[str, dict[int, LookupEntry]]:
        path = self.output_dir / file_name
        if not path.is_file():
            return {}
        payload = load_json_file(path)
        result: dict[str, dict[int, LookupEntry]] = {}
        for record in payload.get("records", []):
            owner_id = stringify(record.get(owner_field))
            if not owner_id:
                continue
            slot_or_limit = to_int(record.get("slot_number", record.get(limit_field or "", 0)))
            result.setdefault(owner_id, {})[slot_or_limit] = LookupEntry(
                label=record.get("name", ""),
                detail=detail_from_record(record),
                group=lookup_group_for_record(file_name, record),
            )
        return result

    def _load_gacha_lookup_file(self, file_name: str) -> dict[str, LookupEntry]:
        path = self.output_dir / file_name
        if not path.is_file():
            return {}
        payload = load_json_file(path)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in payload.get("records", []):
            destination_id = to_int(record.get("DestinationDomainId"))
            if destination_id <= 0:
                continue
            asset_name = str(record.get("BannerAssetName", "") or "")
            if asset_name.startswith("step_up_"):
                gacha_id = str(destination_id // 1000)
            else:
                gacha_id = str(destination_id)
            grouped.setdefault(gacha_id, []).append(record)

        result: dict[str, LookupEntry] = {}
        for gacha_id, records in grouped.items():
            chosen = max(
                records,
                key=lambda record: (
                    1 if record.get("name_found") else 0,
                    to_int(record.get("SortOrderDesc")),
                    -to_int(record.get("id")),
                ),
            )
            mode = "step-up" if str(chosen.get("BannerAssetName", "")).startswith("step_up_") else "banner"
            detail_parts = [
                mode,
                f"banner {chosen.get('id')}",
                f"asset {chosen.get('BannerAssetName')}" if chosen.get("BannerAssetName") else "",
                detail_from_record(chosen),
            ]
            result[gacha_id] = LookupEntry(
                label=chosen.get("name", f"Gacha {gacha_id}"),
                detail=join_detail(*detail_parts),
                group=lookup_group_for_record(file_name, chosen),
            )
        return result

    def resolve_column_entries(self, column: str) -> dict[str, LookupEntry]:
        canonical = LOOKUP_ALIASES.get(column, column)
        if canonical in self.option_columns:
            return self.option_columns[canonical]
        if column in self.option_columns:
            return self.option_columns[column]
        for suffix, entries in self.option_columns.items():
            if column.endswith(suffix):
                return entries
        return {}

    def resolve_annotation(self, column: str, value: Any) -> LookupEntry | None:
        identifier = stringify(value).strip()
        if not identifier or identifier == "0":
            return None
        canonical = LOOKUP_ALIASES.get(column, column)
        if canonical in self.columns and identifier in self.columns[canonical]:
            return self.columns[canonical][identifier]
        if column in self.columns and identifier in self.columns[column]:
            return self.columns[column][identifier]
        for suffix, entries in self.columns.items():
            if column.endswith(suffix) and identifier in entries:
                return entries[identifier]
        return None

    def costume_active_skill_for_limit_break(self, costume_id: str, limit_break: int) -> LookupEntry | None:
        by_limit = self.costume_active_skills.get(costume_id, {})
        chosen: LookupEntry | None = None
        chosen_limit = -1
        for threshold, entry in by_limit.items():
            if threshold <= limit_break and threshold > chosen_limit:
                chosen = entry
                chosen_limit = threshold
        if chosen is not None:
            return chosen
        return by_limit.get(0)


class UserLookupContext:
    def __init__(self) -> None:
        self.columns: dict[str, dict[str, LookupEntry]] = {}
        self.weapon_refs: dict[str, OwnedEntityRef] = {}
        self.costume_refs: dict[str, OwnedEntityRef] = {}
        self.companion_refs: dict[str, OwnedEntityRef] = {}
        self.parts_refs: dict[str, OwnedEntityRef] = {}
        self.thought_refs: dict[str, OwnedEntityRef] = {}
        self.deck_character_refs: dict[str, LookupEntry] = {}

    def set_aliases(self, *column_names: str, entries: dict[str, LookupEntry]) -> None:
        for column_name in column_names:
            self.columns[column_name] = entries

    def resolve_options(self, column: str) -> dict[str, LookupEntry]:
        if column in self.columns:
            return self.columns[column]
        for suffix, entries in self.columns.items():
            if column.endswith(suffix):
                return entries
        return {}

    def resolve_annotation(self, column: str, value: Any) -> LookupEntry | None:
        identifier = stringify(value).strip()
        if not identifier or identifier == "0":
            return None
        entries = self.resolve_options(column)
        return entries.get(identifier)

    def table_annotations(self, table: str, row: dict[str, Any]) -> dict[str, LookupEntry]:
        annotations: dict[str, LookupEntry] = {}
        if table == "user_weapon_abilities":
            ref = self.weapon_refs.get(stringify(row.get("user_weapon_uuid")))
            if ref and ref.ability_slots:
                entry = ref.ability_slots.get(to_int(row.get("slot_number")))
                if entry:
                    annotations["slot_number"] = LookupEntry(label=entry.label)
        elif table == "user_weapon_skills":
            ref = self.weapon_refs.get(stringify(row.get("user_weapon_uuid")))
            if ref and ref.skill_slots:
                entry = ref.skill_slots.get(to_int(row.get("slot_number")))
                if entry:
                    annotations["slot_number"] = LookupEntry(label=entry.label)
        elif table == "user_costume_active_skills":
            ref = self.costume_refs.get(stringify(row.get("user_costume_uuid")))
            if ref and ref.active_skill_by_limit:
                entry = ref.active_skill_by_limit.get(ref.limit_break) or ref.active_skill_by_limit.get(0)
                if entry:
                    annotations["level"] = LookupEntry(label=entry.label)
        return annotations


class EditorApp:
    def __init__(self, db_path: Path, extraction_output_dir: Path, mom_banner_path: Path):
        self.db_path = db_path
        self.extraction_output_dir = extraction_output_dir
        self.mom_banner_path = mom_banner_path
        self.lookup_registry = LookupRegistry(extraction_output_dir)
        self.playable_characters = self._load_record_index("playable_characters.json")
        self.playable_costumes = self._load_record_index("playable_costumes.json")
        self.costumes = self._load_record_index("costumes.json")
        self.consumables = self._load_record_index("consumables.json")
        self.materials = self._load_record_index("materials.json")
        self.weapons = self._load_record_index("weapons.json")
        self.companions = self._load_record_index("companions.json")
        self.parts = self._load_record_index("parts.json")
        self.thoughts = self._load_record_index("thoughts.json")
        self.gacha_banners = self._load_record_index("gacha_banners.json")
        self.gacha_medals = self._load_record_index("gacha_medals.json")
        self.shop_defs = self._load_record_index("shops.json")
        self.shop_items_by_price = self._load_grouped_records("shop_items.json", "PriceId")
        self.main_quest_defs = self._load_record_index("main_quests.json")
        self.quest_defs = self._load_record_index("quests.json")
        self.event_quest_defs = self._load_record_index("event_quests.json")
        self.extra_quest_defs = self._load_record_index("extra_quests.json")
        self.side_story_quest_defs = self._load_record_index("side_story_quests.json")
        self.presets = self._load_presets()
        self.gacha_medal_ids = self._load_gacha_medal_ids()
        self.gacha_medals_by_gacha_id = self._load_gacha_medals_by_gacha_id()
        self.weapon_skill_defs = self._load_grouped_records("weapon_skills.json", "weapon_id")
        self.weapon_ability_defs = self._load_grouped_records("weapon_abilities.json", "weapon_id")
        self.image_index = self._build_image_index()
        self.schema = self.load_schema()
        self.table_groups = build_table_groups(self.schema)

    def _load_record_index(self, file_name: str) -> dict[str, dict[str, Any]]:
        path = self.extraction_output_dir / file_name
        if not path.is_file():
            return {}
        payload = load_json_file(path)
        result: dict[str, dict[str, Any]] = {}
        for record in payload.get("records", []):
            record_id = stringify(record.get("id")).strip()
            if record_id:
                result[record_id] = record
        return result

    def _load_grouped_records(self, file_name: str, key_field: str) -> dict[str, list[dict[str, Any]]]:
        path = self.extraction_output_dir / file_name
        if not path.is_file():
            return {}
        payload = load_json_file(path)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in payload.get("records", []):
            key = stringify(record.get(key_field)).strip()
            if not key:
                continue
            grouped.setdefault(key, []).append(record)
        return grouped

    def _load_presets(self) -> list[dict[str, Any]]:
        if not PRESETS_PATH.is_file():
            return []
        payload = load_json_file(PRESETS_PATH)
        presets = payload.get("presets", []) if isinstance(payload, dict) else []
        return [preset for preset in presets if isinstance(preset, dict)]

    def _build_image_index(self) -> dict[str, dict[str, set[str]]]:
        index: dict[str, dict[str, set[str]]] = {}
        for category in ("costume", "weapon", "companions"):
            root = IMAGES_DIR / category
            category_index: dict[str, set[str]] = {}
            if root.is_dir():
                for folder in root.iterdir():
                    if not folder.is_dir():
                        continue
                    category_index[folder.name] = {child.name for child in folder.iterdir() if child.is_file()}
            index[category] = category_index
        return index

    def _display_path(self, path: Path) -> str:
        return display_path(path)

    def _load_gacha_medal_ids(self) -> set[int]:
        medal_path = self.mom_banner_path.parent / "EntityMGachaMedalTable.json"
        if not medal_path.is_file():
            return set()
        payload = load_json_file(medal_path)
        if not isinstance(payload, list):
            return set()
        result: set[int] = set()
        for row in payload:
            if not isinstance(row, dict):
                continue
            gacha_id = to_int(row.get("ShopTransitionGachaId"))
            if gacha_id > 0:
                result.add(gacha_id)
        return result

    def _load_gacha_medals_by_gacha_id(self) -> dict[int, list[dict[str, Any]]]:
        grouped: dict[int, list[dict[str, Any]]] = {}
        for medal in self.gacha_medals.values():
            gacha_id = to_int(medal.get("ShopTransitionGachaId"))
            if gacha_id <= 0:
                continue
            grouped.setdefault(gacha_id, []).append(medal)
        for medals in grouped.values():
            medals.sort(key=lambda record: to_int(record.get("id")))
        return grouped

    def _featured_shop_labels_for_price(self, price_id: int) -> list[str]:
        if price_id <= 0:
            return []

        character_labels: list[str] = []
        reward_labels: list[str] = []
        for item in self.shop_items_by_price.get(str(price_id), []):
            contents = item.get("contents", [])
            if not isinstance(contents, list):
                continue
            for content in contents:
                if not isinstance(content, dict):
                    continue
                possession_type = to_int(content.get("possession_type"))
                possession_id = to_int(content.get("possession_id"))
                if possession_id <= 0:
                    continue
                if possession_type == 1:
                    costume = self.playable_costumes.get(str(possession_id)) or self.costumes.get(str(possession_id), {})
                    character_name = stringify(costume.get("character_name")).strip()
                    costume_name = stringify(costume.get("name")).strip()
                    if character_name:
                        character_labels.append(character_name)
                    elif costume_name and not re.match(r"^Costume\s+\d+$", costume_name):
                        reward_labels.append(costume_name)
                    continue
                if possession_type == 2:
                    weapon = self.weapons.get(str(possession_id), {})
                    weapon_name = stringify(weapon.get("name")).strip()
                    if weapon_name and not re.match(r"^Weapon\s+\d+$", weapon_name):
                        reward_labels.append(weapon_name)

        preferred = character_labels or reward_labels
        return list(dict.fromkeys(label for label in preferred if label))

    def _normalize_banner_theme_label(self, label: str) -> str:
        text = stringify(label).replace("\n", " ").strip()
        if not text:
            return ""
        shard_match = re.fullmatch(r"Shard\s*\((.+)\)", text, flags=re.IGNORECASE)
        if shard_match:
            text = shard_match.group(1).strip()
        text = re.sub(r"\s+Exchange$", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"\s+", " ", text)
        return text

    def _trim_banner_theme_suffix(self, label: str) -> str:
        text = stringify(label).strip()
        prefix, _, suffix = text.rpartition(" ")
        if prefix and re.fullmatch(r"[A-Z][A-Z0-9]*(?:/[A-Z][A-Z0-9]*)*", suffix):
            return prefix.strip()
        return text

    def _infer_gacha_banner_identity(self, row: dict[str, Any]) -> dict[str, Any]:
        destination_id = to_int(row.get("DestinationDomainId"))
        if destination_id <= 0:
            return {}

        for medal in self.gacha_medals_by_gacha_id.get(destination_id, []):
            medal_id = to_int(medal.get("id"))
            medal_name = stringify(medal.get("name")).strip()
            shop_name = stringify((self.shop_defs.get(str(medal_id)) or {}).get("name")).strip()
            featured_labels = self._featured_shop_labels_for_price(medal_id)
            theme_label = self._normalize_banner_theme_label(medal_name or shop_name)
            if not theme_label:
                theme_label = self._normalize_banner_theme_label(shop_name)
            if not theme_label:
                continue

            trimmed_theme = self._trim_banner_theme_suffix(theme_label) if featured_labels else theme_label
            label = trimmed_theme
            if featured_labels:
                featured_summary = "/".join(featured_labels)
                if re.search(r"\bsummons?\b", trimmed_theme, flags=re.IGNORECASE):
                    label = f"{trimmed_theme} {featured_summary}"
                else:
                    label = f"{trimmed_theme} Summons {featured_summary}"

            detail_parts = [
                "label inferred from medal exchange",
                medal_name,
                shop_name,
            ]
            if featured_labels:
                detail_parts.append("featured " + ", ".join(featured_labels))

            return {
                "label": label,
                "detail": join_detail(*detail_parts),
                "source": "medal-exchange",
            }

        return {}

    def _mom_banner_backup_candidates(self) -> list[Path]:
        if not self.mom_banner_path.name:
            return []
        stable = self.mom_banner_path.with_name(f"{self.mom_banner_path.name}.full-backup")
        candidates: list[Path] = []
        if stable.is_file():
            candidates.append(stable)
        candidates.extend(
            sorted(
                self.mom_banner_path.parent.glob(f"{self.mom_banner_path.name}.bak-*"),
                key=lambda path: path.stat().st_mtime,
                reverse=True,
            )
        )
        return candidates

    def _mom_banner_logic_source_path(self) -> Path:
        try:
            base = self.mom_banner_path.parents[2]
        except IndexError:
            base = self.mom_banner_path.parent
        return base / "internal" / "masterdata" / "gacha.go"

    def _master_data_backup_candidates(self, path: Path) -> list[Path]:
        stable = path.with_name(f"{path.name}.full-backup")
        candidates: list[Path] = []
        if stable.is_file():
            candidates.append(stable)
        candidates.extend(
            sorted(
                path.parent.glob(f"{path.name}.bak-*"),
                key=lambda current: current.stat().st_mtime,
                reverse=True,
            )
        )
        return candidates

    def _master_data_catalog_source(self, path: Path) -> Path:
        for candidate in self._master_data_backup_candidates(path):
            if candidate.is_file():
                return candidate
        return path

    def _ensure_master_data_backup(self, path: Path, source_path: Path) -> None:
        stable_backup = path.with_name(f"{path.name}.full-backup")
        if source_path == path and path.is_file() and not stable_backup.exists():
            stable_backup.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")

    def _backup_timestamp(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")

    def _master_memory_database_path(self) -> Path:
        release_dir = self.mom_banner_path.parent.parent / "release"
        versioned = sorted(
            path
            for path in release_dir.glob("*.bin.e")
            if path.name != "database.bin.e"
        )
        if versioned:
            return versioned[-1]
        fallback = release_dir / "database.bin.e"
        return fallback

    def _ensure_binary_backup(self, path: Path) -> None:
        stable_backup = path.with_name(f"{path.name}.full-backup")
        if path.is_file() and not stable_backup.exists():
            stable_backup.write_bytes(path.read_bytes())
        if path.is_file():
            timestamped = path.with_name(f"{path.name}.bak-{self._backup_timestamp()}")
            timestamped.write_bytes(path.read_bytes())

    def _master_memory_table_schema(self, table_name: str) -> list[str]:
        schemas = {
            "m_event_quest_chapter": [
                "EventQuestChapterId",
                "EventQuestType",
                "SortOrder",
                "NameEventQuestTextId",
                "BannerAssetId",
                "EventQuestLinkId",
                "EventQuestDisplayItemGroupId",
                "EventQuestSequenceGroupId",
                "StartDatetime",
                "EndDatetime",
                "DisplaySortOrder",
            ],
            "m_event_quest_sequence_group": [
                "EventQuestSequenceGroupId",
                "DifficultyType",
                "EventQuestSequenceId",
            ],
            "m_event_quest_sequence": [
                "EventQuestSequenceId",
                "SortOrder",
                "QuestId",
            ],
            "m_side_story_quest": [
                "SideStoryQuestId",
                "SideStoryQuestType",
                "TargetId",
            ],
            "m_side_story_quest_limit_content": [
                "SideStoryQuestLimitContentId",
                "CharacterId",
                "EventQuestChapterId",
                "DifficultyType",
                "NextSideStoryQuestId",
            ],
            "m_side_story_quest_scene": [
                "SideStoryQuestId",
                "SideStoryQuestSceneId",
                "SortOrder",
                "AssetBackgroundId",
                "EventMapNumberUpper",
                "EventMapNumberLower",
            ],
            "m_webview_mission": [
                "WebviewMissionId",
                "TitleTextId",
                "WebviewMissionType",
                "WebviewMissionTargetId",
                "StartDatetime",
                "EndDatetime",
            ],
            "m_webview_panel_mission": [
                "WebviewPanelMissionId",
                "Page",
                "WebviewPanelMissionPageId",
                "StartDatetime",
                "EndDatetime",
            ],
            "m_event_quest_labyrinth_season": [
                "EventQuestChapterId",
                "SeasonNumber",
                "StartDatetime",
                "EndDatetime",
                "SeasonRewardGroupId",
            ],
        }
        schema = schemas.get(table_name)
        if not schema:
            raise ValueError(f"unsupported master-memory table: {table_name}")
        return schema

    def _encode_master_memory_table(self, table_name: str, rows: list[dict[str, Any]]) -> bytes:
        try:
            import lz4.block
            import msgpack
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "master data patching requires the Python packages msgpack and lz4"
            ) from exc

        schema = self._master_memory_table_schema(table_name)
        array_rows = [
            [to_int(row.get(field_name)) for field_name in schema]
            for row in rows
            if isinstance(row, dict)
        ]
        raw_payload = msgpack.packb(array_rows, use_bin_type=True)
        payload = msgpack.packb(len(raw_payload), use_bin_type=True) + lz4.block.compress(
            raw_payload,
            store_size=False,
        )
        return msgpack.packb(msgpack.ExtType(99, payload), use_bin_type=True)

    def _patch_master_memory_tables(self, replacements: dict[str, list[dict[str, Any]]]) -> None:
        if not replacements:
            return

        try:
            from Crypto.Cipher import AES
            import msgpack
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "master data patching requires the Python packages pycryptodome and msgpack"
            ) from exc

        database_path = self._master_memory_database_path()
        if not database_path.is_file():
            raise FileNotFoundError(f"master data database not found: {database_path}")

        encrypted = database_path.read_bytes()
        cipher = AES.new(b"6Cb01321EE5e6bBe", AES.MODE_CBC, b"EfcAef4CAe5f6DaA")
        plaintext = cipher.decrypt(encrypted)
        pad_size = plaintext[-1]
        if pad_size < 1 or pad_size > 16 or plaintext[-pad_size:] != bytes([pad_size]) * pad_size:
            raise ValueError(f"unexpected PKCS7 padding in {database_path}")
        plaintext = plaintext[:-pad_size]

        unpacker = msgpack.Unpacker(raw=False, strict_map_key=False)
        unpacker.feed(plaintext)
        header_raw = next(unpacker)
        blob = plaintext[unpacker.tell():]
        header = {
            stringify(name): (to_int(offset_size[0]), to_int(offset_size[1]))
            for name, offset_size in header_raw.items()
            if isinstance(offset_size, (list, tuple)) and len(offset_size) == 2
        }
        missing_tables = sorted(table_name for table_name in replacements if table_name not in header)
        if missing_tables:
            raise ValueError(
                "master data database is missing tables: "
                + ", ".join(missing_tables)
            )

        encoded_replacements = {
            table_name: self._encode_master_memory_table(table_name, rows)
            for table_name, rows in replacements.items()
        }

        new_blob = bytearray()
        new_header: dict[str, list[int]] = {}
        for table_name, (offset, size) in header.items():
            table_blob = encoded_replacements.get(table_name)
            if table_blob is None:
                table_blob = blob[offset:offset + size]
            new_header[table_name] = [len(new_blob), len(table_blob)]
            new_blob.extend(table_blob)

        rebuilt_plaintext = msgpack.packb(new_header, use_bin_type=True) + bytes(new_blob)
        self._ensure_binary_backup(database_path)
        pad_size = 16 - (len(rebuilt_plaintext) % 16)
        rebuilt_plaintext += bytes([pad_size]) * pad_size
        encrypted = AES.new(b"6Cb01321EE5e6bBe", AES.MODE_CBC, b"EfcAef4CAe5f6DaA").encrypt(rebuilt_plaintext)
        database_path.write_bytes(encrypted)

    def _load_master_memory_tables(self, table_names: list[str]) -> dict[str, list[dict[str, Any]]]:
        try:
            from Crypto.Cipher import AES
            import lz4.block
            import msgpack
        except ModuleNotFoundError:
            return {table_name: [] for table_name in table_names}

        database_path = self._master_memory_database_path()
        if not database_path.is_file():
            return {table_name: [] for table_name in table_names}

        encrypted = database_path.read_bytes()
        plaintext = AES.new(b"6Cb01321EE5e6bBe", AES.MODE_CBC, b"EfcAef4CAe5f6DaA").decrypt(encrypted)
        pad_size = plaintext[-1]
        if pad_size < 1 or pad_size > 16 or plaintext[-pad_size:] != bytes([pad_size]) * pad_size:
            return {table_name: [] for table_name in table_names}
        plaintext = plaintext[:-pad_size]

        unpacker = msgpack.Unpacker(raw=False, strict_map_key=False)
        unpacker.feed(plaintext)
        header_raw = next(unpacker)
        blob = plaintext[unpacker.tell():]
        header = {
            stringify(name): (to_int(offset_size[0]), to_int(offset_size[1]))
            for name, offset_size in header_raw.items()
            if isinstance(offset_size, (list, tuple)) and len(offset_size) == 2
        }

        result: dict[str, list[dict[str, Any]]] = {}
        for table_name in table_names:
            offset, size = header.get(table_name, (0, 0))
            if size <= 0:
                result[table_name] = []
                continue
            ext = msgpack.unpackb(blob[offset:offset + size], raw=False, strict_map_key=False)
            ext_unpacker = msgpack.Unpacker(raw=False, strict_map_key=False)
            ext_unpacker.feed(ext.data)
            uncompressed_size = next(ext_unpacker)
            compressed_payload = ext.data[ext_unpacker.tell():]
            rows = msgpack.unpackb(
                lz4.block.decompress(compressed_payload, uncompressed_size=uncompressed_size),
                raw=False,
                strict_map_key=False,
            )
            schema = self._master_memory_table_schema(table_name)
            result[table_name] = [
                {
                    field_name: row[index]
                    for index, field_name in enumerate(schema)
                    if index < len(row)
                }
                for row in rows
                if isinstance(row, list)
            ]
        return result

    def _mom_banner_catalog_source(self) -> Path:
        for candidate in self._mom_banner_backup_candidates():
            if candidate.is_file():
                return candidate
        return self.mom_banner_path

    def _load_mom_banner_rows(self, path: Path) -> list[dict[str, Any]]:
        if not path.is_file():
            return []
        payload = load_json_file(path)
        if not isinstance(payload, list):
            return []
        return [row for row in payload if isinstance(row, dict)]

    def _event_selector_paths(self) -> dict[str, dict[str, Path]]:
        root = self.mom_banner_path.parent
        return {
            "event_quests": {
                "chapters": root / "EntityMEventQuestChapterTable.json",
                "sequence_groups": root / "EntityMEventQuestSequenceGroupTable.json",
                "sequences": root / "EntityMEventQuestSequenceTable.json",
                "labyrinth_seasons": root / "EntityMEventQuestLabyrinthSeasonTable.json",
                "webview_missions": root / "EntityMWebviewMissionTable.json",
                "webview_panel_missions": root / "EntityMWebviewPanelMissionTable.json",
            },
            "extra_quests": {
                "groups": root / "EntityMExtraQuestGroupTable.json",
                "chapter_map": root / "EntityMExtraQuestGroupInMainQuestChapterTable.json",
            },
            "side_story_quests": {
                "quests": root / "EntityMSideStoryQuestTable.json",
                "limits": root / "EntityMSideStoryQuestLimitContentTable.json",
                "scenes": root / "EntityMSideStoryQuestSceneTable.json",
            },
        }

    def _is_unknown_event_label(self, label: str) -> bool:
        text = stringify(label).strip()
        if not text:
            return True
        return bool(re.fullmatch(r"Event Quest(?: Chapter)? \d+", text))

    def _event_child_quest_names(self, extracted: dict[str, Any]) -> list[str]:
        names: list[str] = []
        for difficulty in extracted.get("difficulties", []):
            if not isinstance(difficulty, dict):
                continue
            for quest in difficulty.get("quests", []):
                if not isinstance(quest, dict):
                    continue
                quest_name = stringify(quest.get("quest_name")).strip()
                if not quest_name:
                    continue
                if re.fullmatch(r"Quest(?: \d+)?", quest_name):
                    continue
                if quest_name not in names:
                    names.append(quest_name)
        return names

    def _character_name_for_event_chapter(self, chapter_id: int, chapter_character_map: dict[int, int]) -> str:
        character_id = chapter_character_map.get(chapter_id, 0)
        if character_id <= 0:
            return ""
        record = self.playable_characters.get(str(character_id), {})
        return stringify(record.get("name")).strip()

    def _event_quest_names_are_plain_placeholder(self, quest_names: list[str]) -> bool:
        normalized = [stringify(name).strip() for name in quest_names if stringify(name).strip()]
        if not normalized:
            return False
        return all(
            name == "QUEST" or bool(re.fullmatch(r"Quest(?: \d+)?", name))
            for name in normalized
        )

    def _infer_daily_rotation_identity(self, target_rows: list[dict[str, Any]]) -> dict[str, Any]:
        target_ids = sorted(
            {
                to_int(row.get("EventQuestDailyGroupTargetChapterId"))
                for row in target_rows
                if to_int(row.get("EventQuestDailyGroupTargetChapterId")) > 0
            }
        )
        sort_orders = sorted(
            {
                to_int(row.get("SortOrder"))
                for row in target_rows
                if to_int(row.get("SortOrder")) > 0
            }
        )
        if not target_ids:
            return {}

        if len(target_ids) == 1:
            family = f"Daily Rotation Set {target_ids[0]}"
        else:
            family = f"Daily Rotation Sets {target_ids[0]}-{target_ids[-1]}"

        if sort_orders == [2]:
            label = f"{family} Vol.1"
        elif sort_orders == [3]:
            label = f"{family} Vol.2"
        else:
            label = family

        detail = join_detail(
            "label inferred from daily rotation target mapping",
            "targets " + ", ".join(str(value) for value in target_ids),
            "slots " + ", ".join(str(value) for value in sort_orders) if sort_orders else "",
        )
        return {
            "label": label,
            "detail": detail,
            "source": "daily-rotation",
            "family": family,
            "tags": [family, "daily rotation"],
        }

    def _infer_event_quest_identity(
        self,
        chapter_id: int,
        extracted: dict[str, Any],
        chapter_row: dict[str, Any],
        *,
        library_titles: dict[int, str],
        banner_titles: dict[int, str],
        display_titles: dict[int, str],
        chapter_character_map: dict[int, int],
        limit_content_map: dict[int, int],
        limit_content_defs: dict[int, dict[str, Any]],
        webview_titles: dict[int, str],
        webview_start_titles: dict[int, str],
        anecdote_title_fallbacks: dict[int, str],
        daily_target_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        current_label = stringify(extracted.get("name")).strip()
        if current_label and not self._is_unknown_event_label(current_label):
            return {}

        event_type = to_int(chapter_row.get("EventQuestType"))
        banner_asset_id = to_int(chapter_row.get("BannerAssetId"))
        display_group_id = to_int(chapter_row.get("EventQuestDisplayItemGroupId"))
        quest_names = self._event_child_quest_names(extracted)
        character_name = self._character_name_for_event_chapter(chapter_id, chapter_character_map)

        family_label = stringify(library_titles.get(chapter_id)).strip()
        if family_label:
            detail = join_detail(
                "label inferred from story library grouping",
                f"family {family_label}",
            )
            return {
                "label": family_label,
                "detail": detail,
                "source": "story-library",
                "family": family_label,
                "tags": [f"family {family_label}"],
            }

        family_label = stringify(banner_titles.get(chapter_id)).strip()
        if family_label:
            detail = join_detail(
                "label inferred from shared event banner asset",
                f"family {family_label}",
            )
            return {
                "label": family_label,
                "detail": detail,
                "source": "shared-banner",
                "family": family_label,
                "tags": [f"family {family_label}"],
            }

        family_label = stringify(display_titles.get(chapter_id)).strip()
        if family_label:
            detail = join_detail(
                "label inferred from shared event display group",
                f"family {family_label}",
            )
            return {
                "label": family_label,
                "detail": detail,
                "source": "shared-display",
                "family": family_label,
                "tags": [f"family {family_label}"],
            }

        if event_type == 11:
            limit_content_id = limit_content_map.get(chapter_id, 0)
            limit_content = limit_content_defs.get(limit_content_id, {})
            costume_name = ""
            character_family_name = character_name
            costume_id = to_int(limit_content.get("CostumeId"))
            if costume_id > 0:
                costume_record = self.playable_costumes.get(str(costume_id), {}) or self.costumes.get(str(costume_id), {})
                costume_name = stringify(costume_record.get("name")).strip()
                if not character_family_name:
                    character_family_name = stringify(costume_record.get("character_name")).strip()
            difficulty_suffix = {
                1: "Easy",
                2: "Normal",
                3: "Hard",
                4: "Master",
            }.get(chapter_id % 10, "")
            label = "Chambers of Dusk"
            if difficulty_suffix:
                label = f"{label}: {difficulty_suffix}"
            if costume_name:
                label = f"{label} - {costume_name}"
            detail = join_detail(
                "label inferred from limit-content mapping",
                f"limit content {limit_content_id}" if limit_content_id else "",
                costume_name,
            )
            tags = ["family Chambers of Dusk"]
            if character_family_name:
                tags.append(f"character {character_family_name}")
            return {
                "label": label,
                "detail": detail,
                "source": "limit-content",
                "family": "Chambers of Dusk",
                "character": character_family_name,
                "tags": tags,
            }

        if event_type == 7 and character_name:
            label = f"Dark Lair: {character_name}"
            detail = join_detail(
                "label inferred from character event mapping",
                "Dark Lair family",
                character_name,
            )
            return {
                "label": label,
                "detail": detail,
                "source": "character-dark-lair",
                "family": "Dark Lair",
                "character": character_name,
                "tags": ["family Dark Lair", f"character {character_name}"],
            }

        if event_type == 6 and character_name:
            label = f"Character Event: {character_name}"
            detail = join_detail(
                "label inferred from character event mapping",
                character_name,
            )
            return {
                "label": label,
                "detail": detail,
                "source": "character-event",
                "family": "Character Event",
                "character": character_name,
                "tags": ["family Character Event", f"character {character_name}"],
            }

        if event_type == 8 and any("Daily Challenge" in name for name in quest_names):
            detail = join_detail(
                "label inferred from child quest names",
                ", ".join(quest_names[:3]),
            )
            return {
                "label": "Daily Challenge",
                "detail": detail,
                "source": "child-quests",
                "family": "Daily Challenge",
                "tags": ["family Daily Challenge"],
            }

        if event_type == 4 and quest_names:
            detail = join_detail(
                "label inferred from child quest names",
                ", ".join(quest_names[:4]),
            )
            return {
                "label": "Daily Quest & Weekend Special Quests",
                "detail": detail,
                "source": "child-quests",
                "family": "Daily Quest",
                "tags": ["family Daily Quest", "weekend special"],
            }

        if event_type == 9:
            if daily_target_rows and self._event_quest_names_are_plain_placeholder(quest_names):
                inferred_daily_rotation = self._infer_daily_rotation_identity(daily_target_rows)
                if inferred_daily_rotation:
                    inferred_daily_rotation["tags"] = [
                        tag
                        for tag in (
                            inferred_daily_rotation.get("tags", [])
                            + ["family Daily / Routine"]
                        )
                        if tag
                    ]
                    return inferred_daily_rotation
            if banner_asset_id == 1044:
                detail = join_detail(
                    "label inferred from shared event banner asset",
                    "family Once per Day Mama's Dream Summons Special Quest",
                )
                return {
                    "label": "Once per Day Mama's Dream Summons Special Quest",
                    "detail": detail,
                    "source": "shared-banner",
                    "family": "Once per Day Mama's Dream Summons Special Quest",
                    "tags": ["family Once per Day Mama's Dream Summons Special Quest"],
                }
            if banner_asset_id == 1000:
                detail = join_detail(
                    "label inferred from shared event banner asset",
                    "family New Chapter Enhancement Quests",
                )
                return {
                    "label": "New Chapter Enhancement Quests",
                    "detail": detail,
                    "source": "shared-banner",
                    "family": "New Chapter Enhancement Quests",
                    "tags": ["family New Chapter Enhancement Quests"],
                }
            if chapter_id in webview_titles:
                family_label = stringify(webview_titles.get(chapter_id)).strip()
                if family_label:
                    return {
                        "label": family_label,
                        "detail": join_detail(
                            "label inferred from matching Fate/Daily-style webview mission window",
                            f"family {family_label}",
                        ),
                        "source": "webview-window",
                        "family": family_label,
                        "tags": [f"family {family_label}"],
                    }
            if self._event_quest_names_are_plain_placeholder(quest_names):
                return {
                    "label": f"Unidentified Daily Quest {chapter_id}",
                    "detail": join_detail(
                        "no stable localized title found",
                        f"banner asset {banner_asset_id}" if banner_asset_id else "",
                        f"display group {display_group_id}" if display_group_id else "",
                    ),
                    "source": "daily-placeholder",
                    "family": "Daily / Routine",
                    "tags": ["family Daily / Routine", "unidentified"],
                }

        if event_type == 3:
            family_label = stringify(webview_titles.get(chapter_id)).strip()
            if not family_label:
                family_label = stringify(webview_start_titles.get(chapter_id)).strip()
            if family_label:
                return {
                    "label": family_label,
                    "detail": join_detail(
                        "label inferred from matching Anecdote webview mission window",
                        f"family {family_label}",
                    ),
                    "source": "webview-window",
                    "family": family_label,
                    "tags": [f"family {family_label}", "anecdote-linked"],
                }
            family_label = stringify(anecdote_title_fallbacks.get(chapter_id)).strip()
            if family_label:
                return {
                    "label": family_label,
                    "detail": join_detail(
                        "label inferred from remaining Anecdote panel mission titles after exact window matches",
                        f"family {family_label}",
                    ),
                    "source": "anecdote-panel-order",
                    "family": family_label,
                    "tags": [f"family {family_label}", "anecdote-linked", "medium confidence"],
                }
            if 220001 <= chapter_id <= 220007:
                return {
                    "label": f"Anecdote Family Entry {chapter_id - 220000}",
                    "detail": join_detail(
                        "label inferred from Anecdote card-story mission chain",
                        f"stained glass {chapter_id}",
                        f"panel mission progression {chapter_id - 220000}",
                    ),
                    "source": "anecdote-family",
                    "family": "Anecdotes",
                    "tags": ["family Anecdotes", "anecdote-linked"],
                }

        if quest_names:
            primary = quest_names[0]
            detail = join_detail(
                "label inferred from child quest names",
                ", ".join(quest_names[:4]),
            )
            return {
                "label": primary if primary != "QUEST" else f"Unidentified Event Quest {chapter_id}",
                "detail": detail,
                "source": "child-quests" if primary != "QUEST" else "child-placeholder",
            }

        return {}

    def _quest_selector_record(
        self,
        extracted: dict[str, Any],
        *,
        active: bool,
        detail_parts: list[str],
        row_count: int,
        file_group: str,
    ) -> dict[str, Any]:
        identifier = to_int(extracted.get("id"))
        name_found = bool(extracted.get("name_found")) and not self._is_unknown_event_label(stringify(extracted.get("name")).strip())
        return {
            "id": identifier,
            "label": stringify(extracted.get("name")).strip() or f"Entry {identifier}",
            "detail": join_detail(*detail_parts),
            "group": lookup_group_for_record(f"{file_group}.json", extracted),
            "isActive": active,
            "rowCount": row_count,
            "nameFound": name_found,
        }

    def _event_category_label(self, event_type: int) -> str:
        return {
            1: "Story Records",
            2: "Variations",
            3: "Special / Misc",
            4: "Daily / Routine",
            5: "Guerrilla",
            6: "Special / Misc",
            7: "Special / Misc",
            8: "Special / Misc",
            9: "Daily / Routine",
            10: "Tower",
            11: "Limit Content",
            12: "Fate Board / Labyrinth",
        }.get(event_type, "Special / Misc")

    def _event_category_override(self, base_category: str, family_label: str, tags: list[str]) -> str:
        family = stringify(family_label).strip()
        tag_set = {stringify(tag).strip() for tag in tags if stringify(tag).strip()}
        family_map = {
            "Dark Lair": "Dark Lair",
            "Chambers of Dusk": "Chambers of Dusk",
            "Daily Challenge": "Daily Challenge",
            "Character Event": "Character Events",
            "Anecdotes": "Anecdotes",
        }
        if family in family_map:
            return family_map[family]
        if family.startswith("Anecdote:"):
            return "Anecdotes"
        if "anecdote-linked" in tag_set:
            return "Anecdotes"
        return base_category

    def event_selector_catalog(self) -> dict[str, Any]:
        paths = self._event_selector_paths()
        live_master_tables = self._load_master_memory_tables(
            [
                "m_event_quest_chapter",
                "m_side_story_quest",
                "m_webview_mission",
            ]
        )

        event_source = self._master_data_catalog_source(paths["event_quests"]["chapters"])
        event_chapters = live_master_tables.get("m_event_quest_chapter") or self._load_mom_banner_rows(
            paths["event_quests"]["chapters"]
        )
        event_chapter_rows = {
            to_int(row.get("EventQuestChapterId")): row
            for row in self._load_mom_banner_rows(event_source)
            if to_int(row.get("EventQuestChapterId")) > 0
        }
        root = self.mom_banner_path.parent
        daily_target_chapters = {
            to_int(row.get("EventQuestChapterId"))
            for row in self._load_mom_banner_rows(root / "EntityMEventQuestDailyGroupTargetChapterTable.json")
            if to_int(row.get("EventQuestChapterId")) > 0
        }
        tower_chapters = {
            to_int(row.get("EventQuestChapterId"))
            for row in self._load_mom_banner_rows(root / "EntityMEventQuestTowerAssetTable.json")
            if to_int(row.get("EventQuestChapterId")) > 0
        }
        labyrinth_chapters = {
            to_int(row.get("EventQuestChapterId"))
            for row in self._load_mom_banner_rows(root / "EntityMEventQuestLabyrinthSeasonTable.json")
            if to_int(row.get("EventQuestChapterId")) > 0
        }
        limit_content_chapters = {
            to_int(row.get("EventQuestChapterId"))
            for row in self._load_mom_banner_rows(root / "EntityMEventQuestChapterLimitContentRelationTable.json")
            if to_int(row.get("EventQuestChapterId")) > 0
        }
        limit_content_map = {
            to_int(row.get("EventQuestChapterId")): to_int(row.get("EventQuestLimitContentId"))
            for row in self._load_mom_banner_rows(root / "EntityMEventQuestChapterLimitContentRelationTable.json")
            if to_int(row.get("EventQuestChapterId")) > 0 and to_int(row.get("EventQuestLimitContentId")) > 0
        }
        limit_content_defs = {
            to_int(row.get("EventQuestLimitContentId")): row
            for row in self._load_mom_banner_rows(root / "EntityMEventQuestLimitContentTable.json")
            if to_int(row.get("EventQuestLimitContentId")) > 0
        }
        library_grouped_chapters = {
            to_int(row.get("EventQuestChapterId"))
            for row in self._load_mom_banner_rows(root / "EntityMLibraryEventQuestStoryGroupingTable.json")
            if to_int(row.get("EventQuestChapterId")) > 0
        }
        chapter_character_map = {
            to_int(row.get("EventQuestChapterId")): to_int(row.get("CharacterId"))
            for row in self._load_mom_banner_rows(root / "EntityMEventQuestChapterCharacterTable.json")
            if to_int(row.get("EventQuestChapterId")) > 0 and to_int(row.get("CharacterId")) > 0
        }

        library_rows = self._load_mom_banner_rows(root / "EntityMLibraryEventQuestStoryGroupingTable.json")
        chapters_by_library_group: dict[int, list[int]] = {}
        for row in library_rows:
            group_id = to_int(row.get("LibraryStoryGroupingId"))
            chapter_id = to_int(row.get("EventQuestChapterId"))
            if group_id <= 0 or chapter_id <= 0:
                continue
            chapters_by_library_group.setdefault(group_id, []).append(chapter_id)
        library_titles: dict[int, str] = {}
        for chapter_ids in chapters_by_library_group.values():
            titled_labels = [
                stringify((self.event_quest_defs.get(str(chapter_id)) or {}).get("name")).strip()
                for chapter_id in chapter_ids
                if not self._is_unknown_event_label(stringify((self.event_quest_defs.get(str(chapter_id)) or {}).get("name")).strip())
            ]
            titled_labels = list(dict.fromkeys(label for label in titled_labels if label))
            if len(titled_labels) != 1:
                continue
            for chapter_id in chapter_ids:
                library_titles[chapter_id] = titled_labels[0]

        def unique_shared_titles(field_name: str) -> dict[int, str]:
            grouped: dict[int, list[int]] = {}
            for key, extracted in self.event_quest_defs.items():
                chapter_id = to_int(key)
                chapter_row = event_chapter_rows.get(chapter_id, {})
                field_value = to_int(chapter_row.get(field_name))
                if chapter_id <= 0 or field_value <= 0:
                    continue
                grouped.setdefault(field_value, []).append(chapter_id)

            result: dict[int, str] = {}
            for chapter_ids in grouped.values():
                titled_labels = [
                    stringify((self.event_quest_defs.get(str(chapter_id)) or {}).get("name")).strip()
                    for chapter_id in chapter_ids
                    if not self._is_unknown_event_label(stringify((self.event_quest_defs.get(str(chapter_id)) or {}).get("name")).strip())
                ]
                titled_labels = list(dict.fromkeys(label for label in titled_labels if label))
                if len(titled_labels) != 1:
                    continue
                for chapter_id in chapter_ids:
                    result[chapter_id] = titled_labels[0]
            return result

        daily_target_rows = self._load_mom_banner_rows(root / "EntityMEventQuestDailyGroupTargetChapterTable.json")
        daily_target_map: dict[int, list[dict[str, Any]]] = {}
        for row in daily_target_rows:
            chapter_id = to_int(row.get("EventQuestChapterId"))
            if chapter_id > 0:
                daily_target_map.setdefault(chapter_id, []).append(row)

        webview_title_texts = {
            to_int(row.get("WebviewMissionTitleTextId")): stringify(row.get("Text")).strip()
            for row in self._load_mom_banner_rows(root / "EntityMWebviewMissionTitleTextTable.json")
            if to_int(row.get("LanguageType")) == 2 and to_int(row.get("WebviewMissionTitleTextId")) > 0
        }
        webview_titles_by_window: dict[tuple[int, int], str] = {}
        webview_titles_by_start: dict[int, set[str]] = {}
        anecdote_webview_schedule: list[tuple[int, str]] = []
        webview_mission_catalog_source = self._master_data_catalog_source(paths["event_quests"]["webview_missions"])
        for row in self._load_mom_banner_rows(webview_mission_catalog_source):
            title_id = to_int(row.get("TitleTextId"))
            start = to_int(row.get("StartDatetime"))
            end = to_int(row.get("EndDatetime"))
            title = stringify(webview_title_texts.get(title_id)).strip()
            if title and start > 0 and end > 0:
                webview_titles_by_window[(start, end)] = title
                webview_titles_by_start.setdefault(start, set()).add(title)
                if title.startswith("Anecdote:"):
                    anecdote_webview_schedule.append((start, title))
        current_webview_titles = {
            stringify(webview_title_texts.get(to_int(row.get("TitleTextId")))).strip()
            for row in self._load_mom_banner_rows(paths["event_quests"]["webview_missions"])
            if stringify(webview_title_texts.get(to_int(row.get("TitleTextId")))).strip()
        }
        live_webview_titles = {
            stringify(webview_title_texts.get(to_int(row.get("TitleTextId")))).strip()
            for row in live_master_tables.get("m_webview_mission", [])
            if stringify(webview_title_texts.get(to_int(row.get("TitleTextId")))).strip()
        }
        if live_webview_titles:
            current_webview_titles = live_webview_titles
        webview_titles_by_chapter: dict[int, str] = {}
        webview_start_titles_by_chapter: dict[int, str] = {}
        for key, extracted in self.event_quest_defs.items():
            chapter_id = to_int(key)
            start = to_int(extracted.get("StartDatetime"))
            end = to_int(extracted.get("EndDatetime"))
            if chapter_id > 0 and start > 0 and end > 0 and (start, end) in webview_titles_by_window:
                webview_titles_by_chapter[chapter_id] = webview_titles_by_window[(start, end)]
            if chapter_id > 0 and start > 0:
                start_titles = sorted(webview_titles_by_start.get(start, set()))
                if len(start_titles) == 1:
                    webview_start_titles_by_chapter[chapter_id] = start_titles[0]

        exact_anecdote_titles = {
            title
            for chapter_id, title in webview_titles_by_chapter.items()
            if 220001 <= chapter_id <= 220007 and title.startswith("Anecdote:")
        }
        exact_anecdote_titles.update(
            title
            for chapter_id, title in webview_start_titles_by_chapter.items()
            if 220001 <= chapter_id <= 220007 and title.startswith("Anecdote:")
        )
        remaining_anecdote_titles = [
            title
            for _, title in sorted(anecdote_webview_schedule, key=lambda item: (item[0], item[1]))
            if title != "Anecdote: Doll" and title not in exact_anecdote_titles
        ]
        remaining_anecdote_titles = list(dict.fromkeys(remaining_anecdote_titles))
        fallback_anecdote_chapters = [
            chapter_id
            for chapter_id in sorted(
                to_int(key) for key in self.event_quest_defs.keys() if 220001 <= to_int(key) <= 220007
            )
            if chapter_id not in webview_titles_by_chapter and chapter_id not in webview_start_titles_by_chapter
        ]
        anecdote_title_fallbacks = {
            chapter_id: title
            for chapter_id, title in zip(fallback_anecdote_chapters, remaining_anecdote_titles)
        }

        shared_banner_titles = unique_shared_titles("BannerAssetId")
        shared_display_titles = unique_shared_titles("EventQuestDisplayItemGroupId")
        event_active_ids = {
            to_int(row.get("EventQuestChapterId"))
            for row in event_chapters
            if to_int(row.get("EventQuestChapterId")) > 0
        }
        event_records: list[dict[str, Any]] = []
        for key, extracted in self.event_quest_defs.items():
            chapter_id = to_int(key)
            chapter_row = event_chapter_rows.get(chapter_id, {})
            event_type = to_int(chapter_row.get("EventQuestType"))
            base_category = self._event_category_label(event_type)
            sequence_group_id = to_int(extracted.get("EventQuestSequenceGroupId"))
            difficulty_count = len(extracted.get("difficulties", [])) if isinstance(extracted.get("difficulties"), list) else 0
            tags: list[str] = []
            if event_type > 0:
                tags.append(f"type {event_type}")
            if chapter_id in daily_target_chapters:
                tags.append("daily group")
            if chapter_id in tower_chapters:
                tags.append("tower")
            if chapter_id in labyrinth_chapters:
                tags.append("labyrinth")
            if chapter_id in limit_content_chapters:
                tags.append("limit content")
            if chapter_id in library_grouped_chapters:
                tags.append("story library")
            for target_row in daily_target_map.get(chapter_id, []):
                target_id = to_int(target_row.get("EventQuestDailyGroupTargetChapterId"))
                sort_order = to_int(target_row.get("SortOrder"))
                if target_id > 0:
                    tags.append(f"daily target {target_id}")
                if sort_order > 0:
                    tags.append(f"pair slot {sort_order}")
            detail_parts = [
                detail_from_record(extracted),
                base_category,
                f"group {sequence_group_id}" if sequence_group_id else "",
                f"{difficulty_count} difficulties" if difficulty_count else "",
                ", ".join(tags) if tags else "",
            ]
            record = self._quest_selector_record(
                extracted,
                active=chapter_id in event_active_ids,
                detail_parts=detail_parts,
                row_count=1,
                file_group="event_quests",
            )
            inferred = self._infer_event_quest_identity(
                chapter_id,
                extracted,
                chapter_row,
                library_titles=library_titles,
                banner_titles=shared_banner_titles,
                display_titles=shared_display_titles,
                chapter_character_map=chapter_character_map,
                limit_content_map=limit_content_map,
                limit_content_defs=limit_content_defs,
                webview_titles=webview_titles_by_chapter,
                webview_start_titles=webview_start_titles_by_chapter,
                anecdote_title_fallbacks=anecdote_title_fallbacks,
                daily_target_rows=daily_target_map.get(chapter_id, []),
            )
            if inferred:
                record["label"] = stringify(inferred.get("label")).strip() or record["label"]
                record["detail"] = join_detail(record.get("detail", ""), stringify(inferred.get("detail")).strip())
                record["nameFound"] = True
                record["labelSource"] = stringify(inferred.get("source")).strip() or "inferred"
                record["familyLabel"] = stringify(inferred.get("family")).strip()
                record["characterLabel"] = stringify(inferred.get("character")).strip()
                inferred_tags = [stringify(value).strip() for value in inferred.get("tags", []) if stringify(value).strip()]
                tags.extend(tag for tag in inferred_tags if tag not in tags)
            else:
                record["labelSource"] = "text" if record.get("nameFound") else "fallback"
            category = self._event_category_override(base_category, record.get("familyLabel", ""), tags)
            record.update(
                {
                    "category": category,
                    "eventType": event_type,
                    "tags": tags,
                    "isSelectable": True,
                }
            )
            event_records.append(record)

        present_anecdote_ids = {
            to_int(record.get("id"))
            for record in event_records
            if stringify(record.get("category")).strip() == "Anecdotes"
        }
        if 220008 not in present_anecdote_ids:
            event_records.append(
                {
                    "id": 220008,
                    "label": "Anecdote: Doll",
                    "detail": join_detail(
                        "panel mission and stained glass entry",
                        "inferred from WebviewMissionId 8 and StainedGlassId 220008",
                        "no direct writable EventQuestChapter row found",
                    ),
                    "group": "",
                    "isActive": "Anecdote: Doll" in current_webview_titles,
                    "rowCount": 1,
                    "nameFound": True,
                    "labelSource": "panel-mission",
                    "familyLabel": "Anecdote: Doll",
                    "characterLabel": "",
                    "category": "Anecdotes",
                    "eventType": 3,
                    "tags": ["family Anecdote: Doll", "anecdote-linked", "panel mission", "stained glass 220008"],
                    "isSelectable": True,
                }
            )

        extra_source = self._master_data_catalog_source(paths["extra_quests"]["groups"])
        extra_map_path = paths["extra_quests"]["chapter_map"]
        extra_groups = self._load_mom_banner_rows(paths["extra_quests"]["groups"])
        extra_map_rows = self._load_mom_banner_rows(extra_map_path)
        extra_active_ids = {
            to_int(row.get("ExtraQuestId"))
            for row in extra_groups
            if to_int(row.get("ExtraQuestId")) > 0
        }
        extra_known_rows: dict[int, dict[str, Any]] = {}
        for row in self._load_mom_banner_rows(extra_source):
            extra_id = to_int(row.get("ExtraQuestId"))
            if extra_id > 0 and extra_id not in extra_known_rows:
                extra_known_rows[extra_id] = row

        extra_chapter_ids: dict[int, set[int]] = {}
        extra_indexes: dict[int, set[int]] = {}
        for row in extra_map_rows:
            extra_id = to_int(row.get("ExtraQuestId"))
            if extra_id <= 0:
                continue
            chapter_id = to_int(row.get("MainQuestChapterId"))
            if chapter_id > 0:
                extra_chapter_ids.setdefault(extra_id, set()).add(chapter_id)
            index_value = to_int(row.get("ExtraQuestIndex"))
            if index_value > 0:
                extra_indexes.setdefault(extra_id, set()).add(index_value)

        extra_catalog_ids = {
            *extra_known_rows.keys(),
            *extra_active_ids,
            *extra_chapter_ids.keys(),
            *(to_int(key) for key in self.extra_quest_defs.keys()),
        }
        extra_records: list[dict[str, Any]] = []
        for extra_id in sorted(value for value in extra_catalog_ids if value > 0):
            extracted = self.extra_quest_defs.get(str(extra_id), {})
            known_row = extra_known_rows.get(extra_id, {})
            quest_id = to_int(extracted.get("QuestId")) or to_int(known_row.get("QuestId"))
            quest_record = self.quest_defs.get(str(quest_id), {}) if quest_id > 0 else {}
            chapter_ids = sorted(extra_chapter_ids.get(extra_id, set()))
            chapter_names = list(
                dict.fromkeys(
                    stringify((self.main_quest_defs.get(str(chapter_id)) or {}).get("name")).strip()
                    for chapter_id in chapter_ids
                    if stringify((self.main_quest_defs.get(str(chapter_id)) or {}).get("name")).strip()
                )
            )
            detail_bits = [detail_from_record(extracted)]
            if quest_id > 0:
                detail_bits.append(f"quest {quest_id}")
            if stringify(quest_record.get("name")).strip():
                detail_bits.append(f"plays {stringify(quest_record.get('name')).strip()}")
            if chapter_names:
                detail_bits.append(
                    "unlocks from " + ", ".join(chapter_names[:3]) + (" +" if len(chapter_names) > 3 else "")
                )
            elif chapter_ids:
                detail_bits.append(
                    "unlocks from chapter " + ", ".join(str(chapter_id) for chapter_id in chapter_ids[:4])
                )
            index_values = sorted(extra_indexes.get(extra_id, set()))
            if index_values:
                detail_bits.append(
                    "slot " + ", ".join(str(index_value) for index_value in index_values[:3])
                    + (" +" if len(index_values) > 3 else "")
                )
            if extra_id not in extra_known_rows:
                detail_bits.append("catalog only: no writable ExtraQuestGroup row found")

            label = stringify(extracted.get("name")).strip()
            if not label:
                if stringify(quest_record.get("name")).strip():
                    label = stringify(quest_record.get("name")).strip()
                elif chapter_names:
                    label = f"{chapter_names[0]} Extra"
                else:
                    label = f"Extra Quest {extra_id}"

            group_label = lookup_group_for_record("main_quests.json", self.main_quest_defs.get(str(chapter_ids[0]), {})) if chapter_ids else ""
            detail_parts = [
                *detail_bits,
            ]
            extra_records.append(
                {
                    "id": extra_id,
                    "label": label,
                    "detail": join_detail(*detail_parts),
                    "group": group_label,
                    "isActive": extra_id in extra_active_ids,
                    "rowCount": 1,
                    "nameFound": bool(extracted.get("name_found") or quest_record.get("name_found")),
                    "isSelectable": extra_id in extra_known_rows,
                    "selectionReason": ""
                    if extra_id in extra_known_rows
                    else "This extra quest is discoverable from chapter mapping data, but its full ExtraQuestGroup row is missing from the writable source file.",
                }
            )

        side_source = self._master_data_catalog_source(paths["side_story_quests"]["quests"])
        side_quests = live_master_tables.get("m_side_story_quest") or self._load_mom_banner_rows(
            paths["side_story_quests"]["quests"]
        )
        side_active_ids = {
            to_int(row.get("SideStoryQuestId"))
            for row in side_quests
            if to_int(row.get("SideStoryQuestId")) > 0
        }
        side_records: list[dict[str, Any]] = []
        for key, extracted in self.side_story_quest_defs.items():
            side_id = to_int(key)
            scene_count = len(extracted.get("scene_ids", [])) if isinstance(extracted.get("scene_ids"), list) else 0
            detail_parts = [
                detail_from_record(extracted),
                f"{scene_count} scenes" if scene_count else "",
            ]
            side_records.append(
                self._quest_selector_record(
                    extracted,
                    active=side_id in side_active_ids,
                    detail_parts=detail_parts,
                    row_count=scene_count or 1,
                    file_group="side_story_quests",
                )
            )

        for records in (event_records, extra_records, side_records):
            records.sort(key=lambda record: (group_sort_key(record.get("group", "")), record["label"].lower(), record["id"]))

        event_category_counts: dict[str, int] = {}
        for record in event_records:
            category = stringify(record.get("category")).strip() or "Special / Misc"
            event_category_counts[category] = event_category_counts.get(category, 0) + 1
        event_active_record_ids = sorted(
            {
                to_int(record.get("id"))
                for record in event_records
                if to_int(record.get("id")) > 0 and bool(record.get("isActive")) and bool(record.get("isSelectable", True))
            }
        )
        category_order = [
            "Story Records",
            "Variations",
            "Daily / Routine",
            "Guerrilla",
            "Tower",
            "Fate Board / Labyrinth",
            "Limit Content",
            "Dark Lair",
            "Chambers of Dusk",
            "Character Events",
            "Daily Challenge",
            "Anecdotes",
            "Special / Misc",
        ]
        category_rank = {label: index for index, label in enumerate(category_order)}
        event_categories = [
            {"id": category.lower().replace(" / ", "-").replace(" ", "-"), "label": category, "count": count}
            for category, count in sorted(
                event_category_counts.items(),
                key=lambda item: (category_rank.get(item[0], len(category_order)), item[0].lower()),
            )
        ]

        return {
            "enabled": True,
            "groups": {
                "event_quests": {
                    "label": "Event Quests",
                    "records": event_records,
                    "activeIds": event_active_record_ids,
                    "sourcePath": self._display_path(event_source),
                    "currentPath": self._display_path(paths["event_quests"]["chapters"]),
                    "categories": event_categories,
                },
                "extra_quests": {
                    "label": "Extra Quests",
                    "records": extra_records,
                    "activeIds": sorted(extra_active_ids),
                    "sourcePath": self._display_path(extra_map_path if extra_map_rows else extra_source),
                    "currentPath": self._display_path(paths["extra_quests"]["groups"]),
                    "writableSourcePath": self._display_path(extra_source),
                },
                "side_story_quests": {
                    "label": "Side Story Quests",
                    "records": side_records,
                    "activeIds": sorted(side_active_ids),
                    "sourcePath": self._display_path(side_source),
                    "currentPath": self._display_path(paths["side_story_quests"]["quests"]),
                },
            },
        }

    def preset_catalog(self) -> dict[str, Any]:
        banner_payload = self.gacha_banner_catalog()
        event_payload = self.event_selector_catalog()
        banner_records = banner_payload.get("usableRecords", [])
        event_records = event_payload.get("groups", {}).get("event_quests", {}).get("records", [])
        event_record_ids = {to_int(record.get("id")) for record in event_records}

        resolved_presets: list[dict[str, Any]] = []
        for preset in self.presets:
            preset_id = stringify(preset.get("id")).strip()
            if not preset_id:
                continue
            banner_matchers = [
                stringify(value).strip().lower()
                for value in preset.get("bannerMatchers", [])
                if stringify(value).strip()
            ]
            explicit_banner_ids = {
                to_int(value) for value in preset.get("bannerIds", []) if to_int(value) > 0
            }
            resolved_banner_ids: set[int] = set()
            matched_banner_labels: list[str] = []
            for record in banner_records:
                record_id = to_int(record.get("id"))
                label = stringify(record.get("label")).strip()
                haystack = " ".join(
                    [
                        label.lower(),
                        stringify(record.get("detail")).lower(),
                        stringify(record.get("assetName")).lower(),
                        stringify(record.get("group")).lower(),
                    ]
                )
                matched = record_id in explicit_banner_ids or any(matcher in haystack for matcher in banner_matchers)
                if not matched:
                    continue
                for banner_id in record.get("momBannerIds", []):
                    if to_int(banner_id) > 0:
                        resolved_banner_ids.add(to_int(banner_id))
                if label:
                    matched_banner_labels.append(label)

            explicit_event_ids = {
                to_int(value) for value in preset.get("eventQuestIds", []) if to_int(value) > 0
            }
            resolved_event_ids = sorted(event_id for event_id in explicit_event_ids if event_id in event_record_ids)
            missing_event_ids = sorted(explicit_event_ids - set(resolved_event_ids))

            resolved_presets.append(
                {
                    "id": preset_id,
                    "label": stringify(preset.get("label")).strip() or preset_id,
                    "description": stringify(preset.get("description")).strip(),
                    "bannerIds": sorted(resolved_banner_ids),
                    "eventQuestIds": resolved_event_ids,
                    "missingEventQuestIds": missing_event_ids,
                    "bannerCount": len(resolved_banner_ids),
                    "eventCount": len(resolved_event_ids),
                    "bannerPreview": matched_banner_labels[:4],
                }
            )

        resolved_presets.sort(key=lambda preset: preset["label"].lower())
        return {
            "enabled": bool(resolved_presets),
            "sourcePath": self._display_path(PRESETS_PATH),
            "presets": resolved_presets,
            "notes": "Preset application updates banner and event chapter selections only. Side stories stay selected.",
        }

    def save_event_selector_group(self, group: str, active_ids: list[Any]) -> dict[str, Any]:
        selected_ids = {to_int(value) for value in active_ids if to_int(value) > 0}
        paths = self._event_selector_paths()

        if group == "event_quests":
            chapter_path = paths[group]["chapters"]
            groups_path = paths[group]["sequence_groups"]
            seq_path = paths[group]["sequences"]
            labyrinth_season_path = paths[group]["labyrinth_seasons"]
            webview_mission_path = paths[group]["webview_missions"]
            webview_panel_mission_path = paths[group]["webview_panel_missions"]
            chapter_source = self._master_data_catalog_source(chapter_path)
            groups_source = self._master_data_catalog_source(groups_path)
            seq_source = self._master_data_catalog_source(seq_path)
            labyrinth_season_source = self._master_data_catalog_source(labyrinth_season_path)
            webview_mission_source = self._master_data_catalog_source(webview_mission_path)
            webview_panel_mission_source = self._master_data_catalog_source(webview_panel_mission_path)
            chapter_rows = self._load_mom_banner_rows(chapter_source)
            group_rows = self._load_mom_banner_rows(groups_source)
            seq_rows = self._load_mom_banner_rows(seq_source)
            labyrinth_season_rows = self._load_mom_banner_rows(labyrinth_season_source)
            event_catalog = self.event_selector_catalog()
            event_records = event_catalog.get("groups", {}).get("event_quests", {}).get("records", [])
            records_by_id = {
                to_int(record.get("id")): record
                for record in event_records
                if to_int(record.get("id")) > 0 and bool(record.get("isSelectable", True))
            }
            known_ids = set(records_by_id.keys())
            unknown_ids = sorted(selected_ids - known_ids)
            if unknown_ids:
                raise ValueError(f"unknown event quest ids: {', '.join(str(value) for value in unknown_ids)}")
            now = datetime.now(timezone.utc)
            save_start = int(now.timestamp() * 1000)
            save_end = int((now + timedelta(days=30)).timestamp() * 1000)
            selected_chapters = [
                {
                    **row,
                    "StartDatetime": save_start,
                    "EndDatetime": save_end,
                }
                for row in chapter_rows
                if to_int(row.get("EventQuestChapterId")) in selected_ids
            ]
            selected_group_ids = {to_int(row.get("EventQuestSequenceGroupId")) for row in selected_chapters}
            selected_groups = [row for row in group_rows if to_int(row.get("EventQuestSequenceGroupId")) in selected_group_ids]
            selected_sequence_ids = {to_int(row.get("EventQuestSequenceId")) for row in selected_groups}
            selected_sequences = [row for row in seq_rows if to_int(row.get("EventQuestSequenceId")) in selected_sequence_ids]
            selected_labyrinth_chapter_ids = {
                to_int(row.get("EventQuestChapterId"))
                for row in selected_chapters
                if to_int(row.get("EventQuestType")) == 12
            }
            latest_labyrinth_season_rows: dict[int, dict[str, Any]] = {}
            for row in labyrinth_season_rows:
                chapter_id = to_int(row.get("EventQuestChapterId"))
                if chapter_id not in selected_labyrinth_chapter_ids:
                    continue
                season_number = to_int(row.get("SeasonNumber"))
                current = latest_labyrinth_season_rows.get(chapter_id)
                if current is None or season_number >= to_int(current.get("SeasonNumber")):
                    latest_labyrinth_season_rows[chapter_id] = row
            selected_labyrinth_seasons = [
                {
                    **row,
                    "StartDatetime": save_start,
                    "EndDatetime": save_end,
                }
                for row in latest_labyrinth_season_rows.values()
            ]
            # Server-side JSON uses a perpetual window so the Go server always
            # sees an active season regardless of when it was started relative
            # to when Mama's Toolbox ran.
            server_labyrinth_seasons = [
                {
                    **row,
                    "StartDatetime": 0,
                    "EndDatetime": 253402300799000,
                }
                for row in latest_labyrinth_season_rows.values()
            ]
            webview_title_texts = {
                to_int(row.get("WebviewMissionTitleTextId")): stringify(row.get("Text")).strip()
                for row in self._load_mom_banner_rows(self.mom_banner_path.parent / "EntityMWebviewMissionTitleTextTable.json")
                if to_int(row.get("LanguageType")) == 2 and to_int(row.get("WebviewMissionTitleTextId")) > 0
            }
            selected_anecdote_titles = {
                stringify(records_by_id[event_id].get("label")).strip()
                for event_id in selected_ids
                if stringify(records_by_id.get(event_id, {}).get("label")).strip().startswith("Anecdote:")
            }
            webview_mission_rows = self._load_mom_banner_rows(webview_mission_source)
            webview_panel_mission_rows = self._load_mom_banner_rows(webview_panel_mission_source)
            selected_webview_mission_ids: set[int] = set()
            filtered_webview_missions: list[dict[str, Any]] = []
            for row in webview_mission_rows:
                title = stringify(webview_title_texts.get(to_int(row.get("TitleTextId")))).strip()
                if title.startswith("Anecdote:"):
                    if title not in selected_anecdote_titles:
                        continue
                    selected_webview_mission_ids.add(to_int(row.get("WebviewMissionId")))
                    filtered_webview_missions.append(
                        {
                            **row,
                            "StartDatetime": save_start,
                            "EndDatetime": save_end,
                        }
                    )
                    continue
                filtered_webview_missions.append(row)
            filtered_webview_panel_missions = [
                {
                    **row,
                    "StartDatetime": save_start,
                    "EndDatetime": save_end,
                }
                for row in webview_panel_mission_rows
                if to_int(row.get("WebviewPanelMissionId")) in selected_webview_mission_ids
            ]
            for path, source in (
                (chapter_path, chapter_source),
                (groups_path, groups_source),
                (seq_path, seq_source),
                (labyrinth_season_path, labyrinth_season_source),
                (webview_mission_path, webview_mission_source),
                (webview_panel_mission_path, webview_panel_mission_source),
            ):
                self._ensure_master_data_backup(path, source)
            chapter_path.write_text(json.dumps(selected_chapters, indent=2) + "\n", encoding="utf-8")
            groups_path.write_text(json.dumps(selected_groups, indent=2) + "\n", encoding="utf-8")
            seq_path.write_text(json.dumps(selected_sequences, indent=2) + "\n", encoding="utf-8")
            labyrinth_season_path.write_text(json.dumps(server_labyrinth_seasons, indent=2) + "\n", encoding="utf-8")
            webview_mission_path.write_text(json.dumps(filtered_webview_missions, indent=2) + "\n", encoding="utf-8")
            webview_panel_mission_path.write_text(json.dumps(filtered_webview_panel_missions, indent=2) + "\n", encoding="utf-8")
            self._patch_master_memory_tables(
                {
                    "m_event_quest_chapter": selected_chapters,
                    "m_event_quest_sequence_group": selected_groups,
                    "m_event_quest_sequence": selected_sequences,
                    "m_event_quest_labyrinth_season": selected_labyrinth_seasons,
                    "m_webview_mission": filtered_webview_missions,
                    "m_webview_panel_mission": filtered_webview_panel_missions,
                }
            )
            return self.event_selector_catalog()

        if group == "extra_quests":
            groups_path = paths[group]["groups"]
            source = self._master_data_catalog_source(groups_path)
            rows = self._load_mom_banner_rows(source)
            known_ids = {to_int(row.get("ExtraQuestId")) for row in rows}
            unknown_ids = sorted(selected_ids - known_ids)
            if unknown_ids:
                raise ValueError(f"unknown extra quest ids: {', '.join(str(value) for value in unknown_ids)}")
            selected_rows = [row for row in rows if to_int(row.get("ExtraQuestId")) in selected_ids]
            self._ensure_master_data_backup(groups_path, source)
            groups_path.write_text(json.dumps(selected_rows, indent=2) + "\n", encoding="utf-8")
            return self.event_selector_catalog()

        if group == "side_story_quests":
            quest_path = paths[group]["quests"]
            limit_path = paths[group]["limits"]
            scene_path = paths[group]["scenes"]
            quest_source = self._master_data_catalog_source(quest_path)
            limit_source = self._master_data_catalog_source(limit_path)
            scene_source = self._master_data_catalog_source(scene_path)
            quest_rows = self._load_mom_banner_rows(quest_source)
            limit_rows = self._load_mom_banner_rows(limit_source)
            scene_rows = self._load_mom_banner_rows(scene_source)
            known_ids = {to_int(row.get("SideStoryQuestId")) for row in quest_rows}
            unknown_ids = sorted(selected_ids - known_ids)
            if unknown_ids:
                raise ValueError(f"unknown side story ids: {', '.join(str(value) for value in unknown_ids)}")
            selected_quests = [row for row in quest_rows if to_int(row.get("SideStoryQuestId")) in selected_ids]
            target_ids = {to_int(row.get("TargetId")) for row in selected_quests}
            selected_limits = [row for row in limit_rows if to_int(row.get("SideStoryQuestLimitContentId")) in target_ids]
            selected_scenes = [row for row in scene_rows if to_int(row.get("SideStoryQuestId")) in selected_ids]
            for path, source in ((quest_path, quest_source), (limit_path, limit_source), (scene_path, scene_source)):
                self._ensure_master_data_backup(path, source)
            quest_path.write_text(json.dumps(selected_quests, indent=2) + "\n", encoding="utf-8")
            limit_path.write_text(json.dumps(selected_limits, indent=2) + "\n", encoding="utf-8")
            scene_path.write_text(json.dumps(selected_scenes, indent=2) + "\n", encoding="utf-8")
            self._patch_master_memory_tables(
                {
                    "m_side_story_quest": selected_quests,
                    "m_side_story_quest_limit_content": selected_limits,
                    "m_side_story_quest_scene": selected_scenes,
                }
            )
            return self.event_selector_catalog()

        raise ValueError(f"unknown event selector group: {group}")

    def _build_gacha_banner_record(self, row: dict[str, Any], active_ids: set[int]) -> dict[str, Any]:
        banner_id = to_int(row.get("MomBannerId"))
        extracted = self.gacha_banners.get(str(banner_id), {})
        asset_name = stringify(row.get("BannerAssetName")).strip()
        inferred = self._infer_gacha_banner_identity(row)
        extracted_label = stringify(extracted.get("name")).strip() if extracted.get("name_found") else ""
        label = (
            extracted_label
            or stringify(inferred.get("label")).strip()
            or f"Gacha Banner {banner_id}"
        )
        group = lookup_group_for_record("gacha_banners.json", extracted or row)
        detail = detail_from_record(extracted or row)
        inferred_detail = stringify(inferred.get("detail")).strip()
        if inferred_detail:
            detail = join_detail(detail, inferred_detail)
        if not detail:
            fallback_parts = []
            if asset_name:
                fallback_parts.append(asset_name)
            destination_id = to_int(row.get("DestinationDomainId"))
            if destination_id:
                fallback_parts.append(f"gacha {destination_id}")
            start = format_unix_millis(row.get("StartDatetime"))
            end = format_unix_millis(row.get("EndDatetime"))
            if start or end:
                fallback_parts.append(f"{start or 'unknown'} to {end or 'unknown'}")
            detail = join_detail(*fallback_parts)
        return {
            "id": banner_id,
            "label": label,
            "detail": detail,
            "group": group,
            "assetName": asset_name,
            "destinationDomainId": to_int(row.get("DestinationDomainId")),
            "startDatetime": to_int(row.get("StartDatetime")),
            "endDatetime": to_int(row.get("EndDatetime")),
            "startDate": format_unix_millis(row.get("StartDatetime")),
            "endDate": format_unix_millis(row.get("EndDatetime")),
            "isActive": banner_id in active_ids,
            "nameFound": bool(extracted.get("name_found") or inferred.get("label")),
            "matchedTextKey": stringify(extracted.get("matched_text_key")).strip(),
            "labelSource": "text" if extracted.get("name_found") else stringify(inferred.get("source")).strip() or "fallback",
        }

    def _build_effective_gacha_banner_catalog(
        self,
        source_rows: list[dict[str, Any]],
        active_ids: set[int],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        effective_records: list[dict[str, Any]] = []
        unusable_records: list[dict[str, Any]] = []
        stepup_groups: dict[int, list[dict[str, Any]]] = {}

        for row in source_rows:
            if to_int(row.get("DestinationDomainType")) != 1:
                continue

            banner_id = to_int(row.get("MomBannerId"))
            destination_id = to_int(row.get("DestinationDomainId"))
            asset_name = stringify(row.get("BannerAssetName")).strip()
            is_stepup = asset_name.startswith("step_up_")
            is_chapter = asset_name.startswith("common_")
            has_medal = destination_id in self.gacha_medal_ids

            if is_stepup:
                if not has_medal:
                    record = self._build_gacha_banner_record(row, active_ids)
                    record.update(
                        {
                            "entryKey": f"raw:{banner_id}",
                            "gameGachaId": destination_id,
                            "momBannerIds": [banner_id],
                            "momBannerCount": 1,
                            "mode": "step-up",
                            "isUsable": False,
                            "usabilityReason": "Ignored by lunar-tear: step-up banner has no matching EntityMGachaMedalTable row.",
                        }
                    )
                    unusable_records.append(record)
                    continue
                stepup_groups.setdefault(destination_id // 1000, []).append(row)
                continue

            if not is_chapter and not has_medal:
                record = self._build_gacha_banner_record(row, active_ids)
                record.update(
                    {
                        "entryKey": f"raw:{banner_id}",
                        "gameGachaId": destination_id,
                        "momBannerIds": [banner_id],
                        "momBannerCount": 1,
                        "mode": "basic",
                        "isUsable": False,
                        "usabilityReason": "Ignored by lunar-tear: premium banner has no matching EntityMGachaMedalTable row.",
                    }
                )
                unusable_records.append(record)
                continue

            record = self._build_gacha_banner_record(row, active_ids)
            record.update(
                {
                    "entryKey": f"direct:{banner_id}",
                    "gameGachaId": destination_id,
                    "momBannerIds": [banner_id],
                    "momBannerCount": 1,
                    "mode": "chapter" if is_chapter else "basic",
                    "isPartiallyActive": False,
                    "selectionState": "active" if record["isActive"] else "inactive",
                    "isUsable": True,
                    "usabilityReason": "Will be loaded by the current lunar-tear gacha catalog logic.",
                }
            )
            effective_records.append(record)

        for group_id, rows in stepup_groups.items():
            rows.sort(key=lambda row: to_int(row.get("MomBannerId")))
            first_row = rows[0]
            first_record = self._build_gacha_banner_record(first_row, active_ids)
            row_ids = [to_int(row.get("MomBannerId")) for row in rows]
            active_count = sum(1 for row_id in row_ids if row_id in active_ids)
            is_active = active_count == len(row_ids)
            is_partial = 0 < active_count < len(row_ids)
            first_record.update(
                {
                    "id": group_id,
                    "entryKey": f"step:{group_id}",
                    "gameGachaId": group_id,
                    "momBannerIds": row_ids,
                    "momBannerCount": len(row_ids),
                    "mode": "step-up",
                    "isActive": is_active,
                    "isPartiallyActive": is_partial,
                    "selectionState": "partial" if is_partial else ("active" if is_active else "inactive"),
                    "isUsable": True,
                    "usabilityReason": "Will be loaded by the current lunar-tear gacha catalog logic as one grouped step-up entry.",
                    "detail": join_detail(
                        first_record["detail"],
                        f"{len(row_ids)} step rows",
                        f"{active_count}/{len(row_ids)} steps selected" if is_partial else "",
                    ),
                }
            )
            effective_records.append(first_record)

        return effective_records, unusable_records

    def gacha_banner_editor_summary(self) -> dict[str, Any]:
        current_exists = self.mom_banner_path.is_file()
        source_path = self._mom_banner_catalog_source()
        source_rows = self._load_mom_banner_rows(source_path)
        current_rows = self._load_mom_banner_rows(self.mom_banner_path)
        source_gacha = [row for row in source_rows if to_int(row.get("DestinationDomainType")) == 1]
        current_active_count = sum(1 for row in current_rows if to_int(row.get("DestinationDomainType")) == 1)
        effective_records, unusable_records = self._build_effective_gacha_banner_catalog(
            source_rows,
            {
                to_int(row.get("MomBannerId"))
                for row in current_rows
                if to_int(row.get("DestinationDomainType")) == 1
            },
        )
        return {
            "enabled": current_exists or source_path.is_file(),
            "currentPath": self._display_path(self.mom_banner_path),
            "sourcePath": self._display_path(source_path),
            "backupPaths": [self._display_path(path) for path in self._mom_banner_backup_candidates()],
            "catalogCount": len(source_gacha),
            "activeCount": current_active_count,
            "effectiveCount": len(effective_records),
            "unsupportedCount": len(unusable_records),
        }

    def gacha_banner_catalog(self) -> dict[str, Any]:
        source_path = self._mom_banner_catalog_source()
        source_rows = self._load_mom_banner_rows(source_path)
        current_rows = self._load_mom_banner_rows(self.mom_banner_path)
        source_gacha_rows = [row for row in source_rows if to_int(row.get("DestinationDomainType")) == 1]
        active_ids = {
            to_int(row.get("MomBannerId"))
            for row in current_rows
            if to_int(row.get("DestinationDomainType")) == 1
        }
        usable_records, unusable_records = self._build_effective_gacha_banner_catalog(source_rows, active_ids)
        usable_records.sort(
            key=lambda record: (
                record["group"].lower() if record["group"] else "zzzz",
                record["label"].lower(),
                record["id"],
            )
        )
        unusable_records.sort(
            key=lambda record: (
                record["group"].lower() if record["group"] else "zzzz",
                record["label"].lower(),
                record["id"],
            )
        )
        return {
            "enabled": bool(source_rows),
            "currentPath": self._display_path(self.mom_banner_path),
            "sourcePath": self._display_path(source_path),
            "backupPaths": [self._display_path(path) for path in self._mom_banner_backup_candidates()],
            "activeBannerIds": sorted(active_ids),
            "records": usable_records,
            "usableRecords": usable_records,
            "unusableRecords": unusable_records,
            "unsupportedCount": len(unusable_records),
            "rawCatalogCount": len(source_gacha_rows),
            "logicSourcePath": self._display_path(self._mom_banner_logic_source_path()),
        }

    def save_active_gacha_banners(self, active_banner_ids: list[Any]) -> dict[str, Any]:
        source_path = self._mom_banner_catalog_source()
        source_rows = self._load_mom_banner_rows(source_path)
        if not source_rows:
            raise ValueError(f"Mom banner catalog not found: {self.mom_banner_path}")

        stable_backup = self.mom_banner_path.with_name(f"{self.mom_banner_path.name}.full-backup")
        if source_path == self.mom_banner_path and self.mom_banner_path.is_file() and not stable_backup.exists():
            stable_backup.write_text(self.mom_banner_path.read_text(encoding="utf-8"), encoding="utf-8")

        selected_ids = {to_int(value) for value in active_banner_ids if to_int(value) > 0}
        known_ids = {
            to_int(row.get("MomBannerId"))
            for row in source_rows
            if to_int(row.get("DestinationDomainType")) == 1
        }
        unknown_ids = sorted(selected_ids - known_ids)
        if unknown_ids:
            raise ValueError(f"unknown banner ids: {', '.join(str(value) for value in unknown_ids)}")

        filtered_rows = [
            row
            for row in source_rows
            if to_int(row.get("DestinationDomainType")) != 1 or to_int(row.get("MomBannerId")) in selected_ids
        ]
        self.mom_banner_path.write_text(json.dumps(filtered_rows, indent=2) + "\n", encoding="utf-8")
        return self.gacha_banner_catalog()

    def connect(self) -> sqlite3.Connection:
        try:
            connection = sqlite3.connect(self.db_path)
        except sqlite3.OperationalError as exc:
            raise sqlite3.OperationalError(
                f"unable to open database file: {self.db_path}"
            ) from exc
        connection.row_factory = sqlite3.Row
        return connection

    def query_rows(self, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            cursor = connection.execute(query, params)
            return [row_to_dict(row) for row in cursor.fetchall()]

    def query_single_row(self, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
        rows = self.query_rows(query, params)
        return rows[0] if rows else {}

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        with closing(self.connect()) as connection:
            connection.execute(query, params)
            connection.commit()

    def find_image_url(self, category: str, asset_ids: list[str], variants: list[str]) -> str:
        category_index = self.image_index.get(category, {})
        for asset_id in asset_ids:
            asset_key = stringify(asset_id).strip()
            if not asset_key:
                continue
            file_names = category_index.get(asset_key, set())
            if not file_names:
                continue
            for variant in variants:
                expected = f"{asset_key}_{variant}.png"
                if expected in file_names:
                    return f"/images/{category}/{asset_key}/{expected}"
            for file_name in sorted(file_names):
                return f"/images/{category}/{asset_key}/{file_name}"
        return ""

    def weapon_image_url(self, weapon_id: str) -> str:
        weapon_record = self.weapons.get(stringify(weapon_id).strip(), {})
        if not weapon_record:
            return ""
        return self.find_image_url(
            "weapon",
            [
                stringify(weapon_record.get("weapon_actor_asset_id")).strip(),
                *[stringify(value).strip() for value in weapon_record.get("weapon_name_asset_ids", [])],
            ],
            ["full", "large", "standard", "gacha"],
        )

    def costume_image_url(self, costume_id: str) -> str:
        costume_record = self.costumes.get(stringify(costume_id).strip()) or self.playable_costumes.get(
            stringify(costume_id).strip(),
            {},
        )
        if not costume_record:
            return ""
        return self.find_image_url(
            "costume",
            [stringify(costume_record.get("costume_actor_asset_id")).strip()],
            ["gacha", "large", "full", "portrait"],
        )

    def character_image_url(self, character_id: str) -> str:
        character_record = self.playable_characters.get(stringify(character_id).strip(), {})
        if not character_record:
            return ""
        costume_id = self.default_costume_id_for_character(character_record)
        if not costume_id:
            return ""
        return self.costume_image_url(costume_id)

    def companion_image_url(self, companion_id: str) -> str:
        companion_record = self.companions.get(stringify(companion_id).strip(), {})
        if not companion_record:
            return ""
        return self.find_image_url(
            "companions",
            [stringify(companion_record.get("companion_actor_asset_id")).strip()],
            ["full", "large", "standard", "portrait"],
        )

    def consumable_image_url(self, consumable_item_id: str) -> str:
        item_id = stringify(consumable_item_id).strip()
        if not item_id:
            return ""
        candidates = [f"consumable{item_id}"]
        extracted_record = self.consumables.get(item_id, {})
        lookup_key = stringify(extracted_record.get("lookup_key")).strip()
        if lookup_key:
            candidates.insert(0, f"consumable{lookup_key}")
        for asset_key in candidates:
            root = IMAGES_DIR / "consumables" / "consumable_item" / asset_key
            if not root.is_dir():
                continue
            for file_name in (f"{asset_key}_standard.png", f"{asset_key}_icon.png"):
                if (root / file_name).is_file():
                    return f"/images/consumables/consumable_item/{asset_key}/{file_name}"
        return ""

    def material_image_url(self, material_id: str) -> str:
        item_id = stringify(material_id).strip()
        if not item_id:
            return ""
        asset_key = f"material{item_id}"
        root = IMAGES_DIR / "materials" / "material" / asset_key
        if not root.is_dir():
            return ""
        file_name = f"{asset_key}_standard.png"
        if (root / file_name).is_file():
            return f"/images/materials/material/{asset_key}/{file_name}"
        return ""

    def load_schema(self) -> dict[str, TableSchema]:
        with closing(self.connect()) as connection:
            tables = connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            ).fetchall()
            schema: dict[str, TableSchema] = {}
            for table_row in tables:
                name = table_row["name"]
                pragma_rows = connection.execute(f'PRAGMA table_info("{name}")').fetchall()
                columns: list[ColumnInfo] = []
                primary_pairs: list[tuple[int, str]] = []
                for pragma_row in pragma_rows:
                    column = ColumnInfo(
                        name=pragma_row["name"],
                        type=pragma_row["type"] or "TEXT",
                        not_null=bool(pragma_row["notnull"]),
                        default_sql=stringify(pragma_row["dflt_value"]),
                        is_primary=to_int(pragma_row["pk"]) > 0,
                    )
                    columns.append(column)
                    if column.is_primary:
                        primary_pairs.append((to_int(pragma_row["pk"]), column.name))
                primary_pairs.sort()
                schema[name] = TableSchema(
                    name=name,
                    columns=columns,
                    primary_key=[name for _, name in primary_pairs],
                )
            return schema

    def must_count(self, table: str) -> int:
        if table not in self.schema:
            return 0
        rows = self.query_rows(f'SELECT COUNT(*) AS count FROM "{table}"')
        return to_int(rows[0]["count"]) if rows else 0

    def list_users(self) -> list[dict[str, Any]]:
        rows = self.query_rows(
            """
            SELECT u.user_id, u.uuid, u.player_id, COALESCE(p.name, '') AS name, COALESCE(p.message, '') AS message,
                   COALESCE(s.level, 0) AS level, COALESCE(s.exp, 0) AS exp,
                   COALESCE(g.paid_gem, 0) AS paid_gem, COALESCE(g.free_gem, 0) AS free_gem,
                   COALESCE(u.latest_version, 0) AS latest_version,
                   COALESCE(q.completed_quests, 0) AS completed_quests
            FROM users u
            LEFT JOIN user_profile p ON p.user_id = u.user_id
            LEFT JOIN user_status s ON s.user_id = u.user_id
            LEFT JOIN user_gem g ON g.user_id = u.user_id
            LEFT JOIN (
                SELECT user_id, COUNT(*) AS completed_quests
                FROM user_quests
                WHERE clear_count > 0 OR last_clear_datetime > 0
                GROUP BY user_id
            ) q ON q.user_id = u.user_id
            ORDER BY u.user_id
            """
        )
        return [
            {
                "userId": to_int(row["user_id"]),
                "uuid": stringify(row["uuid"]),
                "playerId": to_int(row["player_id"]),
                "name": stringify(row["name"]),
                "message": stringify(row["message"]),
                "level": to_int(row["level"]),
                "exp": to_int(row["exp"]),
                "paidGem": to_int(row["paid_gem"]),
                "freeGem": to_int(row["free_gem"]),
                "latestVersion": to_int(row["latest_version"]),
                "completedQuests": to_int(row["completed_quests"]),
            }
            for row in rows
        ]

    def user_summary(self, user_id: str) -> dict[str, Any] | None:
        rows = self.query_rows(
            """
            SELECT u.user_id, u.uuid, u.player_id, COALESCE(p.name, '') AS name, COALESCE(p.message, '') AS message,
                   COALESCE(s.level, 0) AS level, COALESCE(s.exp, 0) AS exp,
                   COALESCE(g.paid_gem, 0) AS paid_gem, COALESCE(g.free_gem, 0) AS free_gem,
                   COALESCE(u.latest_version, 0) AS latest_version
            FROM users u
            LEFT JOIN user_profile p ON p.user_id = u.user_id
            LEFT JOIN user_status s ON s.user_id = u.user_id
            LEFT JOIN user_gem g ON g.user_id = u.user_id
            WHERE u.user_id = ?
            """,
            (user_id,),
        )
        if not rows:
            return None
        row = rows[0]
        return {
            "userId": to_int(row["user_id"]),
            "uuid": stringify(row["uuid"]),
            "playerId": to_int(row["player_id"]),
            "name": stringify(row["name"]),
            "message": stringify(row["message"]),
            "level": to_int(row["level"]),
            "exp": to_int(row["exp"]),
            "paidGem": to_int(row["paid_gem"]),
            "freeGem": to_int(row["free_gem"]),
            "latestVersion": to_int(row["latest_version"]),
        }

    def active_deck(self, user_id: str, deck_type: str = "", deck_number: str = "") -> dict[str, Any]:
        deck_rows = self.query_rows(
            'SELECT * FROM "user_decks" WHERE "user_id" = ? ORDER BY "deck_type", "user_deck_number", ROWID',
            (user_id,),
        )
        if not deck_rows:
            return {"decks": [], "selectedDeckKey": "", "deck": None, "slots": []}

        deck_options = []
        selected_type = to_int(deck_type) if deck_type.strip() else None
        selected_number = to_int(deck_number) if deck_number.strip() else None
        selected_deck: dict[str, Any] | None = None
        for display_index, row in enumerate(deck_rows, start=1):
            row_type = to_int(row.get("deck_type"))
            row_number = to_int(row.get("user_deck_number"))
            key = f"{row_type}:{row_number}"
            deck_options.append(
                {
                    "key": key,
                    "displayIndex": display_index,
                    "deckType": row_type,
                    "deckNumber": row_number,
                    "name": f"Deck {display_index}",
                    "power": to_int(row.get("power")),
                    "label": f"Deck {display_index}",
                }
            )
            if selected_type == row_type and selected_number == row_number:
                selected_deck = row

        deck = selected_deck or deck_rows[0]
        selected_display_index = next(
            (
                option["displayIndex"]
                for option in deck_options
                if option["key"] == f'{to_int(deck.get("deck_type"))}:{to_int(deck.get("user_deck_number"))}'
            ),
            1,
        )
        slot_columns = [
            (1, "user_deck_character_uuid02"),
            (2, "user_deck_character_uuid01"),
            (3, "user_deck_character_uuid03"),
        ]
        slots: list[dict[str, Any]] = []
        for index, column in slot_columns:
            deck_character_uuid = stringify(deck.get(column)).strip()
            if not deck_character_uuid:
                slots.append({"slot": index})
                continue
            slots.append(self.active_deck_slot(user_id, deck_character_uuid, index))

        return {
            "decks": deck_options,
            "selectedDeckKey": f'{to_int(deck.get("deck_type"))}:{to_int(deck.get("user_deck_number"))}',
            "deck": {
                "displayIndex": selected_display_index,
                "deckType": to_int(deck.get("deck_type")),
                "deckNumber": to_int(deck.get("user_deck_number")),
                "name": f"Deck {selected_display_index}",
                "power": to_int(deck.get("power")),
            },
            "slots": slots,
        }

    def active_deck_slot(self, user_id: str, deck_character_uuid: str, slot_number: int) -> dict[str, Any]:
        rows = self.query_rows(
            'SELECT * FROM "user_deck_characters" WHERE "user_id" = ? AND "user_deck_character_uuid" = ? LIMIT 1',
            (user_id, deck_character_uuid),
        )
        if not rows:
            return {"slot": slot_number}

        row = rows[0]
        costume_uuid = stringify(row.get("user_costume_uuid")).strip()
        weapon_uuid = stringify(row.get("main_user_weapon_uuid")).strip()
        companion_uuid = stringify(row.get("user_companion_uuid")).strip()
        thought_uuid = stringify(row.get("user_thought_uuid")).strip()

        costume_row = self.query_single_row(
            'SELECT * FROM "user_costumes" WHERE "user_id" = ? AND "user_costume_uuid" = ? LIMIT 1',
            (user_id, costume_uuid),
        )
        weapon_row = self.query_single_row(
            'SELECT * FROM "user_weapons" WHERE "user_id" = ? AND "user_weapon_uuid" = ? LIMIT 1',
            (user_id, weapon_uuid),
        )
        companion_row = self.query_single_row(
            'SELECT * FROM "user_companions" WHERE "user_id" = ? AND "user_companion_uuid" = ? LIMIT 1',
            (user_id, companion_uuid),
        )
        thought_row = self.query_single_row(
            'SELECT * FROM "user_thoughts" WHERE "user_id" = ? AND "user_thought_uuid" = ? LIMIT 1',
            (user_id, thought_uuid),
        )
        sub_weapon_rows = self.query_rows(
            'SELECT sw.ordinal, sw.user_weapon_uuid, w.weapon_id FROM "user_deck_sub_weapons" sw '
            'JOIN "user_weapons" w ON w.user_id = sw.user_id AND w.user_weapon_uuid = sw.user_weapon_uuid '
            'WHERE sw.user_id = ? AND sw.user_deck_character_uuid = ? ORDER BY sw.ordinal',
            (user_id, deck_character_uuid),
        )
        part_rows = self.query_rows(
            'SELECT dp.ordinal, dp.user_parts_uuid, p.parts_id FROM "user_deck_parts" dp '
            'JOIN "user_parts" p ON p.user_id = dp.user_id AND p.user_parts_uuid = dp.user_parts_uuid '
            'WHERE dp.user_id = ? AND dp.user_deck_character_uuid = ? ORDER BY dp.ordinal',
            (user_id, deck_character_uuid),
        )

        costume_id = stringify(costume_row.get("costume_id")) if costume_row else ""
        weapon_id = stringify(weapon_row.get("weapon_id")) if weapon_row else ""
        companion_id = stringify(companion_row.get("companion_id")) if companion_row else ""
        thought_id = stringify(thought_row.get("thought_id")) if thought_row else ""

        costume_record = self.costumes.get(costume_id) or self.playable_costumes.get(costume_id, {})
        weapon_record = self.weapons.get(weapon_id, {})
        companion_record = self.companions.get(companion_id, {})
        thought_record = self.thoughts.get(thought_id, {})

        sub_weapons = []
        for sub_row in sub_weapon_rows:
            sub_weapon_id = stringify(sub_row.get("weapon_id")).strip()
            sub_record = self.weapons.get(sub_weapon_id, {})
            sub_weapons.append(
                {
                    "userWeaponUuid": stringify(sub_row.get("user_weapon_uuid")).strip(),
                    "weaponId": to_int(sub_weapon_id),
                    "name": stringify(sub_record.get("name")).strip() or f"Weapon {sub_weapon_id}",
                    "imageUrl": self.find_image_url(
                        "weapon",
                        [
                            stringify(sub_record.get("weapon_actor_asset_id")).strip(),
                            *[stringify(value).strip() for value in sub_record.get("weapon_name_asset_ids", [])],
                        ],
                        ["full", "large", "standard"],
                    ),
                }
            )

        parts = []
        for part_row in part_rows:
            part_id = stringify(part_row.get("parts_id")).strip()
            part_record = self.parts.get(part_id, {})
            parts.append(
                {
                    "userPartsUuid": stringify(part_row.get("user_parts_uuid")).strip(),
                    "partsId": to_int(part_id),
                    "name": stringify(part_record.get("name")).strip() or f"Parts {part_id}",
                }
            )

        return {
            "slot": slot_number,
            "deckCharacterUuid": deck_character_uuid,
            "power": to_int(row.get("power")),
            "costume": {
                "userCostumeUuid": costume_uuid,
                "costumeId": to_int(costume_id),
                "characterId": to_int(costume_record.get("CharacterId")),
                "name": stringify(costume_record.get("name")).strip() or f"Costume {costume_id}",
                "characterName": stringify(costume_record.get("character_name")).strip(),
                "imageUrl": self.find_image_url(
                    "costume",
                    [stringify(costume_record.get("costume_actor_asset_id")).strip()],
                    ["gacha"],
                ),
            }
            if costume_id
            else None,
            "weapon": {
                "userWeaponUuid": weapon_uuid,
                "weaponId": to_int(weapon_id),
                "name": stringify(weapon_record.get("name")).strip() or f"Weapon {weapon_id}",
                "imageUrl": self.find_image_url(
                    "weapon",
                    [
                        stringify(weapon_record.get("weapon_actor_asset_id")).strip(),
                        *[stringify(value).strip() for value in weapon_record.get("weapon_name_asset_ids", [])],
                    ],
                    ["full", "large", "standard"],
                ),
            }
            if weapon_id
            else None,
            "companion": {
                "userCompanionUuid": companion_uuid,
                "companionId": to_int(companion_id),
                "name": stringify(companion_record.get("name")).strip() or f"Companion {companion_id}",
                "imageUrl": self.find_image_url(
                    "companions",
                    [stringify(companion_record.get("companion_actor_asset_id")).strip()],
                    ["full", "large", "standard", "portrait"],
                ),
            }
            if companion_id
            else None,
            "thought": {
                "userThoughtUuid": thought_uuid,
                "thoughtId": to_int(thought_id),
                "name": stringify(thought_record.get("name")).strip() or f"Thought {thought_id}",
            }
            if thought_id
            else None,
            "parts": parts,
            "subWeapons": sub_weapons,
        }

    def overview(self) -> dict[str, Any]:
        counts = {}
        for table in (
            "users",
            "user_profile",
            "user_status",
            "user_gem",
            "user_materials",
            "user_consumable_items",
            "user_characters",
            "user_costumes",
            "user_weapons",
            "user_quests",
            "user_missions",
        ):
            counts[table] = self.must_count(table)
        return {
            "dbPath": display_path(self.db_path),
            "userCount": self.must_count("users"),
            "tableCount": len(self.schema),
            "rowCounts": counts,
            "users": self.list_users(),
            "schema": {name: schema.to_dict() for name, schema in self.schema.items()},
            "tableGroups": [group.to_dict() for group in self.table_groups],
            "lookupSummary": self.lookup_registry.summary,
            "gachaBannerEditor": self.gacha_banner_editor_summary(),
        }

    def table_rows(self, table: str, user_id: str = "") -> dict[str, Any]:
        schema = self.schema[table]
        sql = f'SELECT * FROM "{table}"'
        params: list[Any] = []
        if user_id and has_column(schema, "user_id"):
            sql += ' WHERE "user_id" = ?'
            params.append(user_id)
        sql += " ORDER BY ROWID"
        rows = self.query_rows(sql, tuple(params))
        context = self.load_user_lookup_context(user_id)

        annotations: list[dict[str, Any]] = []
        keys: list[dict[str, str]] = []
        for row in rows:
            keys.append({column: stringify(row.get(column)) for column in schema.primary_key})
            annotations.append(self.annotate_row(table, row, context))

        return {
            "table": table,
            "schema": schema.to_dict(),
            "rows": rows,
            "annotations": annotations,
            "canEdit": bool(schema.primary_key),
            "keys": keys,
        }

    def annotate_row(self, table: str, row: dict[str, Any], context: UserLookupContext | None) -> dict[str, Any]:
        annotations: dict[str, Any] = {}
        for column, value in row.items():
            entry = self.lookup_registry.resolve_annotation(column, value)
            if entry:
                annotations[column] = entry.to_dict()
        if context is not None:
            for column, value in row.items():
                entry = context.resolve_annotation(column, value)
                if entry:
                    annotations[column] = entry.to_dict()
            for column, entry in context.table_annotations(table, row).items():
                annotations[column] = entry.to_dict()
        if table == "user_weapons":
            weapon_id = stringify(row.get("weapon_id")).strip()
            if weapon_id:
                image_url = self.weapon_image_url(weapon_id)
                if image_url:
                    annotation = dict(annotations.get("weapon_id", {"label": f"Weapon {weapon_id}"}))
                    annotation["imageUrl"] = image_url
                    annotations["weapon_id"] = annotation
        elif table == "user_costumes":
            costume_id = stringify(row.get("costume_id")).strip()
            if costume_id:
                image_url = self.costume_image_url(costume_id)
                if image_url:
                    annotation = dict(annotations.get("costume_id", {"label": f"Costume {costume_id}"}))
                    annotation["imageUrl"] = image_url
                    annotations["costume_id"] = annotation
        elif table == "user_characters":
            character_id = stringify(row.get("character_id")).strip()
            if character_id:
                image_url = self.character_image_url(character_id)
                if image_url:
                    annotation = dict(annotations.get("character_id", {"label": f"Character {character_id}"}))
                    annotation["imageUrl"] = image_url
                    annotations["character_id"] = annotation
        elif table == "user_companions":
            companion_id = stringify(row.get("companion_id")).strip()
            if companion_id:
                image_url = self.companion_image_url(companion_id)
                if image_url:
                    annotation = dict(annotations.get("companion_id", {"label": f"Companion {companion_id}"}))
                    annotation["imageUrl"] = image_url
                    annotations["companion_id"] = annotation
        elif table == "user_consumable_items":
            consumable_item_id = stringify(row.get("consumable_item_id")).strip()
            if consumable_item_id:
                image_url = self.consumable_image_url(consumable_item_id)
                if image_url:
                    annotation = dict(
                        annotations.get("consumable_item_id", {"label": f"Consumable {consumable_item_id}"})
                    )
                    annotation["imageUrl"] = image_url
                    annotations["consumable_item_id"] = annotation
        elif table == "user_materials":
            material_id = stringify(row.get("material_id")).strip()
            if material_id:
                image_url = self.material_image_url(material_id)
                if image_url:
                    annotation = dict(annotations.get("material_id", {"label": f"Material {material_id}"}))
                    annotation["imageUrl"] = image_url
                    annotations["material_id"] = annotation
        return annotations

    def lookup_options(self, column: str, user_id: str = "") -> list[dict[str, Any]]:
        entries = self.lookup_registry.resolve_column_entries(column)
        context = self.load_user_lookup_context(user_id) if user_id else None
        if context:
            context_entries = context.resolve_options(column)
            if context_entries:
                entries = context_entries
        options = [
            {
                "value": value,
                "label": entry.label,
                **({"detail": entry.detail} if entry.detail else {}),
                **({"group": entry.group} if entry.group else {}),
            }
            for value, entry in entries.items()
        ]
        options.sort(
            key=lambda item: (
                group_sort_key(item.get("group", "")),
                item["label"].lower(),
                item["value"],
            )
        )
        return options

    def load_user_lookup_context(self, user_id: str) -> UserLookupContext:
        if not user_id.strip():
            return UserLookupContext()

        ctx = UserLookupContext()
        weapon_entries: dict[str, LookupEntry] = {}
        costume_entries: dict[str, LookupEntry] = {}
        companion_entries: dict[str, LookupEntry] = {}
        parts_entries: dict[str, LookupEntry] = {}
        thought_entries: dict[str, LookupEntry] = {}
        deck_entries: dict[str, LookupEntry] = {}

        def load_owned_entities(
            table: str,
            uuid_column: str,
            id_column: str,
            target: dict[str, OwnedEntityRef],
            registry_column: str,
        ) -> dict[str, LookupEntry]:
            rows = self.query_rows(f'SELECT * FROM "{table}" WHERE "user_id" = ? ORDER BY ROWID', (user_id,))
            entries: dict[str, LookupEntry] = {}
            registry_entries = self.lookup_registry.columns.get(registry_column, {})
            for row in rows:
                uuid = stringify(row.get(uuid_column))
                entity_id = stringify(row.get(id_column))
                if not uuid or not entity_id:
                    continue
                entry = registry_entries.get(entity_id, LookupEntry(f"{id_column} {entity_id}"))
                ref = OwnedEntityRef(entry=entry, entity_id=entity_id)
                if table == "user_weapons":
                    ref.skill_slots = self.lookup_registry.weapon_skill_slots.get(entity_id, {})
                    ref.ability_slots = self.lookup_registry.weapon_ability_slots.get(entity_id, {})
                if table == "user_costumes":
                    ref.limit_break = to_int(row.get("limit_break_count"))
                    active_skill = self.lookup_registry.costume_active_skills.get(entity_id, {})
                    ref.active_skill_by_limit = active_skill
                    chosen = self.lookup_registry.costume_active_skill_for_limit_break(entity_id, ref.limit_break)
                    if chosen:
                        entry = LookupEntry(entry.label, join_detail(entry.detail, f"active skill {chosen.label}"))
                        ref.entry = entry
                target[uuid] = ref
                entries[uuid] = ref.entry
            return entries

        weapon_entries = load_owned_entities("user_weapons", "user_weapon_uuid", "weapon_id", ctx.weapon_refs, "weapon_id")
        costume_entries = load_owned_entities("user_costumes", "user_costume_uuid", "costume_id", ctx.costume_refs, "costume_id")
        companion_entries = load_owned_entities("user_companions", "user_companion_uuid", "companion_id", ctx.companion_refs, "companion_id")
        parts_entries = load_owned_entities("user_parts", "user_parts_uuid", "parts_id", ctx.parts_refs, "parts_id")
        thought_entries = load_owned_entities("user_thoughts", "user_thought_uuid", "thought_id", ctx.thought_refs, "thought_id")

        ctx.set_aliases("user_weapon_uuid", "main_user_weapon_uuid", entries=weapon_entries)
        ctx.set_aliases("user_costume_uuid", entries=costume_entries)
        ctx.set_aliases("user_companion_uuid", entries=companion_entries)
        ctx.set_aliases("user_parts_uuid", entries=parts_entries)
        ctx.set_aliases("user_thought_uuid", entries=thought_entries)

        deck_rows = self.query_rows(
            'SELECT * FROM "user_deck_characters" WHERE "user_id" = ? ORDER BY ROWID',
            (user_id,),
        )
        for row in deck_rows:
            deck_uuid = stringify(row.get("user_deck_character_uuid"))
            if not deck_uuid:
                continue
            costume_entry = costume_entries.get(stringify(row.get("user_costume_uuid")))
            weapon_entry = weapon_entries.get(stringify(row.get("main_user_weapon_uuid")))
            companion_entry = companion_entries.get(stringify(row.get("user_companion_uuid")))
            thought_entry = thought_entries.get(stringify(row.get("user_thought_uuid")))
            label = costume_entry.label if costume_entry else f"Deck Character {deck_uuid}"
            detail = join_detail(
                "deck character",
                f"main weapon {weapon_entry.label}" if weapon_entry else "",
                f"companion {companion_entry.label}" if companion_entry else "",
                f"thought {thought_entry.label}" if thought_entry else "",
            )
            entry = LookupEntry(label, detail)
            deck_entries[deck_uuid] = entry
            ctx.deck_character_refs[deck_uuid] = entry

        ctx.set_aliases(
            "user_deck_character_uuid",
            "user_deck_character_uuid01",
            "user_deck_character_uuid02",
            "user_deck_character_uuid03",
            entries=deck_entries,
        )

        return ctx

    def upsert_row(self, table: str, row: dict[str, Any]) -> None:
        if table == "user_characters":
            self.upsert_character_bundle(dict(row))
            return
        if table == "user_costumes":
            self.upsert_costume_bundle(dict(row))
            return
        schema = self.schema[table]
        row = self.apply_table_defaults(table, dict(row))
        present_columns = [column for column in schema.columns if column.name in row]
        if not present_columns:
            raise ValueError("no known columns in row payload")

        columns_sql = ", ".join(quote_ident(column.name) for column in present_columns)
        placeholders = ", ".join("?" for _ in present_columns)
        values = [coerce_sql_value(column, row[column.name]) for column in present_columns]

        if schema.primary_key:
            update_columns = [column for column in present_columns if column.name not in schema.primary_key]
            if update_columns:
                update_sql = ", ".join(
                    f'{quote_ident(column.name)} = excluded.{quote_ident(column.name)}' for column in update_columns
                )
                conflict_sql = f'ON CONFLICT ({", ".join(quote_ident(name) for name in schema.primary_key)}) DO UPDATE SET {update_sql}'
            else:
                conflict_sql = f'ON CONFLICT ({", ".join(quote_ident(name) for name in schema.primary_key)}) DO NOTHING'
        else:
            conflict_sql = ""

        query = f'INSERT INTO {quote_ident(table)} ({columns_sql}) VALUES ({placeholders}) {conflict_sql}'.strip()
        with closing(self.connect()) as connection:
            connection.execute(query, values)
            connection.commit()

    def upsert_character_bundle(self, row: dict[str, Any]) -> None:
        row = self.apply_table_defaults("user_characters", row)
        user_id = stringify(row.get("user_id")).strip()
        character_id = stringify(row.get("character_id")).strip()
        if not user_id or not character_id:
            raise ValueError("user_characters requires user_id and character_id")

        character_record = self.playable_characters.get(character_id, {})
        costume_id = self.default_costume_id_for_character(character_record)
        weapon_id = stringify(character_record.get("DefaultWeaponId")).strip()
        if weapon_id == "0":
            weapon_id = ""

        acquired_at = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        with closing(self.connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._upsert_with_connection(connection, "user_characters", row)

            costume_uuid = ""
            if costume_id:
                costume_uuid = self.ensure_owned_costume(connection, user_id, costume_id, acquired_at)
                self.ensure_costume_active_skill(connection, user_id, costume_uuid, acquired_at)

            weapon_uuid = ""
            if weapon_id:
                weapon_uuid = self.ensure_owned_weapon(connection, user_id, weapon_id, acquired_at)
                self.ensure_weapon_support_rows(connection, user_id, weapon_id, weapon_uuid, acquired_at)

            connection.commit()

    def upsert_costume_bundle(self, row: dict[str, Any]) -> None:
        row = self.apply_table_defaults("user_costumes", row)
        user_id = stringify(row.get("user_id")).strip()
        costume_id = stringify(row.get("costume_id")).strip()
        if not user_id or not costume_id:
            raise ValueError("user_costumes requires user_id and costume_id")

        costume_record = self.playable_costumes.get(costume_id, {})
        character_id = stringify(costume_record.get("CharacterId")).strip()
        character_record = self.playable_characters.get(character_id, {}) if character_id and character_id != "0" else {}
        weapon_id = stringify(character_record.get("DefaultWeaponId")).strip()
        if weapon_id == "0":
            weapon_id = ""
        user_costume_uuid = stringify(row.get("user_costume_uuid")).strip()
        acquired_at = to_int(row.get("acquisition_datetime"))
        if acquired_at <= 0:
            acquired_at = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        with closing(self.connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._upsert_with_connection(connection, "user_costumes", row)
            self.ensure_costume_active_skill(connection, user_id, user_costume_uuid, acquired_at)

            if character_id and character_id != "0":
                self._upsert_with_connection(
                    connection,
                    "user_characters",
                    self.apply_table_defaults(
                        "user_characters",
                        {
                            "user_id": user_id,
                            "character_id": character_id,
                        },
                    ),
                )

            weapon_uuid = ""
            if weapon_id:
                weapon_uuid = self.ensure_owned_weapon(connection, user_id, weapon_id, acquired_at)
                self.ensure_weapon_support_rows(connection, user_id, weapon_id, weapon_uuid, acquired_at)

            connection.commit()

    def apply_table_defaults(self, table: str, row: dict[str, Any]) -> dict[str, Any]:
        schema = self.schema.get(table)
        user_id = stringify(row.get("user_id")).strip()
        if schema and user_id:
            for column_name in schema.primary_key:
                if column_name.endswith("_uuid") and not stringify(row.get(column_name)).strip():
                    row[column_name] = scoped_uuid_for_user(user_id)

        if table == "user_costumes":
            defaults: dict[str, Any] = {
                "limit_break_count": 0,
                "level": 1,
                "exp": 0,
                "headup_display_view_id": 1,
                "acquisition_datetime": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
                "awaken_count": 0,
                "latest_version": 0,
            }
            for field, default_value in defaults.items():
                current = stringify(row.get(field)).strip()
                if current == "":
                    row[field] = default_value
        elif table == "user_weapons":
            defaults = {
                "level": 1,
                "exp": 0,
                "limit_break_count": 0,
                "is_protected": 0,
                "acquisition_datetime": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
                "latest_version": 0,
            }
            for field, default_value in defaults.items():
                current = stringify(row.get(field)).strip()
                if current == "":
                    row[field] = default_value
        elif table == "user_characters":
            defaults = {
                "level": 1,
                "exp": 0,
                "latest_version": 0,
            }
            for field, default_value in defaults.items():
                current = stringify(row.get(field)).strip()
                if current == "":
                    row[field] = default_value
        elif table == "user_companions":
            defaults = {
                "headup_display_view_id": 1,
                "level": 1,
                "acquisition_datetime": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
                "latest_version": 0,
            }
            for field, default_value in defaults.items():
                current = stringify(row.get(field)).strip()
                if current == "" or current == "0":
                    row[field] = default_value
        return row

    def _upsert_with_connection(self, connection: sqlite3.Connection, table: str, row: dict[str, Any]) -> None:
        schema = self.schema[table]
        present_columns = [column for column in schema.columns if column.name in row]
        if not present_columns:
            raise ValueError("no known columns in row payload")

        columns_sql = ", ".join(quote_ident(column.name) for column in present_columns)
        placeholders = ", ".join("?" for _ in present_columns)
        values = [coerce_sql_value(column, row[column.name]) for column in present_columns]

        if schema.primary_key:
            update_columns = [column for column in present_columns if column.name not in schema.primary_key]
            if update_columns:
                update_sql = ", ".join(
                    f'{quote_ident(column.name)} = excluded.{quote_ident(column.name)}' for column in update_columns
                )
                conflict_sql = (
                    f'ON CONFLICT ({", ".join(quote_ident(name) for name in schema.primary_key)}) '
                    f"DO UPDATE SET {update_sql}"
                )
            else:
                conflict_sql = (
                    f'ON CONFLICT ({", ".join(quote_ident(name) for name in schema.primary_key)}) DO NOTHING'
                )
        else:
            conflict_sql = ""

        query = f'INSERT INTO {quote_ident(table)} ({columns_sql}) VALUES ({placeholders}) {conflict_sql}'.strip()
        connection.execute(query, values)

    def default_costume_id_for_character(self, character_record: dict[str, Any]) -> str:
        costume_id = stringify(character_record.get("DefaultCostumeId")).strip()
        if costume_id and costume_id != "0":
            return costume_id
        character_id = stringify(character_record.get("id")).strip()
        if not character_id:
            return ""
        fallback = [
            record
            for record in self.playable_costumes.values()
            if stringify(record.get("CharacterId")).strip() == character_id
        ]
        if not fallback:
            return ""
        fallback.sort(key=lambda record: to_int(record.get("id")))
        return stringify(fallback[0].get("id")).strip()

    def ensure_owned_costume(
        self,
        connection: sqlite3.Connection,
        user_id: str,
        costume_id: str,
        acquired_at: int,
    ) -> str:
        existing = connection.execute(
            'SELECT user_costume_uuid FROM "user_costumes" WHERE "user_id" = ? AND "costume_id" = ? ORDER BY ROWID LIMIT 1',
            (user_id, costume_id),
        ).fetchone()
        if existing:
            return stringify(existing[0])

        row = self.apply_table_defaults(
            "user_costumes",
            {
                "user_id": user_id,
                "user_costume_uuid": scoped_uuid_for_user(user_id),
                "costume_id": costume_id,
                "acquisition_datetime": acquired_at,
            },
        )
        self._upsert_with_connection(connection, "user_costumes", row)
        return stringify(row["user_costume_uuid"])

    def ensure_costume_active_skill(
        self,
        connection: sqlite3.Connection,
        user_id: str,
        user_costume_uuid: str,
        acquired_at: int,
    ) -> None:
        if not user_costume_uuid:
            return
        existing = connection.execute(
            'SELECT 1 FROM "user_costume_active_skills" WHERE "user_id" = ? AND "user_costume_uuid" = ? LIMIT 1',
            (user_id, user_costume_uuid),
        ).fetchone()
        if existing:
            return
        connection.execute(
            'INSERT INTO "user_costume_active_skills" ("user_id", "user_costume_uuid", "level", "acquisition_datetime", "latest_version") VALUES (?, ?, ?, ?, ?)',
            (user_id, user_costume_uuid, 1, acquired_at, 0),
        )

    def ensure_owned_weapon(
        self,
        connection: sqlite3.Connection,
        user_id: str,
        weapon_id: str,
        acquired_at: int,
    ) -> str:
        existing = connection.execute(
            'SELECT user_weapon_uuid FROM "user_weapons" WHERE "user_id" = ? AND "weapon_id" = ? ORDER BY ROWID LIMIT 1',
            (user_id, weapon_id),
        ).fetchone()
        if existing:
            return stringify(existing[0])

        row = self.apply_table_defaults(
            "user_weapons",
            {
                "user_id": user_id,
                "user_weapon_uuid": scoped_uuid_for_user(user_id),
                "weapon_id": weapon_id,
                "acquisition_datetime": acquired_at,
            },
        )
        self._upsert_with_connection(connection, "user_weapons", row)
        return stringify(row["user_weapon_uuid"])

    def ensure_weapon_support_rows(
        self,
        connection: sqlite3.Connection,
        user_id: str,
        weapon_id: str,
        user_weapon_uuid: str,
        acquired_at: int,
    ) -> None:
        if not user_weapon_uuid:
            return

        existing_skill_slots = {
            to_int(row[0])
            for row in connection.execute(
                'SELECT slot_number FROM "user_weapon_skills" WHERE "user_id" = ? AND "user_weapon_uuid" = ?',
                (user_id, user_weapon_uuid),
            ).fetchall()
        }
        for record in self.weapon_skill_defs.get(weapon_id, []):
            slot_number = to_int(record.get("slot_number"))
            if slot_number <= 0 or slot_number in existing_skill_slots:
                continue
            connection.execute(
                'INSERT INTO "user_weapon_skills" ("user_id", "user_weapon_uuid", "slot_number", "level") VALUES (?, ?, ?, ?)',
                (user_id, user_weapon_uuid, slot_number, 1),
            )

        existing_ability_slots = {
            to_int(row[0])
            for row in connection.execute(
                'SELECT slot_number FROM "user_weapon_abilities" WHERE "user_id" = ? AND "user_weapon_uuid" = ?',
                (user_id, user_weapon_uuid),
            ).fetchall()
        }
        for record in self.weapon_ability_defs.get(weapon_id, []):
            slot_number = to_int(record.get("slot_number"))
            if slot_number <= 0 or slot_number in existing_ability_slots:
                continue
            connection.execute(
                'INSERT INTO "user_weapon_abilities" ("user_id", "user_weapon_uuid", "slot_number", "level") VALUES (?, ?, ?, ?)',
                (user_id, user_weapon_uuid, slot_number, 1),
            )

        existing_note = connection.execute(
            'SELECT 1 FROM "user_weapon_notes" WHERE "user_id" = ? AND "weapon_id" = ? LIMIT 1',
            (user_id, weapon_id),
        ).fetchone()
        if not existing_note:
            connection.execute(
                'INSERT INTO "user_weapon_notes" ("user_id", "weapon_id", "max_level", "max_limit_break_count", "first_acquisition_datetime", "latest_version") VALUES (?, ?, ?, ?, ?, ?)',
                (user_id, weapon_id, 1, 0, acquired_at, acquired_at),
            )

        existing_story = connection.execute(
            'SELECT 1 FROM "user_weapon_stories" WHERE "user_id" = ? AND "weapon_id" = ? LIMIT 1',
            (user_id, weapon_id),
        ).fetchone()
        if not existing_story:
            connection.execute(
                'INSERT INTO "user_weapon_stories" ("user_id", "weapon_id", "released_max_story_index", "latest_version") VALUES (?, ?, ?, ?)',
                (user_id, weapon_id, 1, acquired_at),
            )

    def character_id_for_costume(self, costume_id: str) -> str:
        costume_record = self.playable_costumes.get(costume_id, {}) or self.costumes.get(costume_id, {})
        return stringify(costume_record.get("CharacterId")).strip()

    def _delete_deck_character_bundle(
        self,
        connection: sqlite3.Connection,
        user_id: str,
        user_deck_character_uuid: str,
    ) -> None:
        if not user_deck_character_uuid:
            return

        connection.execute(
            'DELETE FROM "user_deck_sub_weapons" WHERE "user_id" = ? AND "user_deck_character_uuid" = ?',
            (user_id, user_deck_character_uuid),
        )
        connection.execute(
            'DELETE FROM "user_deck_parts" WHERE "user_id" = ? AND "user_deck_character_uuid" = ?',
            (user_id, user_deck_character_uuid),
        )
        for column_name in ("user_deck_character_uuid01", "user_deck_character_uuid02", "user_deck_character_uuid03"):
            connection.execute(
                f'UPDATE "user_decks" SET {quote_ident(column_name)} = NULL WHERE "user_id" = ? AND {quote_ident(column_name)} = ?',
                (user_id, user_deck_character_uuid),
            )
        connection.execute(
            'DELETE FROM "user_deck_characters" WHERE "user_id" = ? AND "user_deck_character_uuid" = ?',
            (user_id, user_deck_character_uuid),
        )

    def _delete_costume_bundle(
        self,
        connection: sqlite3.Connection,
        user_id: str,
        user_costume_uuid: str,
    ) -> None:
        if not user_costume_uuid:
            return

        deck_character_rows = connection.execute(
            'SELECT "user_deck_character_uuid" FROM "user_deck_characters" WHERE "user_id" = ? AND "user_costume_uuid" = ?',
            (user_id, user_costume_uuid),
        ).fetchall()
        for row in deck_character_rows:
            self._delete_deck_character_bundle(connection, user_id, stringify(row[0]).strip())

        connection.execute(
            'DELETE FROM "user_costume_active_skills" WHERE "user_id" = ? AND "user_costume_uuid" = ?',
            (user_id, user_costume_uuid),
        )
        connection.execute(
            'DELETE FROM "user_costumes" WHERE "user_id" = ? AND "user_costume_uuid" = ?',
            (user_id, user_costume_uuid),
        )

    def _delete_character_if_no_costumes(
        self,
        connection: sqlite3.Connection,
        user_id: str,
        character_id: str,
    ) -> None:
        if not user_id or not character_id:
            return

        remaining_costume_rows = connection.execute(
            'SELECT "costume_id" FROM "user_costumes" WHERE "user_id" = ?',
            (user_id,),
        ).fetchall()
        for row in remaining_costume_rows:
            if self.character_id_for_costume(stringify(row[0]).strip()) == character_id:
                return

        connection.execute(
            'DELETE FROM "user_characters" WHERE "user_id" = ? AND "character_id" = ?',
            (user_id, character_id),
        )

    def _delete_character_bundle(
        self,
        connection: sqlite3.Connection,
        user_id: str,
        character_id: str,
    ) -> None:
        if not user_id or not character_id:
            return

        costume_rows = connection.execute(
            'SELECT "user_costume_uuid", "costume_id" FROM "user_costumes" WHERE "user_id" = ?',
            (user_id,),
        ).fetchall()
        for user_costume_uuid, costume_id in costume_rows:
            if self.character_id_for_costume(stringify(costume_id).strip()) != character_id:
                continue
            self._delete_costume_bundle(connection, user_id, stringify(user_costume_uuid).strip())

        connection.execute(
            'DELETE FROM "user_characters" WHERE "user_id" = ? AND "character_id" = ?',
            (user_id, character_id),
        )

    def delete_row(self, table: str, key: dict[str, Any]) -> None:
        schema = self.schema[table]
        if not schema.primary_key:
            raise ValueError("table has no primary key")
        missing = [name for name in schema.primary_key if name not in key]
        if missing:
            raise ValueError(f"missing primary key column(s): {', '.join(missing)}")
        where_sql = " AND ".join(f"{quote_ident(name)} = ?" for name in schema.primary_key)
        values = [key[name] for name in schema.primary_key]
        with closing(self.connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            if table == "user_characters":
                self._delete_character_bundle(
                    connection,
                    stringify(key.get("user_id")).strip(),
                    stringify(key.get("character_id")).strip(),
                )
            elif table == "user_costumes":
                user_id = stringify(key.get("user_id")).strip()
                user_costume_uuid = stringify(key.get("user_costume_uuid")).strip()
                costume_row = connection.execute(
                    'SELECT "costume_id" FROM "user_costumes" WHERE "user_id" = ? AND "user_costume_uuid" = ? LIMIT 1',
                    (user_id, user_costume_uuid),
                ).fetchone()
                character_id = self.character_id_for_costume(stringify(costume_row[0]).strip()) if costume_row else ""
                self._delete_costume_bundle(connection, user_id, user_costume_uuid)
                self._delete_character_if_no_costumes(connection, user_id, character_id)
            else:
                connection.execute(f'DELETE FROM {quote_ident(table)} WHERE {where_sql}', values)
            connection.commit()

    def delete_user(self, user_id: str) -> None:
        try:
            int(user_id)
        except ValueError as exc:
            raise ValueError(f"invalid user id {user_id!r}") from exc

        child_tables = sorted(
            name
            for name, schema in self.schema.items()
            if name != "users" and has_column(schema, "user_id")
        )
        with closing(self.connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                for table in child_tables:
                    connection.execute(f'DELETE FROM {quote_ident(table)} WHERE "user_id" = ?', (user_id,))
                connection.execute('DELETE FROM "users" WHERE "user_id" = ?', (user_id,))
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def invalidate_user_sessions(self, user_id: str) -> int:
        try:
            int(user_id)
        except ValueError as exc:
            raise ValueError(f"invalid user id {user_id!r}") from exc

        with closing(self.connect()) as connection:
            cursor = connection.execute('DELETE FROM "sessions" WHERE "user_id" = ?', (user_id,))
            connection.commit()
            return cursor.rowcount


class EditorRequestHandler(BaseHTTPRequestHandler):
    app_state: EditorApp

    def handle_backend_error(self, exc: Exception) -> None:
        if isinstance(exc, sqlite3.Error):
            self.send_text_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))
            return
        raise exc

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            if parsed.path == "/api/overview":
                self.write_json(self.app_state.overview())
                return
            if parsed.path == "/api/master-data/gacha-banners":
                self.write_json(self.app_state.gacha_banner_catalog())
                return
            if parsed.path == "/api/master-data/events":
                self.write_json(self.app_state.event_selector_catalog())
                return
            if parsed.path == "/api/master-data/presets":
                self.write_json(self.app_state.preset_catalog())
                return
            if parsed.path == "/api/users":
                self.write_json(self.app_state.list_users())
                return
            if parsed.path.startswith("/api/lookups/"):
                self.handle_lookup(parsed)
                return
            if parsed.path.startswith("/api/user/"):
                self.handle_user_routes(parsed)
                return
            if parsed.path.startswith("/api/table/"):
                self.handle_table_routes(parsed)
                return
            self.serve_static(parsed.path)
        except Exception as exc:  # pragma: no cover - HTTP server boundary
            self.handle_backend_error(exc)

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            if parsed.path == "/api/master-data/gacha-banners":
                try:
                    payload = self.read_json_body()
                    banner_ids = payload.get("activeBannerIds", [])
                    if not isinstance(banner_ids, list):
                        raise ValueError("activeBannerIds must be an array")
                    self.write_json(self.app_state.save_active_gacha_banners(banner_ids))
                except ValueError as exc:
                    self.send_text_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            if parsed.path == "/api/master-data/events":
                try:
                    payload = self.read_json_body()
                    group = stringify(payload.get("group")).strip()
                    active_ids = payload.get("activeIds", [])
                    if not isinstance(active_ids, list):
                        raise ValueError("activeIds must be an array")
                    self.write_json(self.app_state.save_event_selector_group(group, active_ids))
                except ValueError as exc:
                    self.send_text_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            if parsed.path.startswith("/api/user/"):
                self.handle_user_routes(parsed)
                return
            if parsed.path.startswith("/api/table/"):
                self.handle_table_routes(parsed)
                return
            self.send_error(HTTPStatus.NOT_FOUND)
        except Exception as exc:  # pragma: no cover - HTTP server boundary
            self.handle_backend_error(exc)

    def do_DELETE(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/user/"):
                self.handle_user_routes(parsed)
                return
            if parsed.path.startswith("/api/table/"):
                self.handle_table_routes(parsed)
                return
            self.send_error(HTTPStatus.NOT_FOUND)
        except Exception as exc:  # pragma: no cover - HTTP server boundary
            self.handle_backend_error(exc)

    def handle_lookup(self, parsed) -> None:
        column = parsed.path.removeprefix("/api/lookups/")
        if not column or "/" in column:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        params = parse_qs(parsed.query)
        user_id = first_param(params, "user_id")
        self.write_json({"column": column, "options": self.app_state.lookup_options(column, user_id)})

    def handle_user_routes(self, parsed) -> None:
        parts = [part for part in parsed.path.removeprefix("/api/user/").split("/") if part]
        if len(parts) == 1 and self.command == "DELETE":
            try:
                self.app_state.delete_user(parts[0])
            except ValueError as exc:
                self.send_text_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            self.write_json({"ok": True})
            return
        if len(parts) == 2 and parts[1] == "sessions" and self.command == "DELETE":
            try:
                deleted = self.app_state.invalidate_user_sessions(parts[0])
            except ValueError as exc:
                self.send_text_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            self.write_json({"ok": True, "deletedSessions": deleted})
            return
        if len(parts) < 2:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        user_id = parts[0]
        if len(parts) == 2 and parts[1] == "summary" and self.command == "GET":
            summary = self.app_state.user_summary(user_id)
            if summary is None:
                self.send_text_error(HTTPStatus.NOT_FOUND, "user not found")
                return
            self.write_json(summary)
            return
        if len(parts) == 2 and parts[1] == "active-deck" and self.command == "GET":
            params = parse_qs(parsed.query)
            self.write_json(
                self.app_state.active_deck(
                    user_id,
                    first_param(params, "deck_type"),
                    first_param(params, "deck_number"),
                )
            )
            return
        if len(parts) == 3 and parts[1] == "table":
            table = parts[2]
            self.handle_table_action(table, user_id, parsed)
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def handle_table_routes(self, parsed) -> None:
        table = parsed.path.removeprefix("/api/table/")
        if not table or "/" in table:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        params = parse_qs(parsed.query)
        self.handle_table_action(table, first_param(params, "user_id"), parsed)

    def handle_table_action(self, table: str, user_id: str, parsed) -> None:
        if table not in self.app_state.schema:
            self.send_text_error(HTTPStatus.BAD_REQUEST, "unsupported table")
            return
        if self.command == "GET":
            self.write_json(self.app_state.table_rows(table, user_id))
            return
        if self.command == "POST":
            try:
                payload = self.read_json_body()
                row = payload.get("row")
                if not isinstance(row, dict):
                    raise ValueError("row is required")
                schema = self.app_state.schema[table]
                if user_id and has_column(schema, "user_id"):
                    row["user_id"] = user_id
                self.app_state.upsert_row(table, row)
            except ValueError as exc:
                self.send_text_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            self.write_json({"ok": True})
            return
        if self.command == "DELETE":
            try:
                payload = self.read_json_body()
                key = payload.get("key") if isinstance(payload, dict) else None
                if key is None:
                    key = {}
                if not isinstance(key, dict):
                    raise ValueError("key must be an object")
                schema = self.app_state.schema[table]
                if user_id and has_column(schema, "user_id"):
                    key["user_id"] = user_id
                self.app_state.delete_row(table, key)
            except ValueError as exc:
                self.send_text_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            self.write_json({"ok": True})
            return
        self.send_text_error(HTTPStatus.METHOD_NOT_ALLOWED, "method not allowed")

    def serve_static(self, path: str) -> None:
        path = unquote(path)
        if path in ("", "/"):
            file_path = WEB_DIR / "index.html"
        elif path.startswith("/theme/"):
            file_path = THEME_DIR / path.removeprefix("/theme/")
        elif path.startswith("/images/"):
            file_path = IMAGES_DIR / path.removeprefix("/images/")
        else:
            file_path = WEB_DIR / path.removeprefix("/")

        try:
            resolved = file_path.resolve()
        except FileNotFoundError:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if path.startswith("/theme/"):
            allowed_root = THEME_DIR.resolve()
        elif path.startswith("/images/"):
            allowed_root = IMAGES_DIR.resolve()
        else:
            allowed_root = WEB_DIR.resolve()
        if allowed_root not in resolved.parents and resolved != allowed_root:
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        if not resolved.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        content_type, _ = mimetypes.guess_type(resolved.name)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.end_headers()
        self.wfile.write(resolved.read_bytes())

    def read_json_body(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON: {exc.msg}") from exc
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    def write_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_text_error(self, status: HTTPStatus, message: str) -> None:
        body = message.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        print(f"{self.command} {self.path} - {format % args}")


def row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in row.keys():
        value = row[key]
        result[key] = value
    return result


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def scoped_uuid_for_user(user_id: Any) -> str:
    user_int = max(0, to_int(user_id))
    user_prefix = f"{user_int & 0xFFFFFFFF:08x}"
    base = str(uuid.uuid4())
    return user_prefix + base[8:]


def has_column(schema: TableSchema, name: str) -> bool:
    return any(column.name == name for column in schema.columns)


def build_table_groups(schema: dict[str, TableSchema]) -> list[TableGroup]:
    grouped: dict[tuple[str, str], list[str]] = {}
    for name, table_schema in schema.items():
        key, label = classify_table(name, table_schema)
        grouped.setdefault((key, label), []).append(name)

    groups = [
        TableGroup(key=key, label=label, tables=sorted(tables))
        for (key, label), tables in grouped.items()
    ]
    order = {
        "identity": 0,
        "profile": 1,
        "inventory": 2,
        "economy": 3,
        "collection": 4,
        "deck": 5,
        "progress": 6,
        "combat": 7,
        "user-misc": 8,
        "global": 9,
        "system": 10,
    }
    groups.sort(key=lambda group: (order.get(group.key, 999), group.label))
    return groups


def classify_table(name: str, schema: TableSchema) -> tuple[str, str]:
    if name == "goose_db_version":
        return "system", "System"
    if name in {"users", "sessions"}:
        return "identity", "Identity"
    if name in {"user_profile", "user_status", "user_setting", "user_login", "user_login_bonus"}:
        return "profile", "Profile and Account"
    if name == "user_gem" or any(token in name for token in ("gacha", "shop", "gift")):
        return "economy", "Economy, Shop, and Gacha"
    if "deck" in name:
        return "deck", "Decks and Party Setup"
    if any(token in name for token in ("weapon", "costume", "character", "companion", "thought", "parts")):
        return "collection", "Collection and Loadout"
    if any(token in name for token in ("quest", "mission", "story", "tutorial", "explore", "portal", "cage")):
        return "progress", "Progress and World State"
    if any(token in name for token in ("battle", "hunt", "gimmick")):
        return "combat", "Combat and Challenge State"
    if any(token in name for token in ("material", "consumable", "important_item", "premium_item")):
        return "inventory", "Inventory and Currency"
    if has_column(schema, "user_id"):
        return "user-misc", "Other User Tables"
    return "global", "Other Global Tables"


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    return str(value)


def to_int(value: Any) -> int:
    text = stringify(value).strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except (OverflowError, ValueError):
        return 0


def coerce_sql_value(column: ColumnInfo, value: Any) -> Any:
    if value is None:
        return None
    column_type = column.type.upper()
    if any(token in column_type for token in ("INT", "REAL", "FLOA", "DOUB")):
        text = stringify(value).strip()
        if text == "":
            return None
        try:
            if any(token in column_type for token in ("REAL", "FLOA", "DOUB")):
                return float(text)
            return int(float(text))
        except ValueError:
            return None
    return stringify(value)


def first_param(params: dict[str, list[str]], key: str) -> str:
    values = params.get(key, [])
    return values[0] if values else ""


def join_detail(*parts: str) -> str:
    return " · ".join(part for part in parts if part)


WEAPON_TYPE_LABELS = {
    1: "One-Handed Swords",
    2: "Spears",
    3: "Two-Handed Swords",
    4: "Fists",
    5: "Staves",
    6: "Guns",
}

ALPHA_GROUPS = (
    ("0-9", "0123456789"),
    ("A-D", "ABCD"),
    ("E-H", "EFGH"),
    ("I-L", "IJKL"),
    ("M-P", "MNOP"),
    ("Q-T", "QRST"),
    ("U-Z", "UVWXYZ"),
)


def titleize_identifier(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").strip().title()


def normalize_item_family(label: str) -> str:
    text = stringify(label).strip()
    if not text:
        return ""
    for pattern in (
        r"^(Small|Medium|Large|XL)\s+",
        r"^(Tiny|Minor|Major|Massive)\s+",
    ):
        updated = re.sub(pattern, "", text, flags=re.IGNORECASE)
        if updated != text:
            return updated.strip()
    return ""


def fallback_alpha_group(label: str) -> str:
    text = stringify(label).strip()
    if not text:
        return "Other"
    char = text[0].upper()
    for group, chars in ALPHA_GROUPS:
        if char in chars:
            return group
    return "Other"


def is_unknown_record(record: dict[str, Any]) -> bool:
    if not bool(record.get("name_found", True)):
        return True
    name = stringify(record.get("name")).strip()
    if not name:
        return True
    return bool(
        re.match(
            r"^(Weapon|Character|Costume|Ability|Skill|Material|Consumable|Gacha Banner|Premium Item)\s+\d+$",
            name,
        )
    )


def lookup_group_for_record(file_name: str, record: dict[str, Any]) -> str:
    if is_unknown_record(record):
        return "Unknown"

    name = stringify(record.get("name")).strip()

    if file_name in {"costumes.json", "playable_costumes.json"}:
        return stringify(record.get("character_name")).strip() or "Unknown"

    if file_name in {"characters.json", "playable_characters.json"}:
        return "Playable Characters" if record.get("is_playable_character") else "Other Characters"

    if file_name == "weapons.json":
        return WEAPON_TYPE_LABELS.get(to_int(record.get("WeaponType")), "Unknown")

    if file_name == "companions.json":
        if ":" in name:
            return name.split(":", 1)[0].strip()
        return fallback_alpha_group(name)

    if file_name == "thoughts.json":
        return titleize_identifier(stringify(record.get("display_entity_type")).strip() or "Other")

    if file_name in {"materials.json", "consumables.json", "important_items.json", "premium_items.json", "gacha_medals.json"}:
        family = normalize_item_family(name)
        if family:
            return family
        return fallback_alpha_group(name)

    if file_name == "gacha_banners.json":
        asset_name = stringify(record.get("BannerAssetName")).strip()
        if asset_name:
            return titleize_identifier(asset_name.split("_", 1)[0])
        return fallback_alpha_group(name)

    if file_name == "parts.json":
        rarity = to_int(record.get("RarityType"))
        if rarity:
            return f"Rarity {rarity}"

    return fallback_alpha_group(name)


def group_sort_key(group: str) -> tuple[int, str]:
    text = stringify(group).strip()
    if not text:
        return (1, "")
    if text.lower() == "unknown":
        return (2, text.lower())
    return (0, text.lower())


def format_unix_millis(value: Any) -> str:
    millis = to_int(value)
    if millis <= 0:
        return ""
    try:
        return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except (OSError, OverflowError, ValueError):
        return ""


def detail_from_record(record: dict[str, Any]) -> str:
    parts: list[str] = []
    if record.get("description"):
        parts.append(str(record["description"]))
    if record.get("character_name") and record.get("character_name") != record.get("name"):
        parts.append(f"character {record['character_name']}")
    if record.get("weapon_name") and record.get("weapon_name") != record.get("name"):
        parts.append(f"weapon {record['weapon_name']}")
    if record.get("reward_name") and record.get("reward_name") != record.get("name"):
        parts.append(f"reward {record['reward_name']}")
    if record.get("season_title"):
        parts.append(f"season {record['season_title']}")
    if record.get("event_quest_chapter_name"):
        parts.append(f"chapter {record['event_quest_chapter_name']}")
    if record.get("StartDatetime") or record.get("EndDatetime"):
        start = format_unix_millis(record.get("StartDatetime"))
        end = format_unix_millis(record.get("EndDatetime"))
        if start or end:
            parts.append(f"{start or 'unknown'} to {end or 'unknown'}")
    return join_detail(*parts)


def parse_addr(value: str) -> tuple[str, int]:
    if ":" not in value:
        raise ValueError("address must be in host:port form")
    host, port_text = value.rsplit(":", 1)
    host = host or "127.0.0.1"
    port = int(port_text)
    return host, port


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Python 3 backend for Mama's Toolbox.")
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help=f"path to the SQLite save DB (default: first existing of {DEFAULT_DB_HELP})",
    )
    parser.add_argument(
        "--addr",
        default="127.0.0.1:8081",
        help="listen address in host:port form (default: 127.0.0.1:8081)",
    )
    parser.add_argument(
        "--extraction-output",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="path to Engels/output (default: ../Engels/output)",
    )
    parser.add_argument(
        "--mom-banner-table",
        type=Path,
        default=DEFAULT_MOM_BANNER_PATH,
        help="path to EntityMMomBannerTable.json (default: ../lunar-tear/server/assets/master_data/EntityMMomBannerTable.json)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = default_db_path() if args.db is None else resolve_cli_path(args.db)
    extraction_output = resolve_cli_path(args.extraction_output)
    mom_banner_path = resolve_cli_path(args.mom_banner_table)
    if not db_path.is_file():
        raise SystemExit(f"database not found: {db_path}")

    host, port = parse_addr(args.addr)
    app = EditorApp(db_path, extraction_output, mom_banner_path)
    EditorRequestHandler.app_state = app
    server = ThreadingHTTPServer((host, port), EditorRequestHandler)
    print(f"Mama's Toolbox listening on http://{host}:{port} using {db_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
