#!/usr/bin/env python3
"""Extract Koriyama-stop Tohoku Shinkansen data from JR East timetable pages.

This script fetches the JR East line timetable pages and converts them into the
`HTML/shinkansen-timetable-data.js` structure used by this project.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup


DEFAULT_URLS = {
    "down_weekday": "https://timetables.jreast.co.jp/2603/timetable-v/003d1.html",
    "down_weekend": "https://timetables.jreast.co.jp/2603/timetable-v/003d2.html",
    "up_weekday": "https://timetables.jreast.co.jp/2603/timetable-v/003u1.html",
    "up_weekend": "https://timetables.jreast.co.jp/2603/timetable-v/003u2.html",
}

TYPE_MAP = {
    "やまびこ": "yamabiko",
    "はやて": "hayate",
    "なすの": "nasu",
}

DOWN_ROWS = ("東京 発", "上野 発", "郡山 発")
UP_ROWS = ("郡山 発", "上野 着", "東京 着")
TIME_PATTERN = re.compile(r"^\d{4}$")


@dataclass(frozen=True)
class Train:
    id: str
    type: str
    name: str
    dest: str
    times: tuple[str, str, str]
    origin: str | None = None


def fetch_table(url: str) -> list[list[str]]:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    response.encoding = "utf-8"

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.select_one("table.paper_table")
    if table is None:
        raise ValueError(f"paper_table not found: {url}")

    rows: list[list[str]] = []
    for tr in table.select("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]
        if cells:
            rows.append(cells)
    return rows


def build_row_map(rows: Iterable[list[str]]) -> dict[str, list[str]]:
    row_map: dict[str, list[str]] = {}
    for row in rows:
        if len(row) < 3:
            continue
        row_key = " ".join(part for part in row[:2] if part).strip()
        row_map[row_key] = row[2:]
    return row_map


def normalize_train_name(raw: str) -> tuple[str, str]:
    cleaned = " ".join(raw.split())
    for jp_name, train_type in TYPE_MAP.items():
        if cleaned.startswith(jp_name):
            train_id = cleaned.removeprefix(jp_name).strip()
            return train_id, train_type
    raise ValueError(f"Unsupported train type in row: {raw}")


def normalize_time(raw: str) -> str | None:
    value = raw.strip()
    if not TIME_PATTERN.fullmatch(value):
        return None
    return f"{value[:2]}:{value[2:]}"


def infer_destination(train_type: str, direction: str) -> str:
    if direction == "up":
        return "東京"
    if train_type == "nasu":
        return "郡山"
    if train_type == "hayate":
        return "盛岡"
    return "仙台"


def infer_origin(train_type: str, direction: str) -> str | None:
    if direction == "up" and train_type == "nasu":
        return "koriyama"
    return None


def extract_trains(url: str, direction: str) -> list[Train]:
    row_map = build_row_map(fetch_table(url))

    train_names = row_map["列車名"]
    stations = DOWN_ROWS if direction == "down" else UP_ROWS
    station_rows = [row_map[name] for name in stations]
    trains: list[Train] = []
    seen: set[tuple[str, str, tuple[str, str, str]]] = set()
    for index, raw_name in enumerate(train_names):
        try:
            train_id, train_type = normalize_train_name(raw_name)
        except ValueError:
            continue

        raw_times = [row[index] if index < len(row) else "" for row in station_rows]
        normalized_times = [normalize_time(value) for value in raw_times]
        if any(time is None for time in normalized_times):
            continue
        times = tuple(time for time in normalized_times if time is not None)
        dedupe_key = (train_id, train_type, times)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        trains.append(
            Train(
                id=train_id,
                type=train_type,
                name=next(name for name, mapped in TYPE_MAP.items() if mapped == train_type),
                dest=infer_destination(train_type, direction),
                times=times,
                origin=infer_origin(train_type, direction),
            )
        )

    return trains


def render_train(train: Train) -> str:
    origin = f", origin:'{train.origin}'" if train.origin else ""
    times = ", ".join(f"'{value}'" for value in train.times)
    return (
        f"  {{ id:'{train.id}', type:'{train.type}', name:'{train.name}', "
        f"dest:'{train.dest}', times:[{times}]{origin} }},"
    )


def render_js(data: dict[str, list[Train]]) -> str:
    sections = [
        ("DOWN_WEEKDAY", "down_weekday"),
        ("DOWN_WEEKEND", "down_weekend"),
        ("UP_WEEKDAY", "up_weekday"),
        ("UP_WEEKEND", "up_weekend"),
    ]

    lines = ["// Timetable data (generated from JR East timetable pages)"]
    for const_name, key in sections:
        lines.append(f"const {const_name} = [")
        lines.extend(render_train(train) for train in data[key])
        lines.append("];")
        lines.append("")

    lines.extend(
        [
            "window.SHINKANSEN_TIMETABLE_DATA = {",
            "  DOWN_WEEKDAY,",
            "  DOWN_WEEKEND,",
            "  UP_WEEKDAY,",
            "  UP_WEEKEND",
            "};",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    default_output = repo_root / "HTML" / "shinkansen-timetable-data.js"

    parser = argparse.ArgumentParser(
        description="Extract Koriyama-stop Shinkansen data from JR East timetable pages."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output,
        help=f"Write output JS file to this path (default: {default_output})",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print generated JavaScript to stdout instead of writing a file.",
    )
    parser.add_argument(
        "--down-weekday-url",
        default=DEFAULT_URLS["down_weekday"],
        help="Source URL for down weekday timetable.",
    )
    parser.add_argument(
        "--down-weekend-url",
        default=DEFAULT_URLS["down_weekend"],
        help="Source URL for down weekend timetable.",
    )
    parser.add_argument(
        "--up-weekday-url",
        default=DEFAULT_URLS["up_weekday"],
        help="Source URL for up weekday timetable.",
    )
    parser.add_argument(
        "--up-weekend-url",
        default=DEFAULT_URLS["up_weekend"],
        help="Source URL for up weekend timetable.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data = {
        "down_weekday": extract_trains(args.down_weekday_url, "down"),
        "down_weekend": extract_trains(args.down_weekend_url, "down"),
        "up_weekday": extract_trains(args.up_weekday_url, "up"),
        "up_weekend": extract_trains(args.up_weekend_url, "up"),
    }
    js = render_js(data)

    if args.stdout:
        print(js, end="")
        return

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(js, encoding="utf-8")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
