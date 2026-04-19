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
    "down_weekday": "https://timetables.jreast.co.jp/2604/timetable-v/003d1.html",
    "down_weekend": "https://timetables.jreast.co.jp/2604/timetable-v/003d2.html",
    "up_weekday": "https://timetables.jreast.co.jp/2604/timetable-v/003u1.html",
    "up_weekend": "https://timetables.jreast.co.jp/2604/timetable-v/003u2.html",
}

NAVITIME_URLS = {
    "down_weekday": (
        "https://www.navitime.co.jp/diagram/depArrTimeList"
        "?line=00000185&departure=00006668&arrival=00002012&updown=1"
    ),
    "down_weekend": (
        "https://www.navitime.co.jp/diagram/depArrTimeList"
        "?date=2026-04-18&hour=4&departure=00006668&arrival=00002012"
        "&line=00000185&updown=1"
    ),
    "up_weekday": (
        "https://www.navitime.co.jp/diagram/depArrTimeList"
        "?line=00000185&departure=00002012&arrival=00006668&updown=0"
    ),
    "up_weekend": (
        "https://www.navitime.co.jp/diagram/depArrTimeList"
        "?date=2026-04-18&hour=4&departure=00002012&arrival=00006668"
        "&line=00000185&updown=0"
    ),
}

TYPE_MAP = {
    "やまびこ": "yamabiko",
    "はやて": "hayate",
    "なすの": "nasu",
}

NAVITIME_TYPE_MAP = {
    **TYPE_MAP,
    "つばさ": "tsubasa",
}

DOWN_ROWS = ("東京 発", "上野 発", "郡山 発")
UP_ROWS = ("郡山 発", "上野 着", "東京 着")
TIME_PATTERN = re.compile(r"^\d{4}$")
NAVITIME_CLOCK_PATTERN = re.compile(r"^\d{2}:\d{2}$")


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


def normalize_train_name(raw: str, type_map: dict[str, str] = TYPE_MAP) -> tuple[str, str]:
    cleaned = " ".join(raw.split())
    for jp_name, train_type in type_map.items():
        if cleaned.startswith(jp_name):
            train_id = cleaned.removeprefix(jp_name).strip().removesuffix("号")
            return train_id, train_type
    raise ValueError(f"Unsupported train type in row: {raw}")


def normalize_time(raw: str) -> str | None:
    value = raw.strip()
    if not TIME_PATTERN.fullmatch(value):
        return None
    return f"{value[:2]}:{value[2:]}"


def fetch_text_lines(url: str) -> list[str]:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    response.encoding = "utf-8"

    soup = BeautifulSoup(response.text, "html.parser")
    return [text.strip() for text in soup.stripped_strings if text.strip()]


def parse_navitime_arrivals(url: str) -> dict[tuple[str, str, str], str]:
    lines = fetch_text_lines(url)
    arrivals: dict[tuple[str, str, str], str] = {}

    for index, line in enumerate(lines):
        if (
            not NAVITIME_CLOCK_PATTERN.fullmatch(line)
            or index + 4 >= len(lines)
            or lines[index + 1] != "発"
            or not NAVITIME_CLOCK_PATTERN.fullmatch(lines[index + 2])
            or lines[index + 3] != "着"
        ):
            continue

        departure_time = line
        arrival_time = lines[index + 2]
        train_name: tuple[str, str] | None = None
        for candidate in lines[index + 4 : index + 8]:
            try:
                train_name = normalize_train_name(candidate, type_map=NAVITIME_TYPE_MAP)
                break
            except ValueError:
                continue

        if train_name is None:
            continue
        train_id, train_type = train_name

        key = (train_type, train_id, departure_time)
        if key in arrivals and arrivals[key] != arrival_time:
            print(
                "NAVITIME duplicate mismatch for "
                f"{train_type} {train_id} departing {departure_time}: "
                f"{arrivals[key]} vs {arrival_time}"
            )
            continue
        arrivals[key] = arrival_time

    return arrivals


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


def is_all_day_service(service_note: str) -> bool:
    return service_note.strip() == "全日"


def apply_arrival_corrections(
    trains: list[Train],
    navitime_arrivals: dict[tuple[str, str, str], str],
    direction: str,
    schedule_label: str,
) -> list[Train]:
    corrected_trains: list[Train] = []
    matched_keys: set[tuple[str, str, str]] = set()
    destination_label = "Koriyama" if direction == "down" else "Tokyo"
    context = f"[{schedule_label}] "

    for train in trains:
        key = (train.type, train.id, train.times[0])
        corrected_arrival = navitime_arrivals.get(key)
        if corrected_arrival is None:
            fallback_matches = {
                nav_key: arrival_time
                for nav_key, arrival_time in navitime_arrivals.items()
                if nav_key[1] == train.id and nav_key[2] == train.times[0]
            }
            unique_arrivals = sorted(set(fallback_matches.values()))
            if len(unique_arrivals) == 1:
                fallback_key = sorted(fallback_matches)[0]
                corrected_arrival = unique_arrivals[0]
                matched_keys.add(fallback_key)
                print(
                    context
                    + "Using NAVITIME fallback by id and departure for "
                    f"{train.type} {train.id} departing {train.times[0]} "
                    f"via {fallback_key[0]}"
                )
            else:
                type_matches = sorted(
                    departure_time
                    for nav_type, nav_id, departure_time in navitime_arrivals
                    if nav_type == train.type and nav_id == train.id
                )
                print(
                    context
                    + "NAVITIME correction missing for "
                    f"{train.type} {train.id} departing {train.times[0]}"
                )
                if type_matches:
                    print(
                        context
                        + "NAVITIME departure mismatch for "
                        f"{train.type} {train.id}: available departures {', '.join(type_matches)}"
                    )
                elif fallback_matches:
                    fallback_types = ", ".join(
                        sorted(
                            f"{nav_type}@{departure_time}"
                            for nav_type, _, departure_time in fallback_matches
                        )
                    )
                    print(
                        context
                        + "NAVITIME type mismatch candidates for "
                        f"{train.id} departing {train.times[0]}: {fallback_types}"
                    )
                corrected_trains.append(train)
                continue

        matched_keys.add(key)
        if corrected_arrival != train.times[2]:
            print(
                context
                + f"Corrected {destination_label} arrival for "
                f"{train.type} {train.id} departing {train.times[0]}: "
                f"{train.times[2]} -> {corrected_arrival}"
            )

        corrected_trains.append(
            Train(
                id=train.id,
                type=train.type,
                name=train.name,
                dest=train.dest,
                times=(train.times[0], train.times[1], corrected_arrival),
                origin=train.origin,
            )
        )

    extra_keys = sorted(set(navitime_arrivals) - matched_keys)
    for train_type, train_id, departure_time in extra_keys:
        if any(
            matched_id == train_id and matched_departure == departure_time
            for _, matched_id, matched_departure in matched_keys
        ):
            continue
        print(
            context
            + "NAVITIME train not found in JR extraction: "
            f"{train_type} {train_id} departing {departure_time}"
        )

    return corrected_trains


def extract_trains(
    url: str,
    direction: str,
    schedule_label: str,
    navitime_arrivals: dict[tuple[str, str, str], str] | None = None,
) -> list[Train]:
    row_map = build_row_map(fetch_table(url))

    train_names = row_map["列車名"]
    service_days = row_map.get("運転日")
    stations = DOWN_ROWS if direction == "down" else UP_ROWS
    station_rows = [row_map[name] for name in stations]
    trains: list[Train] = []
    seen: set[tuple[str, str, tuple[str, str, str]]] = set()
    for index, raw_name in enumerate(train_names):
        if service_days is not None:
            service_note = service_days[index] if index < len(service_days) else ""
            if not is_all_day_service(service_note):
                continue

        try:
            train_id, train_type = normalize_train_name(raw_name)
        except ValueError:
            continue

        raw_times = [row[index] if index < len(row) else "" for row in station_rows]
        departure_1, departure_2, departure_3 = (
            normalize_time(value) for value in raw_times
        )
        if (
            departure_1 is None
            or departure_2 is None
            or departure_3 is None
        ):
            continue
        times: tuple[str, str, str] = (departure_1, departure_2, departure_3)
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

    if navitime_arrivals is not None:
        return apply_arrival_corrections(
            trains, navitime_arrivals, direction, schedule_label
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
    navitime_arrivals = {
        key: parse_navitime_arrivals(url) for key, url in NAVITIME_URLS.items()
    }
    data = {
        "down_weekday": extract_trains(
            args.down_weekday_url,
            "down",
            "weekday/down",
            navitime_arrivals=navitime_arrivals["down_weekday"],
        ),
        "down_weekend": extract_trains(
            args.down_weekend_url,
            "down",
            "weekend/down",
            navitime_arrivals=navitime_arrivals["down_weekend"],
        ),
        "up_weekday": extract_trains(
            args.up_weekday_url,
            "up",
            "weekday/up",
            navitime_arrivals=navitime_arrivals["up_weekday"],
        ),
        "up_weekend": extract_trains(
            args.up_weekend_url,
            "up",
            "weekend/up",
            navitime_arrivals=navitime_arrivals["up_weekend"],
        ),
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
