#!/usr/bin/env python3
"""Extract Fukushima Kotsu bus timetable data for selected Koriyama routes."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://busget.fukushima-koutu.co.jp"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
    )
}
TIME_PATTERN = re.compile(r"(\d{1,2}:\d{2})")

ROUTES = {
    "taishin": {
        "label": "郡山駅前 ⇄ 台新",
        "stops": {"down": ("郡山駅前", "台新"), "up": ("台新", "郡山駅前")},
        "urls": {
            "down": {
                "weekday": f"{BASE_URL}/fromto/result/1557/1636/?week=1",
                "weekend": f"{BASE_URL}/fromto/result/1557/1636/?week=2",
            },
            "up": {
                "weekday": f"{BASE_URL}/fromto/result/1636/1557/?week=1",
                "weekend": f"{BASE_URL}/fromto/result/1636/1557/?week=2",
            },
        },
    },
    "haribu": {
        "label": "郡山駅前 ⇄ 針生",
        "stops": {"down": ("郡山駅前", "針生"), "up": ("針生", "郡山駅前")},
        "urls": {
            "down": {
                "weekday": f"{BASE_URL}/fromto/result/1557/1670/?week=1",
                "weekend": f"{BASE_URL}/fromto/result/1557/1670/?week=2",
            },
            "up": {
                "weekday": f"{BASE_URL}/fromto/result/1670/1557/?week=1",
                "weekend": f"{BASE_URL}/fromto/result/1670/1557/?week=2",
            },
        },
    },
}

MIDPOINTS = {
    "taishin": {
        "stop_name": "静御前堂",
        "urls": {
            "down": {
                "weekday": f"{BASE_URL}/fromto/result/1557/4104/?week=1",
                "weekend": f"{BASE_URL}/fromto/result/1557/4104/?week=2",
            },
            "up": {
                "weekday": f"{BASE_URL}/fromto/result/4104/1636/?week=1",
                "weekend": f"{BASE_URL}/fromto/result/4104/1636/?week=2",
            },
        },
    }
}


@dataclass(frozen=True)
class BusTrip:
    route_no: str
    destination: str
    platform: str
    departure: str
    arrival: str
    midpoint_time: str | None = None


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    response.encoding = "utf-8"
    return response.text


def normalize_text(node) -> str:
    return " ".join(node.get_text(" ", strip=True).split())


def sort_key_hhmm(value: str) -> tuple[int, int]:
    hour, minute = value.split(":")
    return int(hour), int(minute)


def extract_time(node) -> str | None:
    match = TIME_PATTERN.search(normalize_text(node))
    if match is None:
        return None
    return match.group(1)


def parse_trips(html: str) -> list[BusTrip]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.timeTable")
    if table is None:
        raise ValueError("timeTable not found")

    trips: list[BusTrip] = []
    for row in table.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 6:
            continue

        route_no = normalize_text(cells[0])
        destination = normalize_text(cells[1])
        platform = normalize_text(cells[2])
        departure = extract_time(cells[3])
        arrival = extract_time(cells[5])

        if departure is None or arrival is None:
            continue

        trips.append(
            BusTrip(
                route_no=route_no,
                destination=destination,
                platform=platform,
                departure=departure,
                arrival=arrival,
            )
        )

    return sorted(trips, key=lambda trip: (sort_key_hhmm(trip.departure), sort_key_hhmm(trip.arrival), trip.route_no))


def build_midpoint_map(trips: list[BusTrip]) -> dict[tuple[str, str], str]:
    midpoint_map: dict[tuple[str, str], str] = {}
    for trip in trips:
        midpoint_map[(trip.route_no, trip.departure)] = trip.arrival
    return midpoint_map


def build_upstream_map(trips: list[BusTrip]) -> dict[tuple[str, str], str]:
    upstream_map: dict[tuple[str, str], str] = {}
    for trip in trips:
        upstream_map[(trip.route_no, trip.arrival)] = trip.departure
    return upstream_map


def with_midpoint_times(
    trips: list[BusTrip],
    midpoint_map: dict[tuple[str, str], str],
) -> list[BusTrip]:
    enriched: list[BusTrip] = []
    for trip in trips:
        enriched.append(
            BusTrip(
                route_no=trip.route_no,
                destination=trip.destination,
                platform=trip.platform,
                departure=trip.departure,
                arrival=trip.arrival,
                midpoint_time=midpoint_map.get((trip.route_no, trip.departure)),
            )
        )
    return enriched


def with_upstream_times(
    trips: list[BusTrip],
    upstream_map: dict[tuple[str, str], str],
) -> list[BusTrip]:
    enriched: list[BusTrip] = []
    for trip in trips:
        enriched.append(
            BusTrip(
                route_no=trip.route_no,
                destination=trip.destination,
                platform=trip.platform,
                departure=trip.departure,
                arrival=trip.arrival,
                midpoint_time=upstream_map.get((trip.route_no, trip.departure)),
            )
        )
    return enriched


def build_data() -> dict[str, Any]:
    routes: dict[str, Any] = {}
    for route_key, route_def in ROUTES.items():
        route_entry = {
            "label": route_def["label"],
            "stops": route_def["stops"],
            "midpointStop": MIDPOINTS.get(route_key, {}).get("stop_name"),
            "weekday": {"down": [], "up": []},
            "weekend": {"down": [], "up": []},
        }

        for direction in ("down", "up"):
            for service_day in ("weekday", "weekend"):
                html = fetch_html(route_def["urls"][direction][service_day])
                trips = parse_trips(html)
                midpoint_def = MIDPOINTS.get(route_key)
                if midpoint_def is not None:
                    midpoint_html = fetch_html(midpoint_def["urls"][direction][service_day])
                    midpoint_trips = parse_trips(midpoint_html)
                    if direction == "down":
                        trips = with_midpoint_times(
                            trips, build_midpoint_map(midpoint_trips)
                        )
                    else:
                        trips = with_upstream_times(
                            trips, build_upstream_map(midpoint_trips)
                        )
                route_entry[service_day][direction] = [asdict(trip) for trip in trips]

        routes[route_key] = route_entry

    return {"generatedAt": datetime.now().isoformat(timespec="seconds"), "routes": routes}


def render_js(data: dict[str, object]) -> str:
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    return (
        "// Bus timetable data (generated from Fukushima Kotsu pages)\n"
        f"window.BUS_TIMETABLE_DATA = {payload};\n"
    )


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    default_output = repo_root / "data" / "bus" / "timetable-data.js"

    parser = argparse.ArgumentParser(
        description="Extract selected Fukushima Kotsu bus timetable data."
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data = build_data()
    js = render_js(data)

    if args.stdout:
        print(js, end="")
        return

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(js, encoding="utf-8")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
