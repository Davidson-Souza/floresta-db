# SPDX-License-Identifier: MIT OR Apache-2.0

"""Attribute perf page-fault samples to hintsgen database mappings."""

from __future__ import annotations

import argparse
import bisect
import csv
import html
import math
import os
from pathlib import Path
import subprocess
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class Mapping:
    pid: int
    start: int
    end: int
    file_offset: int
    file: str


@dataclass
class FaultCounts:
    minor: int = 0
    major: int = 0
    other: int = 0

    @property
    def total(self) -> int:
        return self.minor + self.major + self.other


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create CSV and SVG heatmaps from an exact perf page-fault recording."
    )
    parser.add_argument("perf_data", type=Path)
    parser.add_argument("mappings_csv", type=Path)
    parser.add_argument("output_directory", type=Path)
    return parser.parse_args()


def read_mappings(path: Path) -> list[Mapping]:
    mappings: list[Mapping] = []
    with path.open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            mappings.append(
                Mapping(
                    pid=int(row["pid"]),
                    start=int(row["start"], 16),
                    end=int(row["end"], 16),
                    file_offset=int(row["file_offset"], 16),
                    file=row["file"],
                )
            )
    mappings.sort(key=lambda mapping: mapping.start)
    return mappings


def read_faults(perf_data: Path, mappings: list[Mapping]) -> dict[tuple[str, int], FaultCounts]:
    command = [
        "perf",
        "script",
        "-i",
        str(perf_data),
        "-F",
        "pid,event,addr",
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    starts = [mapping.start for mapping in mappings]
    pids = {mapping.pid for mapping in mappings}
    page_size = os.sysconf("SC_PAGE_SIZE")
    faults: dict[tuple[str, int], FaultCounts] = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) != 3:
            continue
        try:
            pid = int(fields[0])
            address = int(fields[2], 16)
        except ValueError:
            continue
        if pid not in pids:
            continue
        event = fields[1].removesuffix(":")
        index = bisect.bisect_right(starts, address) - 1
        if index < 0:
            continue
        mapping = mappings[index]
        if mapping.pid != pid or address >= mapping.end:
            continue
        file_offset = mapping.file_offset + address - mapping.start
        page = file_offset // page_size
        counts = faults.setdefault((mapping.file, page), FaultCounts())
        if event == "minor-faults":
            counts.minor += 1
        elif event == "major-faults":
            counts.major += 1
        else:
            counts.other += 1
    return faults


def write_page_csv(path: Path, faults: dict[tuple[str, int], FaultCounts]) -> None:
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(["file", "os_page", "minor_faults", "major_faults", "other_faults", "total_faults"])
        for (file, page), counts in sorted(
            faults.items(), key=lambda item: item[1].total, reverse=True
        ):
            writer.writerow([file, page, counts.minor, counts.major, counts.other, counts.total])


def summarize_files(faults: dict[tuple[str, int], FaultCounts]) -> dict[str, FaultCounts]:
    files: dict[str, FaultCounts] = {}
    for (file, _page), counts in faults.items():
        total = files.setdefault(file, FaultCounts())
        total.minor += counts.minor
        total.major += counts.major
        total.other += counts.other
    return files


def write_file_csv(path: Path, files: dict[str, FaultCounts]) -> None:
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(["file", "minor_faults", "major_faults", "other_faults", "total_faults"])
        for file, counts in sorted(files.items(), key=lambda item: item[1].total, reverse=True):
            writer.writerow([file, counts.minor, counts.major, counts.other, counts.total])


def file_page_counts(
    mappings: list[Mapping], faults: dict[tuple[str, int], FaultCounts]
) -> dict[str, int]:
    page_size = os.sysconf("SC_PAGE_SIZE")
    pages: dict[str, int] = {}
    for mapping in mappings:
        try:
            byte_length = Path(mapping.file).stat().st_size
        except OSError:
            byte_length = 0
        sampled_pages = (
            max(
                (page for (file, page) in faults if file == mapping.file),
                default=-1,
            )
            + 1
        )
        pages[mapping.file] = max(math.ceil(byte_length / page_size), sampled_pages)
    return pages


def heat_color(value: int, maximum: int) -> str:
    if value == 0 or maximum == 0:
        return "#1e293b"
    intensity = math.log1p(value) / math.log1p(maximum)
    red = int(30 + 225 * intensity)
    green = int(64 + 100 * intensity)
    blue = int(175 - 120 * intensity)
    return f"#{red:02x}{green:02x}{blue:02x}"


def write_svg(
    path: Path,
    mappings: list[Mapping],
    faults: dict[tuple[str, int], FaultCounts],
    files: dict[str, FaultCounts],
) -> None:
    columns = 256
    cell = 3
    page_counts = file_page_counts(mappings, faults)
    ordered_files = sorted(page_counts, key=lambda file: files.get(file, FaultCounts()).total, reverse=True)
    panel_heights = [max(1, math.ceil(page_counts[file] / columns)) * cell + 64 for file in ordered_files]
    height = 72 + sum(panel_heights)
    with path.open("w", encoding="utf-8") as output:
        output.write(f'<svg xmlns="http://www.w3.org/2000/svg" width="1180" height="{height}" viewBox="0 0 1180 {height}">\n')
        output.write(f'<rect width="1180" height="{height}" fill="#0f172a"/>\n')
        output.write('<text x="28" y="38" fill="#f8fafc" font-family="monospace" font-size="22">hintsgen database page faults</text>\n')
        y = 64
        for file, panel_height in zip(ordered_files, panel_heights, strict=True):
            file_faults = files.get(file, FaultCounts())
            label = html.escape(file)
            output.write(
                f'<text x="28" y="{y + 16}" fill="#cbd5e1" font-family="monospace" font-size="14">'
                f'{label}: pages={page_counts[file]} faults={file_faults.total} major={file_faults.major}</text>\n'
            )
            maximum = max((counts.total for (name, _), counts in faults.items() if name == file), default=0)
            grid_y = y + 28
            for page in range(page_counts[file]):
                counts = faults.get((file, page), FaultCounts())
                x = 28 + page % columns * cell
                cell_y = grid_y + page // columns * cell
                color = heat_color(counts.total, maximum)
                output.write(
                    f'<rect x="{x}" y="{cell_y}" width="{cell}" height="{cell}" fill="{color}">'
                    f'<title>{label} OS page {page}: total={counts.total} minor={counts.minor} major={counts.major}</title></rect>\n'
                )
            hottest = sorted(
                ((page, counts) for (name, page), counts in faults.items() if name == file),
                key=lambda item: item[1].total,
                reverse=True,
            )[:8]
            for rank, (page, counts) in enumerate(hottest, start=1):
                output.write(
                    f'<text x="820" y="{grid_y + rank * 15}" fill="#94a3b8" font-family="monospace" font-size="12">'
                    f'#{rank} page={page} faults={counts.total} major={counts.major}</text>\n'
                )
            y += panel_height
        output.write("</svg>\n")


def main() -> int:
    args = parse_args()
    mappings = read_mappings(args.mappings_csv)
    if not mappings:
        print("no database mappings found", file=sys.stderr)
        return 1
    args.output_directory.mkdir(parents=True, exist_ok=True)
    faults = read_faults(args.perf_data, mappings)
    files = summarize_files(faults)
    write_page_csv(args.output_directory / "fault-pages.csv", faults)
    write_file_csv(args.output_directory / "fault-files.csv", files)
    write_svg(args.output_directory / "page-faults.svg", mappings, faults, files)
    for file, counts in sorted(files.items(), key=lambda item: item[1].total, reverse=True):
        print(f"file={file} faults={counts.total} minor={counts.minor} major={counts.major}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
