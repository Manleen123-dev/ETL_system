from __future__ import annotations

import csv
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def _load_font(size: int, bold: bool = False):
    candidates = ["arialbd.ttf", "DejaVuSans-Bold.ttf"] if bold else ["arial.ttf", "DejaVuSans.ttf"]
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size)
        except OSError:
            continue
    return ImageFont.load_default()


def _text_center(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], text: str, font, fill: str) -> None:
    left, top, right, bottom = box
    draw.text(((left + right) / 2, (top + bottom) / 2), text, fill=fill, font=font, anchor="mm")


def read_results(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def render_results_table(csv_path: Path, output_path: Path) -> None:
    rows = read_results(csv_path)
    headers = ["SrNo", "#Records", "DB Size (MB)", "CASE1", "CASE2", "1 MB", "2 MB", "5 MB", "6 MB"]
    widths = [110, 120, 140, 110, 110, 110, 110, 110, 110]
    row_height = 44
    margin = 16
    table_width = sum(widths)
    image_width = table_width + margin * 2
    image_height = 260 + row_height * len(rows)

    image = Image.new("RGB", (image_width, image_height), "#EEF3FB")
    draw = ImageDraw.Draw(image)
    title_font = _load_font(24, bold=True)
    header_font = _load_font(17, bold=True)
    cell_font = _load_font(16)
    cell_font_bold = _load_font(16, bold=True)
    note_font = _load_font(14)

    draw.text((image_width / 2, 34), "ETL Benchmark Results", fill="#12233D", font=title_font, anchor="mm")

    top = 78
    left = margin
    x = left
    for width, header in zip(widths, headers):
        box = (x, top, x + width, top + row_height)
        draw.rectangle(box, fill="#BFD6F5", outline="#C9D5E6", width=1)
        _text_center(draw, box, header, header_font, "#1F2937")
        x += width

    for index, row in enumerate(rows, start=1):
        values = [
            str(index),
            row["DATASET"],
            f'{float(row["SOURCE_DB_MB"]):.4f}',
            row["CASE1_DIRECT_SEC"],
            row["CASE2_STAGED_SEC"],
            row["1_MB_SEC"],
            row["2_MB_SEC"],
            row["5_MB_SEC"],
            row["6_MB_SEC"],
        ]
        y = top + row_height * index
        x = left
        for column_index, (width, value) in enumerate(zip(widths, values)):
            box = (x, y, x + width, y + row_height)
            draw.rectangle(box, fill="#FFFFFF", outline="#C9D5E6", width=1)
            font = cell_font_bold if column_index >= 3 else cell_font
            _text_center(draw, box, value, font, "#334155")
            x += width

    draw.text(
        (image_width / 2, image_height - 28),
        "Case 3 columns use shared chunk sizes across all source databases.",
        fill="#6B7A90",
        font=note_font,
        anchor="mm",
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(output_path)


def render_benchmark_plot(csv_path: Path, output_path: Path) -> None:
    rows = read_results(csv_path)
    dataset_labels = [row["DATASET"] for row in rows]
    case1 = [float(row["CASE1_DIRECT_SEC"]) for row in rows]
    case2 = [float(row["CASE2_STAGED_SEC"]) for row in rows]
    case3 = [float(row["CASE3_OPTIMAL_SEC"]) for row in rows]

    width = 1320
    height = 760
    left = 90
    top = 90
    right = 90
    bottom = 80
    plot_width = width - left - right
    plot_height = height - top - bottom
    max_seconds = max(case1 + case2 + case3)

    image = Image.new("RGB", (width, height), "#FFFFFF")
    draw = ImageDraw.Draw(image)
    title_font = _load_font(30, bold=True)
    legend_font = _load_font(16)
    label_font = _load_font(15)
    axis_font = _load_font(18, bold=True)

    def x_position(seconds: float) -> float:
        usable_width = plot_width - 80
        return plot_left + 40 + (seconds / max_seconds) * usable_width

    def y_position(index: int) -> float:
        if len(rows) == 1:
            return top + plot_height / 2
        return plot_bottom - 24 - (plot_height - 48) * (index / (len(rows) - 1))

    draw.text((width / 2, 30), "ETL Benchmark Comparison", fill="#17233A", font=title_font, anchor="mm")

    plot_left = left + 20
    plot_top = top
    plot_right = left + plot_width
    plot_bottom = top + plot_height
    draw.rectangle((plot_left, plot_top, plot_right, plot_bottom), outline="#606770", width=2)

    grid_ticks = 6
    for tick in range(grid_ticks):
        x = plot_left + (plot_right - plot_left) * tick / (grid_ticks - 1)
        draw.line((x, plot_top, x, plot_bottom), fill="#E2E8F0", width=1)
        tick_value = max_seconds * tick / (grid_ticks - 1)
        draw.text((x, plot_bottom + 24), f"{tick_value:.1f}", fill="#374151", font=label_font, anchor="mm")

    for index, label in enumerate(dataset_labels):
        y = y_position(index)
        draw.line((plot_left, y, plot_right, y), fill="#E5E7EB", width=1)
        draw.text((plot_left - 14, y), label, fill="#374151", font=label_font, anchor="rm")

    legend_items = [
        ("Case 1", "#2F66E0", "circle"),
        ("Case 2", "#FF6A00", "square"),
        ("Case 3 (Optimal)", "#11A37F", "triangle"),
    ]
    legend_x = plot_left + 16
    legend_y = plot_top + 20
    for label, color, marker in legend_items:
        _draw_marker(draw, legend_x + 14, legend_y + 10, color, marker)
        draw.line((legend_x, legend_y + 10, legend_x + 28, legend_y + 10), fill=color, width=3)
        draw.text((legend_x + 42, legend_y + 10), label, fill="#1F2937", font=legend_font, anchor="lm")
        legend_y += 34

    _draw_series(draw, case1, "#2F66E0", "circle", x_position, y_position)
    _draw_series(draw, case2, "#FF6A00", "square", x_position, y_position)
    _draw_series(draw, case3, "#11A37F", "triangle", x_position, y_position)

    draw.text((24, top + plot_height / 2), "#Records", fill="#1F2937", font=axis_font, anchor="mm")
    draw.text((width / 2, height - 28), "Time (seconds)", fill="#1F2937", font=axis_font, anchor="mm")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(output_path)


def _draw_series(draw: ImageDraw.ImageDraw, values: list[float], color: str, marker: str, x_position, y_position) -> None:
    points = [(x_position(value), y_position(index)) for index, value in enumerate(values)]
    draw.line(points, fill=color, width=4)
    for x, y in points:
        _draw_marker(draw, x, y, color, marker)


def _draw_marker(draw: ImageDraw.ImageDraw, x: float, y: float, color: str, marker: str) -> None:
    if marker == "circle":
        draw.ellipse((x - 5, y - 5, x + 5, y + 5), fill=color, outline=color)
        return
    if marker == "square":
        draw.rectangle((x - 5, y - 5, x + 5, y + 5), fill=color, outline=color)
        return
    draw.polygon([(x, y - 6), (x - 6, y + 5), (x + 6, y + 5)], fill=color, outline=color)
