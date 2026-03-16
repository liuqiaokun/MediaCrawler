from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean


ROOT = Path(__file__).resolve().parents[1]
PYSENTI_ROOT = ROOT / "third_party" / "pysenti"
if str(PYSENTI_ROOT) not in sys.path:
    sys.path.insert(0, str(PYSENTI_ROOT))

import loguru  # noqa: F401
import pysenti


TEXT_FIELDS = ("title", "content", "desc", "text")
URL_FIELDS = ("note_url", "aweme_url", "video_url", "content_url", "url")
ID_FIELDS = ("note_id", "aweme_id", "video_id", "comment_id", "question_id", "content_id")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a basic sentiment report from MediaCrawler json/jsonl output.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--positive-threshold", type=float, default=0.3)
    parser.add_argument("--negative-threshold", type=float, default=-0.3)
    parser.add_argument("--top-n", type=int, default=10)
    return parser


def load_records(run_dir: Path) -> list[dict]:
    files = sorted(run_dir.rglob("*.jsonl")) + sorted(run_dir.rglob("*.json"))
    records: list[dict] = []

    for file_path in files:
        relative = file_path.relative_to(run_dir)
        if len(relative.parts) < 3:
            continue

        platform = relative.parts[0]
        item_type = "comments" if "comment" in file_path.name else "contents"

        if file_path.suffix == ".jsonl":
            with file_path.open("r", encoding="utf-8", errors="ignore") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    records.append(build_record(platform, item_type, file_path, payload))
        else:
            with file_path.open("r", encoding="utf-8", errors="ignore") as handle:
                try:
                    payload = json.load(handle)
                except json.JSONDecodeError:
                    continue
            if isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict):
                        records.append(build_record(platform, item_type, file_path, item))
            elif isinstance(payload, dict):
                records.append(build_record(platform, item_type, file_path, payload))

    return [record for record in records if record["text"]]


def build_record(platform: str, item_type: str, file_path: Path, payload: dict) -> dict:
    text = ""
    for field in TEXT_FIELDS:
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            text = value.strip()
            break

    url = ""
    for field in URL_FIELDS:
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            url = value.strip()
            break

    item_id = ""
    for field in ID_FIELDS:
        value = payload.get(field)
        if value not in (None, ""):
            item_id = str(value)
            break

    source_keyword = str(payload.get("source_keyword", "") or "")
    return {
        "platform": platform,
        "item_type": item_type,
        "file_name": file_path.name,
        "id": item_id,
        "url": url,
        "source_keyword": source_keyword,
        "text": text,
    }


def classify_label(score: float, positive_threshold: float, negative_threshold: float) -> str:
    if score >= positive_threshold:
        return "positive"
    if score <= negative_threshold:
        return "negative"
    return "neutral"


def analyze_records(records: list[dict], positive_threshold: float, negative_threshold: float) -> list[dict]:
    analyzed: list[dict] = []
    for record in records:
        result = pysenti.classify(record["text"])
        score = float(result.get("score", 0.0))
        analyzed.append(
            {
                **record,
                "sentiment_score": round(score, 4),
                "sentiment_label": classify_label(score, positive_threshold, negative_threshold),
            }
        )
    return analyzed


def summarize(records: list[dict]) -> dict:
    by_platform: dict[str, dict] = {}
    totals = {
        "records": len(records),
        "contents": sum(1 for item in records if item["item_type"] == "contents"),
        "comments": sum(1 for item in records if item["item_type"] == "comments"),
        "avg_score": round(mean(item["sentiment_score"] for item in records), 4) if records else 0.0,
        "labels": Counter(item["sentiment_label"] for item in records),
    }

    grouped: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        grouped[record["platform"]].append(record)

    for platform, items in grouped.items():
        by_platform[platform] = {
            "records": len(items),
            "contents": sum(1 for item in items if item["item_type"] == "contents"),
            "comments": sum(1 for item in items if item["item_type"] == "comments"),
            "avg_score": round(mean(item["sentiment_score"] for item in items), 4),
            "labels": Counter(item["sentiment_label"] for item in items),
            "source_keywords": Counter(
                item["source_keyword"] for item in items if item["source_keyword"]
            ).most_common(10),
        }

    return {"totals": totals, "platforms": by_platform}


def write_csv(output_dir: Path, file_name: str, records: list[dict]) -> None:
    if not records:
        return

    fields = [
        "platform",
        "item_type",
        "id",
        "sentiment_label",
        "sentiment_score",
        "source_keyword",
        "url",
        "text",
        "file_name",
    ]
    with (output_dir / file_name).open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for record in records:
            writer.writerow(record)


def render_table(records: list[dict], top_n: int) -> list[str]:
    lines: list[str] = []
    for item in records[:top_n]:
        text = item["text"].replace("\n", " ").strip()
        if len(text) > 120:
            text = f"{text[:117]}..."
        lines.append(
            f"- [{item['platform']}/{item['item_type']}] {item['sentiment_label']} "
            f"({item['sentiment_score']}) {text}"
        )
    return lines or ["- None"]


def write_markdown(output_dir: Path, summary: dict, records: list[dict], top_n: int) -> None:
    totals = summary["totals"]
    platforms = summary["platforms"]

    top_negative = sorted(records, key=lambda item: item["sentiment_score"])[:top_n]
    top_positive = sorted(records, key=lambda item: item["sentiment_score"], reverse=True)[:top_n]

    lines = [
        "# Keyword Sentiment Report",
        "",
        "## Overview",
        "",
        f"- Records: {totals['records']}",
        f"- Contents: {totals['contents']}",
        f"- Comments: {totals['comments']}",
        f"- Average score: {totals['avg_score']}",
        f"- Label counts: {dict(totals['labels'])}",
        "",
        "## By Platform",
        "",
    ]

    if not platforms:
        lines.append("- No platform data found.")
    else:
        for platform, details in sorted(platforms.items()):
            lines.extend(
                [
                    f"### {platform}",
                    "",
                    f"- Records: {details['records']}",
                    f"- Contents: {details['contents']}",
                    f"- Comments: {details['comments']}",
                    f"- Average score: {details['avg_score']}",
                    f"- Label counts: {dict(details['labels'])}",
                    f"- Top source keywords: {details['source_keywords']}",
                    "",
                ]
            )

    lines.extend(
        [
            "## Top Negative Signals",
            "",
            *render_table(top_negative, top_n),
            "",
            "## Top Positive Signals",
            "",
            *render_table(top_positive, top_n),
            "",
        ]
    )

    (output_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    run_dir = Path(args.run_dir).resolve()
    if not run_dir.exists():
        print(f"Run directory does not exist: {run_dir}", file=sys.stderr)
        return 1

    output_dir = Path(args.output_dir).resolve() if args.output_dir else ROOT / "workspace" / "reports" / run_dir.name
    output_dir.mkdir(parents=True, exist_ok=True)

    records = load_records(run_dir)
    analyzed_records = analyze_records(records, args.positive_threshold, args.negative_threshold)
    summary = summarize(analyzed_records)

    write_csv(output_dir, "sentiment_records.csv", analyzed_records)
    write_markdown(output_dir, summary, analyzed_records, args.top_n)
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=lambda obj: dict(obj)),
        encoding="utf-8",
    )

    print(f"[generate_sentiment_report] Report written to: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
