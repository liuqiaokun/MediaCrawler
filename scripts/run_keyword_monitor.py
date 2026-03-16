from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
if sys.platform == "win32":
    PYTHON_EXE = ROOT / ".venv" / "Scripts" / "python.exe"
else:
    PYTHON_EXE = ROOT / ".venv" / "bin" / "python"
MEDIA_MAIN = ROOT / "main.py"
RUNS_ROOT = ROOT / "workspace" / "runs"
REPORT_SCRIPT = ROOT / "scripts" / "generate_sentiment_report.py"

SUPPORTED_PLATFORMS = {"xhs", "dy", "ks", "bili", "wb", "tieba", "zhihu"}
SUPPORTED_LOGIN_TYPES = {"qrcode", "phone", "cookie"}
SUPPORTED_SAVE_OPTIONS = {"json", "jsonl", "sqlite", "excel"}
SUPPORTED_PROXY_PROVIDERS = {"kuaidaili", "wandouhttp"}


def load_project_env() -> None:
    for env_name in (".env", ".env.local"):
        env_path = ROOT / env_name
        if env_path.exists():
            load_dotenv(env_path, override=False)


def str_to_bool(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in {"1", "true", "t", "yes", "y", "on"}


def ensure_proxy_env(provider: str) -> None:
    if provider == "wandouhttp":
        key = os.getenv("WANDOU_APP_KEY") or os.getenv("wandou_app_key")
        if not key:
            raise SystemExit("Missing WANDOU_APP_KEY in .env for wandouhttp proxy mode.")
        return

    if provider == "kuaidaili":
        required = {
            "KDL_SECERT_ID": os.getenv("KDL_SECERT_ID") or os.getenv("kdl_secret_id"),
            "KDL_SIGNATURE": os.getenv("KDL_SIGNATURE") or os.getenv("kdl_signature"),
            "KDL_USER_NAME": os.getenv("KDL_USER_NAME") or os.getenv("kdl_user_name"),
            "KDL_USER_PWD": os.getenv("KDL_USER_PWD") or os.getenv("kdl_user_pwd"),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise SystemExit(f"Missing proxy credentials in .env: {', '.join(missing)}")
        return

    raise SystemExit(f"Unsupported proxy provider: {provider}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run MediaCrawler keyword monitoring and optionally generate a sentiment report."
    )
    parser.add_argument("--platform", choices=sorted(SUPPORTED_PLATFORMS), required=True)
    parser.add_argument("--keywords", required=True, help="Comma-separated keywords for MediaCrawler.")
    parser.add_argument("--login-type", choices=sorted(SUPPORTED_LOGIN_TYPES), default="qrcode")
    parser.add_argument("--crawler-type", choices=["search", "detail", "creator"], default="search")
    parser.add_argument("--save-data-option", choices=sorted(SUPPORTED_SAVE_OPTIONS), default="jsonl")
    parser.add_argument("--headless", default="false", help="true/false")
    parser.add_argument("--get-comment", default="true", help="true/false")
    parser.add_argument("--get-sub-comment", default="false", help="true/false")
    parser.add_argument("--max-notes-count", "--max_notes_count", type=int, default=50)
    parser.add_argument("--max-comments-count-singlenotes", type=int, default=20)
    parser.add_argument("--max-concurrency-num", type=int, default=1)
    parser.add_argument("--cookies", default="", help="Cookie string when login-type=cookie.")
    parser.add_argument("--enable-ip-proxy", action="store_true")
    parser.add_argument(
        "--ip-proxy-provider-name",
        choices=sorted(SUPPORTED_PROXY_PROVIDERS),
        default="kuaidaili",
    )
    parser.add_argument("--run-name", default="", help="Optional custom run directory name.")
    parser.add_argument("--save-root", default=str(RUNS_ROOT))
    parser.add_argument("--skip-report", action="store_true")
    return parser


def make_run_dir(save_root: Path, platform: str, run_name: str) -> Path:
    if run_name:
        safe_name = run_name.strip().replace(" ", "_")
    else:
        safe_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{platform}"
    run_dir = save_root / safe_name
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def maybe_init_sqlite(save_data_option: str) -> None:
    if save_data_option != "sqlite":
        return

    cmd = [str(PYTHON_EXE), str(MEDIA_MAIN), "--init_db", "sqlite"]
    subprocess.run(cmd, cwd=ROOT, check=True)


def run_crawler(args: argparse.Namespace, run_dir: Path) -> int:
    cmd = [
        str(PYTHON_EXE),
        str(MEDIA_MAIN),
        "--platform",
        args.platform,
        "--lt",
        args.login_type,
        "--type",
        args.crawler_type,
        "--keywords",
        args.keywords,
        "--headless",
        args.headless,
        "--get_comment",
        args.get_comment,
        "--get_sub_comment",
        args.get_sub_comment,
        "--max_notes_count",
        str(args.max_notes_count),
        "--save_data_option",
        args.save_data_option,
        "--save_data_path",
        str(run_dir),
        "--max_comments_count_singlenotes",
        str(args.max_comments_count_singlenotes),
        "--max_concurrency_num",
        str(args.max_concurrency_num),
        "--enable_ip_proxy",
        "true" if args.enable_ip_proxy else "false",
        "--ip_proxy_provider_name",
        args.ip_proxy_provider_name,
    ]

    if args.cookies:
        cmd.extend(["--cookies", args.cookies])

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["MEDIACRAWLER_RUN_TAG"] = run_dir.name

    print(f"[run_keyword_monitor] Run directory: {run_dir}")
    print("[run_keyword_monitor] Command:")
    print(" ".join(f'"{part}"' if " " in part else part for part in cmd))
    return subprocess.run(cmd, cwd=ROOT, env=env).returncode


def maybe_generate_report(args: argparse.Namespace, run_dir: Path) -> int:
    if args.skip_report:
        print("[run_keyword_monitor] Skipping sentiment report generation.")
        return 0

    if args.save_data_option not in {"json", "jsonl"}:
        print(
            "[run_keyword_monitor] Report generation currently supports json/jsonl runs only. "
            "Skip report or switch to --save-data-option jsonl."
        )
        return 0

    cmd = [str(PYTHON_EXE), str(REPORT_SCRIPT), "--run-dir", str(run_dir)]
    return subprocess.run(cmd, cwd=ROOT, env=os.environ.copy()).returncode


def main() -> int:
    if not PYTHON_EXE.exists():
        print(f"Python environment not found: {PYTHON_EXE}", file=sys.stderr)
        return 1

    load_project_env()
    parser = build_parser()
    args = parser.parse_args()

    save_root = Path(args.save_root).resolve()
    run_dir = make_run_dir(save_root, args.platform, args.run_name)

    if args.enable_ip_proxy:
        ensure_proxy_env(args.ip_proxy_provider_name)

    maybe_init_sqlite(args.save_data_option)
    crawl_code = run_crawler(args, run_dir)
    if crawl_code != 0:
        return crawl_code

    report_code = maybe_generate_report(args, run_dir)
    if report_code == 0:
        print(f"[run_keyword_monitor] Finished. Output saved under: {run_dir}")
    return report_code


if __name__ == "__main__":
    raise SystemExit(main())
