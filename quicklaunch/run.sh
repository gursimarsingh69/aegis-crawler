#!/usr/bin/env bash
# =============================================================
#  Digital Asset Protection — Crawler Pipeline Launcher (Linux)
#  Place: quicklaunch/run.sh
#
#  This script can be run from any working directory.
#  It automatically resolves the project root (one level up),
#  activates the virtual environment, and runs the pipeline.
#
#  Standalone mode 2 — Social (Reddit + Twitter)   → suspicious/
#  Standalone mode 3 — Stock  (Unsplash/Pexels/Pixabay) → assets/
# =============================================================

set -e

# ── Resolve paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# ── Activate virtual environment ──────────────────────────────────────────────
_activate_venv() {
  if [[ -f "venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "venv/bin/activate"
    echo "[INFO] Virtual environment activated: venv/"
  elif [[ -f ".venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source ".venv/bin/activate"
    echo "[INFO] Virtual environment activated: .venv/"
  else
    echo "[WARN] No virtual environment found at venv/ or .venv/"
    echo "[WARN] Using system Python: $(which python)"
    echo "[WARN] Run option 5 to install dependencies if needed."
  fi
}

# ── Helpers ───────────────────────────────────────────────────────────────────
pause() {
  read -rp "Press any key to continue..." -n1 -s
  echo
}

log_file() {
  mkdir -p "$PROJECT_ROOT/logs"
  echo "$PROJECT_ROOT/logs/$(date +%Y%m%d_%H%M%S)_${1}.log"
}

# ── Main menu ─────────────────────────────────────────────────────────────────
menu() {
  clear
  echo "==========================================================="
  echo "  Digital Asset Protection - Crawler Pipeline Launcher"
  echo "  Project: $PROJECT_ROOT"
  echo "==========================================================="
  echo
  echo "  1) Run Full Pipeline (Regular mode)"
  echo "  2) Run Standalone Scraper (Social or Stock)"
  echo "  3) Run Connectivity Test"
  echo "  4) Install / Update Dependencies (Setup)"
  echo "  5) Clean up Output Folders (assets & suspicious)"
  echo "  6) Exit"
  echo
  read -rp "Select an option (1-6): " choice
  case "$choice" in
    1) run_full ;;
    2) run_standalone_menu ;;
    3) run_test ;;
    4) setup ;;
    5) cleanup ;;
    6) exit_script ;;
    *) echo "Invalid choice, please try again."; pause; menu ;;
  esac
}

# ── Standalone Menu ───────────────────────────────────────────────────────────
run_standalone_menu() {
  echo
  echo "  Select standalone source:"
  echo "    1) Social (Reddit + Twitter → suspicious/)"
  echo "    2) Stock  (Unsplash / Pexels / Pixabay → assets/)"
  echo "    3) Back to main menu"
  echo
  read -rp "  Selection (1-3): " schoice
  case "$schoice" in
    1) run_standalone_social ;;
    2) run_standalone_stock ;;
    3) menu ;;
    *) echo "Invalid choice."; pause; run_standalone_menu ;;
  esac
}

# ── Full pipeline ─────────────────────────────────────────────────────────────
run_full() {
  echo
  echo "  ┌─────────────────────────────────────────────────────────┐"
  echo "  │  FULL PIPELINE — Crawl → Preprocess → API             │"
  echo "  └─────────────────────────────────────────────────────────┘"
  echo
  read -rp "Select source (social/stock) [default: social]: " src
  read -rp "Enter keywords/topics      (leave blank for .env defaults): " kws
  read -rp "Max images per source      (leave blank for .env default): " lim

  local lf
  lf="$(log_file pipeline)"

  local cmd="python main.py --logfile \"$lf\""
  [[ "$src" == "stock" ]] && cmd+=" --source stock" || cmd+=" --source social"
  [[ -n "$kws" ]] && cmd+=" --keywords \"$kws\""
  [[ -n "$lim" ]] && cmd+=" --limit \"$lim\""

  echo
  echo "[INFO] Running: $cmd"
  echo "[INFO] Log file: $lf"
  eval "$cmd"
  pause
  menu
}

# ── Standalone shared prompt ──────────────────────────────────────────────────
_ask_output_and_limit() {
  read -rp "Enter output folder     (leave blank for default): " out
  read -rp "Max images per source   (leave blank for .env default): " lim
}

# ── Standalone: Social (Reddit + Twitter) ─────────────────────────────────────
run_standalone_social() {
  echo
  echo "  ┌─────────────────────────────────────────────────────────┐"
  echo "  │  SOCIAL SCRAPER — Reddit + Twitter                      │"
  echo "  │  Output: suspicious/                                    │"
  echo "  │  Files:  <sha256>_reddit.jpg / <sha256>_twitter.jpg     │"
  echo "  └─────────────────────────────────────────────────────────┘"
  echo
  echo "  Select scrape mode:"
  echo "    a) home — keyword-based search (default)"
  echo "    b) top  — top subreddits + X accounts from targets.json"
  echo
  read -rp "  Mode (a/b): " smode
  echo

  case "$smode" in
    b|B|top|2)
      _run_social_top
      ;;
    *)
      _run_social_home
      ;;
  esac
}

_run_social_home() {
  echo "[HOME MODE] Keyword-based search across Reddit and Twitter/X."
  echo
  read -rp "Enter keywords          (leave blank for .env defaults): " kws
  _ask_output_and_limit

  local lf
  lf="$(log_file social_home)"

  local cmd="python main.py --standalone --source social --mode home --logfile \"$lf\""
  [[ -n "$kws" ]] && cmd+=" --keywords \"$kws\""
  [[ -n "$out" ]] && cmd+=" --output \"$out\""
  [[ -n "$lim" ]] && cmd+=" --limit \"$lim\""

  echo
  echo "[INFO] Running: $cmd"
  echo "[INFO] Log file: $lf"
  eval "$cmd"
  echo
  echo "[DONE] Social home-mode scrape finished. Images saved to: suspicious/"
  pause
  menu
}

_run_social_top() {
  echo "[TOP MODE] Browse top subreddits and X accounts from targets.json."
  echo
  read -rp "Path to targets.json    (leave blank for ./targets.json): " targets
  _ask_output_and_limit

  local lf
  lf="$(log_file social_top)"

  local cmd="python main.py --standalone --source social --mode top --logfile \"$lf\""
  [[ -n "$targets" ]] && cmd+=" --targets \"$targets\""
  [[ -n "$out" ]]     && cmd+=" --output \"$out\""
  [[ -n "$lim" ]]     && cmd+=" --limit \"$lim\""

  echo
  echo "[INFO] Running: $cmd"
  echo "[INFO] Log file: $lf"
  eval "$cmd"
  echo
  echo "[DONE] Social top-mode scrape finished. Images saved to: suspicious/"
  pause
  menu
}

# ── Standalone: Stock (Unsplash / Pexels / Pixabay) ──────────────────────────
run_standalone_stock() {
  echo
  echo "  ┌─────────────────────────────────────────────────────────┐"
  echo "  │  STOCK SCRAPER — Unsplash / Pexels / Pixabay            │"
  echo "  │  Output: assets/                                        │"
  echo "  │  Files:  <sha256>_unsplash.jpg / _pexels / _pixabay     │"
  echo "  │                                                         │"
  echo "  │  Requires API keys in .env:                             │"
  echo "  │    UNSPLASH_ACCESS_KEY, PEXELS_API_KEY, PIXABAY_API_KEY │"
  echo "  │  Sites without a key are skipped automatically.         │"
  echo "  └─────────────────────────────────────────────────────────┘"
  echo
  read -rp "Enter keywords          (leave blank for .env defaults): " kws
  _ask_output_and_limit

  local lf
  lf="$(log_file stock)"

  local cmd="python main.py --standalone --source stock --logfile \"$lf\""
  [[ -n "$kws" ]] && cmd+=" --keywords \"$kws\""
  [[ -n "$out" ]] && cmd+=" --output \"$out\""
  [[ -n "$lim" ]] && cmd+=" --limit \"$lim\""

  echo
  echo "[INFO] Running: $cmd"
  echo "[INFO] Log file: $lf"
  eval "$cmd"
  echo
  echo "[DONE] Stock scrape finished. Images saved to: assets/"
  pause
  menu
}

# ── Connectivity test ─────────────────────────────────────────────────────────
run_test() {
  echo
  local lf
  lf="$(log_file test)"
  echo "[INFO] Running connectivity tests..."
  echo "[INFO] Log file: $lf"
  python main.py --test --logfile "$lf"
  pause
  menu
}

# ── Setup ─────────────────────────────────────────────────────────────────────
setup() {
  echo
  if [[ ! -d "$PROJECT_ROOT/venv" ]]; then
    echo "[INFO] Creating virtual environment at venv/ ..."
    python -m venv "$PROJECT_ROOT/venv"
    source "$PROJECT_ROOT/venv/bin/activate"
    echo "[INFO] venv created and activated."
  fi
  echo "[INFO] Installing requirements..."
  python -m pip install --upgrade pip -q
  python -m pip install -r requirements.txt
  echo "[INFO] Installing Playwright browsers..."
  python -m playwright install chromium
  echo
  echo "[DONE] Setup complete. You can now run the pipeline."
  pause
  menu
}

cleanup() {
  echo
  echo "  ┌─────────────────────────────────────────────────────────┐"
  echo "  │  CLEANUP — Digital Asset Protection                     │"
  echo "  │  Target: assets/, suspicious/, and logs/                │"
  echo "  └─────────────────────────────────────────────────────────┘"
  echo
  echo "[WARN] This will delete ALL files and subfolders in:"
  echo "       - $PROJECT_ROOT/assets/"
  echo "       - $PROJECT_ROOT/suspicious/"
  echo "       - $PROJECT_ROOT/logs/"
  echo
  read -rp "Are you sure you want to proceed? (y/N): " confirm
  if [[ "$confirm" =~ ^[Yy]$ ]]; then
    echo "[INFO] Cleaning assets/ ..."
    rm -rf "$PROJECT_ROOT/assets"/*
    echo "[INFO] Cleaning suspicious/ ..."
    rm -rf "$PROJECT_ROOT/suspicious"/*
    echo "[INFO] Cleaning logs/ ..."
    rm -rf "$PROJECT_ROOT/logs"/*
    echo "[DONE] Cleanup complete."
  else
    echo "[INFO] Cleanup cancelled."
  fi
  pause
  menu
}

exit_script() {
  echo "Exiting..."
  exit 0
}

# ── Entry point ───────────────────────────────────────────────────────────────
_activate_venv
menu
