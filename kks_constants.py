"""
kks_constants.py - KKS Voice Studio 共通定数
"""

import re
from pathlib import Path

# ── パス / 履歴 ──────────────────────────────────────────────────────────────

APP_STATE_PATH = Path(__file__).resolve().with_name("kks_voice_studio_state.json")
HISTORY_MAX    = 200
INVALID_FS_CHARS = '<>:"/\\|?*'

# ── キャラクター ──────────────────────────────────────────────────────────────

ALL_CHARS = [f"c{i:02d}" for i in range(44)] + ["c-13", "c-100"]

# ── ファイル名パターン ─────────────────────────────────────────────────────────

# h_{type}_{char}_{level}_{seq}.wav
FILENAME_RE = re.compile(
    r"^h_([a-z0-9]+)_(-?\d+)_(\d{2})_(\d+)\.wav$", re.IGNORECASE)

# ── 型情報 ────────────────────────────────────────────────────────────────────

TYPE_INFO = {
    "ai":   ("喘ぎ",    "aibu"),
    "fe":   ("前戯",    "foreplay"),
    "hh":   ("奉仕",    "houshi"),
    "hh3p": ("奉仕3P",  "houshi_3p"),
    "ka":   ("愛撫",    "aibu_touch"),
    "ka3p": ("愛撫3P",  "aibu_3p"),
    "ko":   ("行為中",  "act_common"),
    "on":   ("オナニー","masturbation"),
    "so":   ("挿入",    "sonyu"),
    "so3p": ("挿入3P",  "sonyu_3p"),
}

LEVEL_NAME = {"00": "控えめ", "01": "通常", "02": "興奮", "03": "絶頂"}

# ── ブラウズ表示列 ────────────────────────────────────────────────────────────

VISIBLE_COLS = {
    "voices":      ["id","chara","mode_name","voice_id","level_name","filename",
                    "file_type","insert_type","houshi_type","aibu_type",
                    "situation_type","wav_path","serif"],
    "breaths":     ["id","chara","mode_name","voice_id","level_name","group_id",
                    "filename","breath_type","wav_path","serif"],
    "shortbreaths":["id","chara","voice_id","level_name","filename",
                    "face","not_overwrite","wav_path","serif"],
}

COMBO_FILTERS  = ["chara","mode_name","level_name","file_type",
                  "insert_type","houshi_type","aibu_type","situation_type","breath_type"]
LIKE_FILTERS   = ["filename","serif","wav_path"]
