"""
build_tab.py - 抽出済み WAV から SQLite DB を構築する BuildDbTab
"""

import queue
import sqlite3
import threading
import tkinter as tk
from collections import defaultdict
from pathlib import Path
from tkinter import filedialog, messagebox

try:
    import UnityPy
    UNITYPY_OK = True
except ImportError:
    UNITYPY_OK = False

from .kks_constants import TYPE_INFO, LEVEL_NAME, INVALID_FS_CHARS, FILENAME_RE

# ── DDL ───────────────────────────────────────────────────────────────────────

DB_DDL = """
CREATE TABLE IF NOT EXISTS voices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chara TEXT, mode_name TEXT, voice_id INTEGER,
    level INTEGER, level_name TEXT,
    filename TEXT, file_type TEXT,
    insert_type TEXT, houshi_type TEXT,
    aibu_type TEXT, situation_type TEXT,
    wav_path TEXT, serif TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS breaths (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chara TEXT, mode_name TEXT, voice_id INTEGER,
    level INTEGER, level_name TEXT,
    group_id TEXT DEFAULT '', filename TEXT,
    breath_type TEXT DEFAULT '', wav_path TEXT, serif TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS shortbreaths (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chara TEXT, voice_id INTEGER,
    level INTEGER, level_name TEXT,
    filename TEXT, face INTEGER DEFAULT -1,
    not_overwrite INTEGER DEFAULT 0,
    wav_path TEXT, serif TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_voices_chara     ON voices(chara);
CREATE INDEX IF NOT EXISTS idx_voices_mode      ON voices(mode_name);
CREATE INDEX IF NOT EXISTS idx_voices_level     ON voices(level);
CREATE INDEX IF NOT EXISTS idx_voices_file_type ON voices(file_type);
"""

# ── VoicePatternData 型マップ用定数 ───────────────────────────────────────────

_TEKOKI_CONDS  = {1, 2}
_PAIZURI_CONDS = {9, 10, 11, 12, 13, 14, 15, 16, 17}
_WP_IS_CONDS   = {1, 3, 5, 7, 9, 11}
_WP_NOT_CONDS  = {2, 4, 6, 8, 10}

_START_TAGS = {
    0: "not_insert_ok",   1: "dangerous_day",    2: "safe_day",
    3: "girlfriend_virgin", 4: "aibu_pos0",       5: "aibu_pos1",
    6: "houshi_fera",     7: "houshi_tekoki",     8: "houshi_paizuri",
    9: "sonyu_female",   10: "sonyu_pos0",       11: "sonyu_pos1",
    12: "sonyu_pos2",    13: "insert_ok",        14: "aibu_pos0",
    15: "aibu_pos1",     16: "sonyu_female",     17: "role_main",
    18: "role_sub",      19: "sonyu_normal",
}
_MAST_TAGS = {0: "found"}
_LES_TAGS  = {0: "role_main", 1: "role_sub"}
_TAG_ORDER = [
    "not_insert_ok", "insert_ok", "dangerous_day", "safe_day", "girlfriend_virgin",
    "aibu_pos0", "aibu_pos1", "houshi_fera", "houshi_tekoki", "houshi_paizuri",
    "sonyu_female", "sonyu_normal", "sonyu_pos0", "sonyu_pos1", "sonyu_pos2",
    "role_main", "role_sub", "found",
]

# ── ヘルパー関数 ──────────────────────────────────────────────────────────────

def sanitize(value: str, max_len: int = 120) -> str:
    if not value:
        return "_"
    for c in INVALID_FS_CHARS:
        value = value.replace(c, "_")
    return value.strip()[:max_len] or "_"


def parse_voice_filename(filename: str):
    m = FILENAME_RE.match(filename)
    if not m:
        return None
    type_code, char_num, level_code, seq = m.groups()
    tc = type_code.lower()
    info = TYPE_INFO.get(tc)
    if not info:
        return None
    cn = int(char_num)
    chara = f"c{cn:02d}" if cn >= 0 else f"c{cn}"
    ft = info[1]
    return {
        "chara":          chara,
        "mode_name":      ft,
        "voice_id":       int(seq),
        "level":          int(level_code),
        "level_name":     LEVEL_NAME.get(level_code, f"level_{level_code}"),
        "filename":       Path(filename).stem,
        "file_type":      ft,
        "type_code":      tc,
        "insert_type":    "",
        "houshi_type":    "",
        "aibu_type":      "",
        "situation_type": "",
    }


def _load_pattern_trees(kks_dir: str, log_fn) -> dict:
    h_list = Path(kks_dir) / "abdata" / "h" / "list"
    if not h_list.is_dir():
        log_fn(f"[WARN] {h_list} が見つかりません\n")
        return {}
    suffixes = {
        0: "voice_00_00", 1: "voice_01_00", 2: "voice_02_00",
        3: "voice_03_00", 4: "voice_04_00", 6: "voice_06_00",
    }
    result = {m: [] for m in suffixes}
    for bp in sorted(h_list.glob("*.unity3d")):
        try:
            env = UnityPy.load(str(bp))
            for key in env.container:
                for mode, suf in suffixes.items():
                    if suf in key:
                        try:
                            result[mode].append(env.container[key].read_typetree())
                        except Exception:
                            pass
        except Exception as e:
            log_fn(f"[WARN] {bp.name}: {e}\n")
    return result


def _iter_lst_info(trees):
    for tree in trees:
        for param in tree.get("param", []):
            for li in param.get("lstInfo", []):
                yield param, li


def _build_insert_map(trees: list) -> dict:
    tags = defaultdict(set)
    for _, li in _iter_lst_info(trees):
        conds  = set(li.get("lstConditions", []))
        voices = li.get("lstVoice", []) + li.get("lstSecondVoice", [])
        for vid in voices:
            if 0 in conds: tags[vid].add("kokan")
            if 1 in conds: tags[vid].add("anal")
    result = {}
    for vid, t in tags.items():
        if "kokan" in t and "anal" in t: result[vid] = "both"
        elif "kokan" in t: result[vid] = "kokan"
        elif "anal"  in t: result[vid] = "anal"
    return result


def _build_houshi_map(trees: list) -> dict:
    tags = defaultdict(set)
    for _, li in _iter_lst_info(trees):
        conds  = set(li.get("lstConditions", []))
        voices = li.get("lstVoice", []) + li.get("lstSecondVoice", [])
        for vid in voices:
            if 0 in conds:                  tags[vid].add("fera")
            if conds & _TEKOKI_CONDS:       tags[vid].add("tekoki")
            if conds & _PAIZURI_CONDS:      tags[vid].add("paizuri")
    return {vid: "/".join(sorted(t)) for vid, t in tags.items() if t}


def _build_aibu_map(trees: list) -> dict:
    vid_tags  = defaultdict(set)
    kiss_vids = set()
    for param, li in _iter_lst_info(trees):
        pid    = param.get("id", -1)
        conds  = set(li.get("lstConditions", []))
        voices = li.get("lstVoice", []) + li.get("lstSecondVoice", [])
        for vid in voices:
            if pid == 41:                       kiss_vids.add(vid)
            if conds & _WP_IS_CONDS:            vid_tags[vid].add("weakpoint")
            if 1 in conds:                      vid_tags[vid].add("no_weakpoint")
            if 0 in conds:                      vid_tags[vid].add("has_weakpoint")
            if conds & _WP_NOT_CONDS and not (conds & _WP_IS_CONDS):
                                                vid_tags[vid].add("nonspot")
    order = ["kiss", "weakpoint", "has_weakpoint", "no_weakpoint", "nonspot"]
    result = {}
    for vid in set(list(vid_tags.keys()) + list(kiss_vids)):
        t = {"kiss"} if vid in kiss_vids else vid_tags.get(vid, set())
        if t:
            result[vid] = "/".join(sorted(t, key=lambda x: order.index(x) if x in order else 99))
    return result


def _build_situation_map(trees: list, tag_dict: dict) -> dict:
    vid_tags = defaultdict(set)
    for _, li in _iter_lst_info(trees):
        conds  = set(li.get("lstConditions", []))
        voices = li.get("lstVoice", []) + li.get("lstSecondVoice", [])
        for c, tag in tag_dict.items():
            if c in conds:
                for vid in voices:
                    vid_tags[vid].add(tag)
    result = {}
    for vid, tags in vid_tags.items():
        result[vid] = "/".join(sorted(tags, key=lambda t: _TAG_ORDER.index(t) if t in _TAG_ORDER else 99))
    return result


# ── BuildDbTab ────────────────────────────────────────────────────────────────

class BuildDbTab(tk.Frame):
    def __init__(self, parent, on_build_done=None, get_kks_dir=None):
        super().__init__(parent)
        self._log_queue   = queue.Queue()
        self._running     = False
        self._on_done     = on_build_done
        self._get_kks_dir = get_kks_dir or (lambda: "")
        self._last_db     = None
        self._build_ui()

    def _build_ui(self):
        pad = {"padx": 6, "pady": 3}

        # WAV source dir
        fr = tk.Frame(self)
        fr.pack(fill="x", **pad)
        tk.Label(fr, text="WAVフォルダ:", width=18, anchor="w").pack(side="left")
        self._wav_var = tk.StringVar(value="")
        tk.Entry(fr, textvariable=self._wav_var).pack(side="left", fill="x", expand=True)
        tk.Button(fr, text="参照", command=self._browse_wav).pack(side="left", padx=2)

        # DB output
        fr2 = tk.Frame(self)
        fr2.pack(fill="x", **pad)
        tk.Label(fr2, text="DB出力先:", width=18, anchor="w").pack(side="left")
        self._db_var = tk.StringVar(value="")
        tk.Entry(fr2, textvariable=self._db_var).pack(side="left", fill="x", expand=True)
        tk.Button(fr2, text="参照", command=self._browse_db).pack(side="left", padx=2)

        # Button
        ctrl = tk.Frame(self)
        ctrl.pack(fill="x", **pad)
        self._build_btn = tk.Button(ctrl, text="▶ DB構築", command=self._start,
                                    bg="#2196F3", fg="white", width=16)
        self._build_btn.pack(side="left", padx=2)
        self._status_var = tk.StringVar(value="待機中")
        tk.Label(ctrl, textvariable=self._status_var).pack(side="left", padx=8)

        # Log
        self._log = tk.Text(self, height=22, state=tk.DISABLED,
                            font=("Consolas", 9), wrap="word")
        sb = tk.Scrollbar(self, command=self._log.yview)
        self._log.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._log.pack(fill="both", expand=True, padx=6, pady=3)

    def _browse_wav(self):
        d = filedialog.askdirectory(title="WAVフォルダを選択")
        if d:
            self._wav_var.set(d)

    def _browse_db(self):
        p = filedialog.asksaveasfilename(
            title="DB出力先を選択",
            defaultextension=".db",
            filetypes=[("SQLite DB", "*.db"), ("All", "*.*")])
        if p:
            self._db_var.set(p)

    def get_settings(self):
        return {
            "wav_dir": self._wav_var.get(),
            "db_path": self._db_var.get(),
        }

    def apply_settings(self, d):
        if not d:
            return
        if d.get("wav_dir"):
            self._wav_var.set(d["wav_dir"])
        if d.get("db_path"):
            self._db_var.set(d["db_path"])

    def _append_log(self, text: str):
        self._log.config(state=tk.NORMAL)
        self._log.insert("end", text)
        self._log.see("end")
        self._log.config(state=tk.DISABLED)

    def _drain(self):
        try:
            while True:
                item = self._log_queue.get_nowait()
                if item == "__done__":
                    self._running = False
                    self._build_btn.config(state=tk.NORMAL)
                    self._status_var.set("完了")
                    if self._on_done and self._last_db:
                        self._on_done(self._last_db)
                    return
                self._append_log(item)
        except queue.Empty:
            pass
        if self._running:
            self.after(100, self._drain)

    def _start(self):
        wav = self._wav_var.get().strip()
        db  = self._db_var.get().strip()
        kks = self._get_kks_dir().strip()
        if not wav:
            messagebox.showerror("エラー", "WAVフォルダを指定してください。")
            return
        if not db:
            messagebox.showerror("エラー", "DB出力先を指定してください。")
            return
        self._running = True
        self._build_btn.config(state=tk.DISABLED)
        self._status_var.set("構築中...")
        threading.Thread(target=self._worker,
                         args=(wav, db, kks),
                         daemon=True).start()
        self.after(100, self._drain)

    def _worker(self, wav_dir: str, db_path: str, kks_dir: str):
        try:
            p = Path(db_path)
            if p.is_dir() or not p.suffix:
                p = p / "kks_voices.db"
                db_path = str(p)
            self._last_db = db_path
            self._log_queue.put(f"[DB] 出力先: {db_path}\n")
            p.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(db_path)
            conn.executescript(DB_DDL)
            conn.execute("DELETE FROM voices")
            conn.execute("DELETE FROM breaths")
            conn.execute("DELETE FROM shortbreaths")
            conn.commit()

            # ── VoicePatternData から型マップを構築 ──
            type_maps = {}
            if kks_dir and UNITYPY_OK and (Path(kks_dir) / "abdata" / "h" / "list").is_dir():
                self._log_queue.put("[DB] AssetBundle から型データ読み込み中...\n")
                try:
                    ptrees = _load_pattern_trees(kks_dir, self._log_queue.put)
                    type_maps = {
                        "insert":   _build_insert_map(ptrees.get(3, [])),
                        "houshi":   _build_houshi_map(ptrees.get(2, [])),
                        "aibu":     _build_aibu_map(ptrees.get(1, [])),
                        "sit_0":    _build_situation_map(ptrees.get(0, []), _START_TAGS),
                        "sit_4":    _build_situation_map(ptrees.get(4, []), _MAST_TAGS),
                        "sit_6":    _build_situation_map(ptrees.get(6, []), _LES_TAGS),
                    }
                    self._log_queue.put(
                        f"[DB] 型マップ: insert={len(type_maps['insert'])}, "
                        f"houshi={len(type_maps['houshi'])}, "
                        f"aibu={len(type_maps['aibu'])}, "
                        f"situation={len(type_maps['sit_0'])}\n"
                    )
                except Exception as e:
                    self._log_queue.put(f"[WARN] 型データ読み込みエラー: {e}\n")
            elif kks_dir:
                self._log_queue.put("[WARN] KKSフォルダ内に abdata/h/list が見つかりません\n")
            else:
                self._log_queue.put("[DB] KKSフォルダ未指定 → 型列は空のまま構築\n")

            # ── CSVからセリフ辞書を構築 ──
            serif_map = {}
            csv_dir = Path(kks_dir) / "voice_extract" / "voice_csv" if kks_dir else None
            if csv_dir and csv_dir.is_dir():
                csv_files = sorted(csv_dir.glob("c*.csv"))
                for cp in csv_files:
                    try:
                        with cp.open(encoding="utf-8-sig") as f:
                            for line in f:
                                parts = line.rstrip("\n").split("|")
                                if len(parts) >= 4:
                                    serif_map[parts[0]] = parts[3]
                    except Exception as e:
                        self._log_queue.put(f"[WARN] CSV読み込みエラー {cp.name}: {e}\n")
                self._log_queue.put(f"[DB] セリフ辞書: {len(serif_map)}件 ({len(csv_files)}ファイル)\n")
            else:
                self._log_queue.put("[DB] voice_extract/voice_csv が見つからないため serif は空\n")

            voices_rows = []
            total_skip  = 0

            wav_root = Path(wav_dir)
            char_dirs = sorted(wav_root.glob("c*"))
            for char_dir in char_dirs:
                if not char_dir.is_dir():
                    continue
                char = char_dir.name
                wavs = sorted(char_dir.rglob("*.wav"))
                self._log_queue.put(f"[{char}] {len(wavs)} ファイル処理中...\n")
                for wav_path in wavs:
                    fn = wav_path.name
                    parsed = parse_voice_filename(fn)
                    if parsed is None:
                        total_skip += 1
                        continue

                    vid = parsed["voice_id"]
                    tc  = parsed["type_code"]

                    if type_maps:
                        if tc in ("so", "so3p"):
                            insert_type    = type_maps["insert"].get(vid)
                            houshi_type    = None
                            aibu_type      = None
                            situation_type = None
                        elif tc in ("hh", "hh3p"):
                            insert_type    = None
                            houshi_type    = type_maps["houshi"].get(vid)
                            aibu_type      = None
                            situation_type = None
                        elif tc == "ai":
                            insert_type    = None
                            houshi_type    = None
                            aibu_type      = type_maps["aibu"].get(vid)
                            situation_type = None
                        elif tc in ("ka", "ka3p"):
                            insert_type    = None
                            houshi_type    = None
                            aibu_type      = None
                            situation_type = type_maps["sit_0"].get(vid)
                        elif tc == "on":
                            insert_type    = None
                            houshi_type    = None
                            aibu_type      = None
                            situation_type = type_maps["sit_4"].get(vid)
                        elif tc == "ko":
                            insert_type    = None
                            houshi_type    = None
                            aibu_type      = None
                            situation_type = type_maps["sit_6"].get(vid)
                        else:
                            insert_type = houshi_type = aibu_type = situation_type = None
                    else:
                        insert_type = houshi_type = aibu_type = situation_type = None

                    voices_rows.append((
                        parsed["chara"],
                        parsed["mode_name"],
                        parsed["voice_id"],
                        parsed["level"],
                        parsed["level_name"],
                        parsed["filename"],
                        parsed["file_type"],
                        insert_type, houshi_type,
                        aibu_type,   situation_type,
                        str(wav_path),
                        serif_map.get(fn, ""),
                    ))

            self._log_queue.put(f"[DB] {len(voices_rows)} 件 INSERT 中...\n")
            conn.executemany("""
                INSERT INTO voices
                    (chara, mode_name, voice_id, level, level_name, filename,
                     file_type, insert_type, houshi_type, aibu_type, situation_type,
                     wav_path, serif)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, voices_rows)
            conn.commit()
            conn.close()

            self._log_queue.put(
                f"\n── 完了 ──\n"
                f"  voices : {len(voices_rows)} 件\n"
                f"  スキップ: {total_skip} 件（名前が不一致）\n"
                f"  DB出力 : {db_path}\n"
            )
        except Exception as e:
            self._log_queue.put(f"[ERROR] {e}\n")
        finally:
            self._log_queue.put("__done__")
