"""
browse_state.py - BrowseTab の状態永続化・履歴機能を提供する Mixin
"""

import datetime as dt
import json
import tkinter as tk
from pathlib import Path
from tkinter import messagebox

from .kks_constants import APP_STATE_PATH, HISTORY_MAX
from .extract_tab import _load_char_display_map


class BrowseStateMixin:
    """状態保存・復元・履歴ウィンドウを担う Mixin。

    利用側クラスが持つべき属性:
        self.app_state       : dict
        self.history_win     : Toplevel | None
        self.history_list    : Listbox | None
        self._char_display_map : dict
        self._db_var         : StringVar
        self._exp_var        : StringVar
        self._tbl_var        : StringVar
        self._combo_vars     : dict[str, StringVar]
        self._like_vars      : dict[str, StringVar]
    利用側クラスが実装すべきメソッド:
        self._connect()
        self._on_table_changed()
        self._run_query()
    """

    # ── スナップショット ───────────────────────────────────────────────────────

    def _snapshot(self):
        return {
            "saved_at":      dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "db_path":       self._db_var.get(),
            "export_dir":    self._exp_var.get(),
            "table":         self._tbl_var.get(),
            "combo_filters": {k: v.get() for k, v in self._combo_vars.items()},
            "like_filters":  {k: v.get() for k, v in self._like_vars.items()},
        }

    def _apply_snapshot(self, snap):
        if not snap:
            return
        if snap.get("db_path"):
            self._db_var.set(snap["db_path"])
        if snap.get("export_dir"):
            self._exp_var.set(snap["export_dir"])
        if snap.get("table"):
            self._tbl_var.set(snap["table"])
        for k, v in snap.get("combo_filters", {}).items():
            if k in self._combo_vars:
                if k == "chara" and v:
                    v = self._char_display_map.get(v, v)
                self._combo_vars[k].set(v)
        for k, v in snap.get("like_filters", {}).items():
            if k in self._like_vars:
                self._like_vars[k].set(v)

    # ── 状態保存 ──────────────────────────────────────────────────────────────

    def _save_last(self):
        self.app_state["last"] = self._snapshot()
        self._write_state()

    def _push_history(self):
        snap = self._snapshot()
        hist = self.app_state.setdefault("history", [])
        hist.insert(0, snap)
        self.app_state["history"] = hist[:HISTORY_MAX]
        self._write_state()

    def _load_state(self):
        if APP_STATE_PATH.exists():
            try:
                self.app_state = json.loads(APP_STATE_PATH.read_text("utf-8"))
            except Exception:
                pass
        kks_dir = self.app_state.get("extract", {}).get("kks_dir", "")
        self._char_display_map = _load_char_display_map(kks_dir)
        self._char_rules = {
            str(k): str(v)
            for k, v in self.app_state.get("char_rules", {}).items()
        }

    def set_char_map(self, char_map: dict):
        self._char_display_map = char_map
        self._load_distinct_values()

    def _write_state(self):
        try:
            existing = {}
            if APP_STATE_PATH.exists():
                try:
                    existing = json.loads(APP_STATE_PATH.read_text("utf-8"))
                except Exception:
                    pass
            existing["last"]      = self.app_state.get("last")
            existing["history"]   = self.app_state.get("history", [])
            existing["char_rules"] = self.app_state.get("char_rules", {})
            tmp = APP_STATE_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(existing, ensure_ascii=False, indent=2),
                           encoding="utf-8")
            tmp.replace(APP_STATE_PATH)
        except Exception:
            pass

    def _apply_last(self):
        self._apply_snapshot(self.app_state.get("last"))
        if not self._exp_var.get():
            kks_dir = self.app_state.get("extract", {}).get("kks_dir", "")
            if kks_dir:
                self._exp_var.set(str(Path(kks_dir) / "extract_wave"))
        db_path = self._db_var.get().strip()
        if db_path and Path(db_path).is_file():
            self._connect()

    # ── 履歴ウィンドウ ────────────────────────────────────────────────────────

    def _open_history(self):
        if self.history_win and self.history_win.winfo_exists():
            self.history_win.lift()
            return
        self.history_win = tk.Toplevel(self)
        self.history_win.title("検索履歴")
        self.history_win.geometry("520x400")
        lb_fr = tk.Frame(self.history_win)
        lb_fr.pack(fill="both", expand=True, padx=6, pady=6)
        self.history_list = tk.Listbox(lb_fr, width=70)
        sb = tk.Scrollbar(lb_fr, command=self.history_list.yview)
        self.history_list.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self.history_list.pack(fill="both", expand=True)
        self._refresh_history()
        btns = tk.Frame(self.history_win)
        btns.pack(fill="x", padx=6, pady=4)
        tk.Button(btns, text="適用", command=self._apply_history,
                  width=10).pack(side="left", padx=2)
        tk.Button(btns, text="削除", command=self._delete_history,
                  width=10).pack(side="left", padx=2)
        tk.Button(btns, text="全削除",
                  command=self._clear_history, width=10).pack(side="left", padx=2)

    def _refresh_history(self):
        if not self.history_list:
            return
        self.history_list.delete(0, "end")
        for h in self.app_state.get("history", []):
            ts    = h.get("saved_at", "")
            tbl   = h.get("table", "")
            combo = {k: v for k, v in h.get("combo_filters", {}).items() if v}
            like  = {k: v for k, v in h.get("like_filters",  {}).items() if v}
            label = f"{ts}  [{tbl}]"
            if combo:
                label += "  " + " ".join(f"{k}={v}" for k, v in combo.items())
            if like:
                label += "  " + " ".join(f"{k}~{v}" for k, v in like.items())
            self.history_list.insert("end", label)

    def _apply_history(self):
        sel = self.history_list.curselection()
        if not sel:
            return
        snap = self.app_state["history"][sel[0]]
        self._apply_snapshot(snap)
        if self.conn:
            self._on_table_changed()
            self._run_query()

    def _delete_history(self):
        sel = self.history_list.curselection()
        if not sel:
            return
        del self.app_state["history"][sel[0]]
        self._write_state()
        self._refresh_history()

    def _clear_history(self):
        if not messagebox.askyesno("確認", "履歴を全削除しますか？"):
            return
        self.app_state["history"] = []
        self._write_state()
        self._refresh_history()
