"""
browse_tab.py - DB を閲覧・絞り込み・エクスポートする BrowseTab
"""

import csv
import datetime as dt
import os
import shutil
import sqlite3
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from kks_constants import VISIBLE_COLS, COMBO_FILTERS, LIKE_FILTERS, INVALID_FS_CHARS
from browse_state import BrowseStateMixin


def sanitize(value: str, max_len: int = 120) -> str:
    if not value:
        return "_"
    for c in INVALID_FS_CHARS:
        value = value.replace(c, "_")
    return value.strip()[:max_len] or "_"


class BrowseTab(BrowseStateMixin, tk.Frame):
    def __init__(self, parent, on_export_done=None):
        # tk.Frame を先に初期化してから Mixin の状態を設定する
        tk.Frame.__init__(self, parent)
        self._on_export_done   = on_export_done
        self.conn              = None
        self.table_columns     = {}
        self.current_rows      = []
        self.current_visible   = []
        self.current_where     = ""
        self.current_params    = []
        self.app_state         = {"last": None, "history": []}
        self.history_win       = None
        self.history_list      = None
        self._char_display_map = {}
        self._load_state()
        self._build_ui()
        self._apply_last()

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", padx=6, pady=3)

        tk.Label(top, text="DB:", width=4, anchor="w").pack(side="left")
        self._db_var = tk.StringVar()
        tk.Entry(top, textvariable=self._db_var, width=50).pack(side="left")
        tk.Button(top, text="参照", command=self._choose_db).pack(side="left", padx=2)
        tk.Button(top, text="接続", command=self._connect).pack(side="left", padx=2)

        tk.Label(top, text="  保存先:", anchor="w").pack(side="left")
        self._exp_var = tk.StringVar(value="")
        tk.Entry(top, textvariable=self._exp_var, width=30).pack(side="left")
        tk.Button(top, text="参照", command=self._choose_exp).pack(side="left", padx=2)

        mid = tk.Frame(self)
        mid.pack(fill="x", padx=6, pady=2)
        tk.Label(mid, text="テーブル:").pack(side="left")
        self._tbl_var = tk.StringVar(value="voices")
        self._tbl_combo = ttk.Combobox(mid, textvariable=self._tbl_var,
                                       state="readonly", width=14)
        self._tbl_combo.pack(side="left", padx=4)
        self._tbl_combo.bind("<<ComboboxSelected>>", lambda e: self._on_table_changed())
        self._total_var = tk.StringVar(value="0件")
        tk.Label(mid, textvariable=self._total_var).pack(side="left", padx=8)

        filt_lf = tk.LabelFrame(self, text="フィルタ")
        filt_lf.pack(fill="x", padx=6, pady=2)
        self._combo_vars = {k: tk.StringVar() for k in COMBO_FILTERS}
        self._like_vars  = {k: tk.StringVar() for k in LIKE_FILTERS}
        self._combo_widgets = {}
        self._like_widgets  = {}

        row1 = tk.Frame(filt_lf)
        row1.pack(fill="x")
        for k in COMBO_FILTERS:
            fr = tk.Frame(row1)
            fr.pack(side="left", padx=3)
            tk.Label(fr, text=k, font=("", 8)).pack()
            cb = ttk.Combobox(fr, textvariable=self._combo_vars[k],
                              state="readonly", width=14)
            cb.pack()
            self._combo_widgets[k] = cb

        row2 = tk.Frame(filt_lf)
        row2.pack(fill="x", pady=2)
        for k in LIKE_FILTERS:
            fr = tk.Frame(row2)
            fr.pack(side="left", padx=3)
            tk.Label(fr, text=f"{k}含む", font=("", 8)).pack()
            e = tk.Entry(fr, textvariable=self._like_vars[k], width=20)
            e.pack()
            self._like_widgets[k] = e

        btns = tk.Frame(filt_lf)
        btns.pack(fill="x", pady=2)
        tk.Button(btns, text="検索", command=self._search,
                  bg="#4CAF50", fg="white", width=10).pack(side="left", padx=4)
        tk.Button(btns, text="クリア", command=self._clear_filters,
                  width=8).pack(side="left", padx=2)
        tk.Button(btns, text="履歴", command=self._open_history,
                  width=8).pack(side="left", padx=2)

        pane = tk.PanedWindow(self, orient="vertical", sashwidth=6)
        pane.pack(fill="both", expand=True, padx=6, pady=3)

        tree_fr = tk.Frame(pane)
        pane.add(tree_fr, height=320)
        self._tree = ttk.Treeview(tree_fr, selectmode="extended")
        xsb = ttk.Scrollbar(tree_fr, orient="horizontal", command=self._tree.xview)
        ysb = ttk.Scrollbar(tree_fr, orient="vertical",   command=self._tree.yview)
        self._tree.configure(xscrollcommand=xsb.set, yscrollcommand=ysb.set)
        xsb.pack(side="bottom", fill="x")
        ysb.pack(side="right",  fill="y")
        self._tree.pack(fill="both", expand=True)
        self._tree.bind("<<TreeviewSelect>>", self._on_select)

        det_fr = tk.Frame(pane)
        pane.add(det_fr, height=120)
        self._detail = tk.Text(det_fr, height=6, state=tk.DISABLED,
                               font=("Consolas", 9), wrap="word")
        det_sb = tk.Scrollbar(det_fr, command=self._detail.yview)
        self._detail.configure(yscrollcommand=det_sb.set)
        det_sb.pack(side="right", fill="y")
        self._detail.pack(fill="both", expand=True)

        exp_fr = tk.Frame(self)
        exp_fr.pack(fill="x", padx=6, pady=3)
        tk.Button(exp_fr, text="全選択",
                  command=self._select_all_rows, width=8).pack(side="left", padx=2)
        tk.Button(exp_fr, text="表示中を保存",
                  command=lambda: self._export(all_displayed=True),
                  width=16).pack(side="left", padx=2)
        tk.Button(exp_fr, text="選択行を保存",
                  command=lambda: self._export(all_displayed=False),
                  width=16).pack(side="left", padx=2)
        self._save_csv_var = tk.BooleanVar(value=True)
        tk.Checkbutton(exp_fr, text="CSVも保存",
                       variable=self._save_csv_var).pack(side="left", padx=6)
        self._flat_var = tk.BooleanVar(value=True)
        tk.Checkbutton(exp_fr, text="フラット(1フォルダ)",
                       variable=self._flat_var).pack(side="left", padx=6)
        self._status_var = tk.StringVar(value="Ready")
        tk.Label(exp_fr, textvariable=self._status_var).pack(side="left", padx=8)

    # ── DB 接続 ───────────────────────────────────────────────────────────────

    def _choose_db(self):
        p = filedialog.askopenfilename(
            title="DB を選択",
            filetypes=[("SQLite", "*.db"), ("All", "*.*")])
        if p:
            self._db_var.set(p)

    def _choose_exp(self):
        d = filedialog.askdirectory(title="保存先を選択")
        if d:
            self._exp_var.set(d)

    def _connect(self):
        db_path = self._db_var.get().strip()
        if not db_path or not Path(db_path).is_file():
            messagebox.showerror("Error", f"DBが見つかりません:\n{db_path}")
            return
        try:
            if self.conn:
                self.conn.close()
            self.conn = sqlite3.connect(db_path)
            self.conn.row_factory = sqlite3.Row
            self.table_columns = {}
            cur = self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [r[0] for r in cur]
            for t in tables:
                cols = [r[1] for r in self.conn.execute(f"PRAGMA table_info({t})")]
                self.table_columns[t] = cols
            self._tbl_combo["values"] = tables
            if "voices" in tables:
                self._tbl_var.set("voices")
            elif tables:
                self._tbl_var.set(tables[0])
            self._on_table_changed()
            self._status_var.set(f"接続: {Path(db_path).name}")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _on_table_changed(self):
        self._refresh_filter_state()
        self._load_distinct_values()

    def _refresh_filter_state(self):
        tbl = self._tbl_var.get()
        cols = self.table_columns.get(tbl, [])
        for k, w in self._combo_widgets.items():
            w.config(state="readonly" if k in cols else tk.DISABLED)
            if k not in cols:
                self._combo_vars[k].set("")
        for k, w in self._like_widgets.items():
            w.config(state=tk.NORMAL if k in cols else tk.DISABLED)
            if k not in cols:
                self._like_vars[k].set("")

    def _load_distinct_values(self):
        if not self.conn:
            return
        tbl  = self._tbl_var.get()
        cols = self.table_columns.get(tbl, [])
        for k in COMBO_FILTERS:
            if k not in cols:
                continue
            cur = self.conn.execute(
                f"SELECT DISTINCT TRIM({k}) FROM {tbl} WHERE {k} IS NOT NULL "
                f"ORDER BY TRIM({k})")
            if k == "chara":
                vals = [""] + sorted({
                    self._char_display_map.get(r[0], r[0]) for r in cur if r[0]
                })
            else:
                vals = [""] + sorted({r[0] for r in cur if r[0]})
            self._combo_widgets[k]["values"] = vals

    # ── 検索 ─────────────────────────────────────────────────────────────────

    def _build_where(self):
        tbl  = self._tbl_var.get()
        cols = self.table_columns.get(tbl, [])
        clauses, params = [], []
        for k in COMBO_FILTERS:
            v = self._combo_vars[k].get().strip()
            if v and k in cols:
                if k == "chara":
                    v = v.split()[0]
                clauses.append(f"TRIM({k}) = ?")
                params.append(v)
        for k in LIKE_FILTERS:
            v = self._like_vars[k].get().strip()
            if v and k in cols:
                clauses.append(f"{k} LIKE ?")
                params.append(f"%{v}%")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        return where, params

    def _search(self):
        self._run_query()
        self._push_history()
        self._save_last()

    def _run_query(self):
        if not self.conn:
            return
        tbl   = self._tbl_var.get()
        cols  = self.table_columns.get(tbl, [])
        where, params = self._build_where()
        self.current_where  = where
        self.current_params = params
        cnt = self.conn.execute(
            f"SELECT COUNT(*) FROM {tbl} {where}", params).fetchone()[0]
        self._total_var.set(f"{cnt:,}件")
        order = next((c for c in ["id","idx","voice_id","filename","rowid"]
                      if c in cols), "rowid")
        cur = self.conn.execute(
            f"SELECT * FROM {tbl} {where} ORDER BY {order}", params)
        self.current_rows    = [dict(r) for r in cur]
        self.current_visible = [c for c in VISIBLE_COLS.get(tbl, []) if c in cols]
        self._populate_tree()

    def _populate_tree(self):
        self._tree.delete(*self._tree.get_children())
        self._tree["columns"] = self.current_visible
        self._tree["show"]    = "headings"
        widths = {"id":50,"chara":60,"mode_name":90,"voice_id":70,
                  "level_name":70,"filename":200,"file_type":70,
                  "wav_path":300,"serif":300}
        for c in self.current_visible:
            w = widths.get(c, 100)
            self._tree.heading(c, text=c)
            self._tree.column(c, width=w, minwidth=40, stretch=False)
        for row in self.current_rows:
            vals = [str(row.get(c, "")) for c in self.current_visible]
            self._tree.insert("", "end", values=vals)

    def _on_select(self, _event=None):
        sel = self._tree.selection()
        if not sel:
            return
        idx = self._tree.index(sel[0])
        if idx >= len(self.current_rows):
            return
        row = self.current_rows[idx]
        text = "\n".join(f"{k}: {v}" for k, v in row.items())
        self._detail.config(state=tk.NORMAL)
        self._detail.delete("1.0", "end")
        self._detail.insert("end", text)
        self._detail.config(state=tk.DISABLED)

    def _select_all_rows(self):
        self._tree.selection_set(self._tree.get_children())

    def _clear_filters(self):
        for v in self._combo_vars.values():
            v.set("")
        for v in self._like_vars.values():
            v.set("")

    # ── エクスポート ──────────────────────────────────────────────────────────

    def _get_rows_for_export(self, all_displayed: bool):
        if all_displayed:
            return self.current_rows
        sel = self._tree.selection()
        idxs = [self._tree.index(s) for s in sel]
        return [self.current_rows[i] for i in idxs if i < len(self.current_rows)]

    def _build_relative_export_path(self, row: dict) -> Path:
        tbl        = self._tbl_var.get()
        chara      = sanitize(str(row.get("chara") or ""))
        mode_name  = row.get("mode_name")
        mode_seg   = sanitize(str(mode_name)) if mode_name \
                     else f"mode_{row.get('mode','unknown')}"
        level_name = row.get("level_name")
        level_seg  = sanitize(str(level_name)) if level_name \
                     else f"level_{row.get('level','unknown')}"
        category   = (row.get("file_type") or row.get("breath_type") or
                      row.get("houshi_type") or row.get("aibu_type") or
                      row.get("situation_type") or "voice")
        cat_seg    = sanitize(str(category))
        src        = str(row.get("wav_path") or "")
        ext        = Path(src).suffix if Path(src).suffix else ".wav"
        fn         = sanitize(str(row.get("filename") or f"id_{row.get('id','unknown')}"))
        return Path(tbl) / chara / mode_seg / level_seg / cat_seg / f"{fn}{ext}"

    def _voice_text_row(self, row: dict) -> list:
        fn = str(row.get("filename") or "").strip()
        if not fn:
            src = str(row.get("wav_path") or "")
            fn  = Path(src).name if src else f"id_{row.get('id','unknown')}.wav"
        if not Path(fn).suffix:
            fn += ".wav"
        chara = str(row.get("chara") or "").strip() or "unknown"
        serif = str(row.get("serif") or "")
        serif = serif.replace("\r\n", "\n").replace("\r", "\n").replace("\n", " ")
        return [fn, chara, "JP", serif]

    def _export(self, all_displayed: bool):
        rows    = self._get_rows_for_export(all_displayed)
        exp_dir = self._exp_var.get().strip()
        tbl     = self._tbl_var.get()
        if not rows:
            messagebox.showinfo("Info", "エクスポート対象がありません。")
            return
        if not exp_dir:
            messagebox.showerror("Error", "保存先を指定してください。")
            return

        filter_parts = [
            sanitize(self._combo_vars[k].get())
            for k in COMBO_FILTERS if self._combo_vars[k].get().strip()
        ]
        filter_tag = "_".join(filter_parts) if filter_parts else tbl

        dest_root = Path(exp_dir)
        if self._flat_var.get():
            dest_root = dest_root / filter_tag
        dest_root.mkdir(parents=True, exist_ok=True)

        copied = missing = failed = duplicate_skipped = 0
        voice_text_rows = []
        seen_sources    = set()
        seen_dest_paths = set()

        for row in rows:
            src = row.get("wav_path")
            if not src:
                missing += 1
                continue
            src_norm = os.path.normcase(os.path.normpath(str(src)))
            if src_norm in seen_sources:
                duplicate_skipped += 1
                continue
            rel_norm = os.path.normcase(str(self._build_relative_export_path(row)))
            if rel_norm in seen_dest_paths:
                duplicate_skipped += 1
                continue
            if not os.path.isfile(src):
                missing += 1
                continue
            if self._flat_var.get():
                src_path = str(row.get("wav_path") or "")
                ext = Path(src_path).suffix if Path(src_path).suffix else ".wav"
                fn  = sanitize(str(row.get("filename") or f"id_{row.get('id','unknown')}"))
                dst = dest_root / f"{fn}{ext}"
            else:
                dst = dest_root / self._build_relative_export_path(row)
            dst.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy2(src, dst)
                copied += 1
                seen_sources.add(src_norm)
                seen_dest_paths.add(rel_norm)
                voice_text_rows.append(self._voice_text_row(row))
            except Exception:
                failed += 1

        vtext_path = None
        if self._save_csv_var.get() and voice_text_rows:
            stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            vtext_path = dest_root / f"voice_text_{filter_tag}_{stamp}.csv"
            with vtext_path.open("w", newline="", encoding="utf-8-sig") as f:
                csv.writer(f, delimiter="|", lineterminator="\n").writerows(voice_text_rows)

        msg = (f"保存完了\n対象行: {len(rows)}\n保存成功: {copied}\n"
               f"重複スキップ: {duplicate_skipped}\nファイルなし: {missing}\n失敗: {failed}")
        self._status_var.set(msg.replace("\n", " | "))
        messagebox.showinfo("エクスポート完了", msg)
        os.startfile(dest_root)

        if self._on_export_done and copied > 0:
            csv_out = str(vtext_path) if (self._save_csv_var.get() and voice_text_rows) else ""
            self._on_export_done(str(dest_root), csv_out)
