"""
char_rules_tab.py - 文字変換ルール管理タブ

変換前→変換後のルールをリストで管理する。
ルールは APP_STATE_PATH の "char_rules" キーに保存される。
"""

import json
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk

from .kks_constants import APP_STATE_PATH


class CharRulesTab(tk.Frame):

    def __init__(self, parent, on_rules_changed=None):
        tk.Frame.__init__(self, parent)
        self._on_rules_changed = on_rules_changed
        self._rules = {}   # {from: to}
        self._load_rules()
        self._build_ui()

    # ── 保存・読込 ──────────────────────────────────────────────────────────

    def _load_rules(self):
        if APP_STATE_PATH.exists():
            try:
                state = json.loads(APP_STATE_PATH.read_text("utf-8"))
                self._rules = {str(k): str(v)
                               for k, v in state.get("char_rules", {}).items()}
            except Exception:
                pass

    def _save_rules(self):
        try:
            existing = {}
            if APP_STATE_PATH.exists():
                try:
                    existing = json.loads(APP_STATE_PATH.read_text("utf-8"))
                except Exception:
                    pass
            existing["char_rules"] = self._rules
            tmp = APP_STATE_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(existing, ensure_ascii=False, indent=2),
                           encoding="utf-8")
            tmp.replace(APP_STATE_PATH)
        except Exception:
            pass
        if self._on_rules_changed:
            self._on_rules_changed(dict(self._rules))

    # ── UI ─────────────────────────────────────────────────────────────────

    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=6)

        tk.Label(top, text="変換前:").pack(side="left")
        self._from_var = tk.StringVar()
        tk.Entry(top, textvariable=self._from_var, width=12).pack(side="left", padx=4)
        tk.Label(top, text="→").pack(side="left")
        self._to_var = tk.StringVar()
        tk.Entry(top, textvariable=self._to_var, width=12).pack(side="left", padx=4)
        tk.Label(top, text="(空白=削除)").pack(side="left")
        tk.Button(top, text="追加", command=self._add_rule,
                  bg="#4CAF50", fg="white", width=8).pack(side="left", padx=8)

        list_fr = tk.Frame(self)
        list_fr.pack(fill="both", expand=True, padx=8, pady=4)

        cols = ("from", "to")
        self._tree = ttk.Treeview(list_fr, columns=cols, show="headings", height=20)
        self._tree.heading("from", text="変換前")
        self._tree.heading("to",   text="変換後")
        self._tree.column("from", width=200, anchor="center")
        self._tree.column("to",   width=200, anchor="center")
        sb = ttk.Scrollbar(list_fr, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._tree.pack(fill="both", expand=True)

        btn_fr = tk.Frame(self)
        btn_fr.pack(fill="x", padx=8, pady=4)
        tk.Button(btn_fr, text="選択行を削除", command=self._delete_rule,
                  width=14).pack(side="left", padx=2)
        tk.Button(btn_fr, text="全削除", command=self._clear_rules,
                  width=10).pack(side="left", padx=2)

        self._refresh_list()

    def _refresh_list(self):
        self._tree.delete(*self._tree.get_children())
        for src, dst in self._rules.items():
            self._tree.insert("", "end", values=(src, dst if dst else "(削除)"))

    # ── 操作 ────────────────────────────────────────────────────────────────

    def _add_rule(self):
        src = self._from_var.get()
        dst = self._to_var.get()
        if not src:
            messagebox.showwarning("入力エラー", "変換前の文字を入力してください。")
            return
        self._rules[src] = dst
        self._from_var.set("")
        self._to_var.set("")
        self._refresh_list()
        self._save_rules()

    def _delete_rule(self):
        sel = self._tree.selection()
        if not sel:
            return
        for item in sel:
            src = self._tree.item(item, "values")[0]
            self._rules.pop(src, None)
        self._refresh_list()
        self._save_rules()

    def _clear_rules(self):
        if not messagebox.askyesno("確認", "全ルールを削除しますか？"):
            return
        self._rules.clear()
        self._refresh_list()
        self._save_rules()

    # ── 外部から規則を取得 ───────────────────────────────────────────────────

    def get_rules(self) -> dict:
        return dict(self._rules)
