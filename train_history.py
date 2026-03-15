"""
train_history.py - TrainTab の履歴ウィンドウ機能を提供する Mixin
"""

import datetime as dt
import tkinter as tk
from tkinter import messagebox, ttk


class TrainHistoryMixin:
    """履歴ウィンドウの表示・適用・削除を担う Mixin。

    利用側クラスが持つべき属性:
        self.app_state  : {"last": ..., "history": [...]}
        self.hist_win   : Toplevel | None
        self.hist_list  : Listbox | None
        self.status_var : StringVar
    利用側クラスが実装すべきメソッド:
        self._snapshot() -> dict
        self._apply_snapshot(snap: dict)
        self._write_state()
    """

    def _hist_label(self, item: dict) -> str:
        t = str(item.get("saved_at", "?"))
        s = item.get("settings", {}) or {}
        return f"{t} | {s.get('mode', '?')} | {s.get('dataset_path', s.get('model_name', '?'))}"

    def _open_history(self):
        if self.hist_win and self.hist_win.winfo_exists():
            self.hist_win.lift()
            self._refresh_hist_listbox()
            return
        win = tk.Toplevel(self)
        win.title("History")
        win.geometry("980x400")
        win.transient(self)
        win.columnconfigure(0, weight=1)
        win.rowconfigure(0, weight=1)
        lb = tk.Listbox(win)
        lb.grid(row=0, column=0, sticky="nsew", padx=(8, 0), pady=8)
        lb.bind("<Double-Button-1>", lambda _: self._apply_hist_selected())
        ys = ttk.Scrollbar(win, orient="vertical", command=lb.yview)
        ys.grid(row=0, column=1, sticky="ns", pady=8, padx=(4, 8))
        lb.configure(yscrollcommand=ys.set)
        bf = ttk.Frame(win)
        bf.grid(row=1, column=0, columnspan=2, sticky="ew", padx=8, pady=(0, 8))
        ttk.Button(bf, text="Apply",     command=self._apply_hist_selected).pack(side="left", padx=2)
        ttk.Button(bf, text="Delete",    command=self._del_hist_selected).pack(side="left", padx=2)
        ttk.Button(bf, text="Clear All", command=self._clear_history).pack(side="left", padx=2)
        ttk.Button(bf, text="Close",     command=win.destroy).pack(side="left", padx=2)
        self.hist_win  = win
        self.hist_list = lb
        self._refresh_hist_listbox()

    def _refresh_hist_listbox(self):
        if not self.hist_list or not self.hist_list.winfo_exists():
            return
        self.hist_list.delete(0, "end")
        for item in self.app_state.get("history", []):
            self.hist_list.insert("end", self._hist_label(item))

    def _apply_hist_selected(self):
        if not self.hist_list or not self.hist_list.winfo_exists():
            return
        sel = self.hist_list.curselection()
        if not sel:
            return
        idx  = int(sel[0])
        hist = self.app_state.get("history", [])
        if 0 <= idx < len(hist):
            self._apply_snapshot(hist[idx].get("settings", {}))
            self.app_state["last"] = self._snapshot()
            self._write_state()
            self.status_var.set("History applied.")

    def _del_hist_selected(self):
        if not self.hist_list or not self.hist_list.winfo_exists():
            return
        sel = self.hist_list.curselection()
        if not sel:
            return
        idx  = int(sel[0])
        hist = self.app_state.get("history", [])
        if 0 <= idx < len(hist):
            del hist[idx]
            self._write_state()
            self._refresh_hist_listbox()

    def _clear_history(self):
        if not messagebox.askyesno("Confirm", "履歴を全て削除しますか？"):
            return
        self.app_state["history"] = []
        self._write_state()
        self._refresh_hist_listbox()

    def _push_history(self, snap: dict):
        import json
        hist = self.app_state.setdefault("history", [])
        item = {"saved_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "settings": snap}
        if hist and self._sig(hist[0].get("settings")) == self._sig(snap):
            hist[0] = item
        else:
            hist.insert(0, item)
        from train_tab import _TRAIN_HISTORY_MAX
        self.app_state["history"] = hist[:_TRAIN_HISTORY_MAX]
        self._refresh_hist_listbox()

    def _sig(self, s) -> str:
        import json
        if not isinstance(s, dict):
            return ""
        return json.dumps(s, sort_keys=True, ensure_ascii=False)
