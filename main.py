"""
KKS to SBV2 Pipeline
---------------------
Tab 1: 抽出              - KKS AssetBundle から WAV を抽出
Tab 2: DB構築            - 抽出済み WAV から SQLite DB を構築
Tab 3: ブラウズ          - DB を閲覧・絞り込み・エクスポート
Tab 4: SBV2トレーニング  - Style-Bert-VITS2 のトレーニング
"""

import json
import tkinter as tk
from pathlib import Path
from tkinter import ttk

from kks_voice_studio import APP_STATE_PATH, ExtractTab, BuildDbTab, BrowseTab
from train_tab import TrainTab


class KksAllInOneGui(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("KKS to SBV2 Pipeline")
        self.geometry("1400x900")

        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True)

        self._tab_extract = ExtractTab(nb, on_kks_change=self._on_kks_change)
        self._tab_build   = BuildDbTab(nb, on_build_done=self._on_build_done,
                                       get_kks_dir=lambda: self._tab_extract._kks_var.get())
        self._tab_browse  = BrowseTab(nb, on_export_done=self._on_export_done)
        self._tab_train   = TrainTab(nb)

        nb.add(self._tab_extract, text="  抽出  ")
        nb.add(self._tab_build,   text="  DB構築  ")
        nb.add(self._tab_browse,  text="  ブラウズ  ")
        nb.add(self._tab_train,   text="  SBV2トレーニング  ")

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._load_settings()

    def _load_settings(self):
        if not APP_STATE_PATH.exists():
            return
        try:
            state = json.loads(APP_STATE_PATH.read_text("utf-8"))
            self._tab_extract.apply_settings(state.get("extract"))
            build_s = state.get("build", {})
            kks_dir = state.get("extract", {}).get("kks_dir", "")
            if kks_dir and not build_s.get("wav_dir"):
                build_s = dict(build_s, wav_dir=str(Path(kks_dir) / "wave"))
            if kks_dir and not build_s.get("db_path"):
                build_s = dict(build_s, db_path=str(Path(kks_dir) / "wave" / "kks_voices.db"))
            self._tab_build.apply_settings(build_s)
        except Exception:
            pass

    def _save_settings(self):
        try:
            existing = {}
            if APP_STATE_PATH.exists():
                try:
                    existing = json.loads(APP_STATE_PATH.read_text("utf-8"))
                except Exception:
                    pass
            existing["extract"] = self._tab_extract.get_settings()
            existing["build"]   = self._tab_build.get_settings()
            tmp = APP_STATE_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(APP_STATE_PATH)
        except Exception:
            pass

    def _on_close(self):
        if not self._tab_train.on_app_close():
            return
        self._save_settings()
        self.destroy()

    def _on_build_done(self, db_path: str):
        self._tab_browse._db_var.set(db_path)
        self._tab_browse._connect()

    def _on_export_done(self, wav_dir: str, csv_path: str):
        self._tab_train.wav_src_var.set(wav_dir)
        if csv_path:
            self._tab_train.csv_src_var.set(csv_path)

    def _on_kks_change(self, kks_dir: str):
        wave_dir = str(Path(kks_dir) / "wave")
        if not self._tab_build._wav_var.get():
            self._tab_build._wav_var.set(wave_dir)
        if not self._tab_build._db_var.get():
            self._tab_build._db_var.set(str(Path(kks_dir) / "wave" / "kks_voices.db"))
        if not self._tab_browse._exp_var.get():
            self._tab_browse._exp_var.set(str(Path(kks_dir) / "extract_wave"))


if __name__ == "__main__":
    KksAllInOneGui().mainloop()
