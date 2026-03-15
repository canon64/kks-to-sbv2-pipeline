"""
extract_tab.py - KKS AssetBundle から WAV を抽出する ExtractTab
"""

import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox

try:
    import UnityPy
    UNITYPY_OK = True
except ImportError:
    UNITYPY_OK = False

from .kks_constants import ALL_CHARS


def _load_char_display_map(kks_dir: str) -> dict:
    """ハードコードされた character_map を返す。"""
    return {
        "c00": "c00 セクシー系お姉さま",
        "c01": "c01 お嬢様",
        "c02": "c02 タカビー",
        "c03": "c03 小悪魔",
        "c04": "c04 ミステリアス",
        "c05": "c05 電波",
        "c06": "c06 大和撫子",
        "c07": "c07 ボーイッシュ",
        "c08": "c08 純真無垢",
        "c09": "c09 アホの子",
        "c10": "c10 邪気眼",
        "c11": "c11 母性的",
        "c12": "c12 姉御肌",
        "c13": "c13 ギャル",
        "c14": "c14 不良少女",
        "c15": "c15 野性的",
        "c16": "c16 意識高クール",
        "c17": "c17 ひねくれ",
        "c18": "c18 不幸少女",
        "c19": "c19 文学少女",
        "c20": "c20 モジモジ",
        "c21": "c21 正統派ヒロイン",
        "c22": "c22 ミーハー",
        "c23": "c23 オタク女子",
        "c24": "c24 ヤンデレ",
        "c25": "c25 ダル",
        "c26": "c26 無口",
        "c27": "c27 意地っ張り",
        "c28": "c28 ロリばばぁ",
        "c29": "c29 素直クール",
        "c30": "c30 気さく",
        "c31": "c31 勝ち気",
        "c32": "c32 誠実",
        "c33": "c33 艶やか",
        "c34": "c34 帰国子女",
        "c35": "c35 方言娘",
        "c36": "c36 Ｓッ気",
        "c37": "c37 無感情",
        "c38": "c38 几帳面",
        "c39": "c39 島っ娘",
        "c40": "c40 高潔",
        "c41": "c41 ボクっ娘",
        "c42": "c42 天真爛漫",
        "c43": "c43 ノリノリ",
    }


class ExtractTab(tk.Frame):
    def __init__(self, parent, on_kks_change=None):
        super().__init__(parent)
        self._log_queue     = queue.Queue()
        self._running       = False
        self._on_kks_change = on_kks_change
        self._build_ui()

    def _build_ui(self):
        pad = {"padx": 6, "pady": 3}

        # KKS root
        fr = tk.Frame(self)
        fr.pack(fill="x", **pad)
        tk.Label(fr, text="KKSフォルダ:", width=14, anchor="w").pack(side="left")
        self._kks_var = tk.StringVar()
        tk.Entry(fr, textvariable=self._kks_var).pack(side="left", fill="x", expand=True)
        tk.Button(fr, text="参照", command=self._browse_kks).pack(side="left", padx=2)

        # Output dir
        fr2 = tk.Frame(self)
        fr2.pack(fill="x", **pad)
        tk.Label(fr2, text="WAV出力先:", width=14, anchor="w").pack(side="left")
        self._out_var = tk.StringVar(value="")
        tk.Entry(fr2, textvariable=self._out_var).pack(side="left", fill="x", expand=True)
        tk.Button(fr2, text="参照", command=self._browse_out).pack(side="left", padx=2)

        # Char select
        lf = tk.LabelFrame(self, text="キャラクター選択")
        lf.pack(fill="x", padx=6, pady=3)

        btn_fr = tk.Frame(lf)
        btn_fr.pack(fill="x")
        tk.Button(btn_fr, text="全選択",  command=self._select_all).pack(side="left", padx=2)
        tk.Button(btn_fr, text="全解除", command=self._deselect_all).pack(side="left", padx=2)

        cb_fr = tk.Frame(lf)
        cb_fr.pack(fill="x")
        self._char_vars = {}
        self._char_cbs  = {}
        for i, ch in enumerate(ALL_CHARS):
            var = tk.BooleanVar(value=False)
            self._char_vars[ch] = var
            w = tk.Checkbutton(cb_fr, text=ch, variable=var, width=14, anchor="w")
            w.grid(row=i // 8, column=i % 8, sticky="w")
            self._char_cbs[ch] = w

        # Start / Stop
        ctrl = tk.Frame(self)
        ctrl.pack(fill="x", **pad)
        self._start_btn = tk.Button(ctrl, text="▶ 抽出開始", command=self._start,
                                    bg="#4CAF50", fg="white", width=16)
        self._start_btn.pack(side="left", padx=2)
        self._stop_btn  = tk.Button(ctrl, text="■ 停止", command=self._stop,
                                    state=tk.DISABLED, width=10)
        self._stop_btn.pack(side="left", padx=2)
        self._status_var = tk.StringVar(value="待機中")
        tk.Label(ctrl, textvariable=self._status_var).pack(side="left", padx=8)

        # Log
        self._log = tk.Text(self, height=18, state=tk.DISABLED,
                            font=("Consolas", 9), wrap="word")
        sb = tk.Scrollbar(self, command=self._log.yview)
        self._log.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._log.pack(fill="both", expand=True, padx=6, pady=3)

    def update_char_labels(self, char_map: dict):
        for ch, widget in self._char_cbs.items():
            widget.config(text=char_map.get(ch, ch))

    def _browse_kks(self):
        d = filedialog.askdirectory(title="KKSインストールフォルダを選択")
        if d:
            self._kks_var.set(d)
            self._out_var.set(str(Path(d) / "wave"))
            self.update_char_labels(_load_char_display_map(d))
            if self._on_kks_change:
                self._on_kks_change(d)

    def _browse_out(self):
        d = filedialog.askdirectory(title="WAV出力先を選択")
        if d:
            self._out_var.set(d)

    def _select_all(self):
        for v in self._char_vars.values():
            v.set(True)

    def _deselect_all(self):
        for v in self._char_vars.values():
            v.set(False)

    def get_settings(self):
        return {
            "kks_dir": self._kks_var.get(),
            "out_dir": self._out_var.get(),
            "chars": {k: v.get() for k, v in self._char_vars.items()},
        }

    def apply_settings(self, d):
        if not d:
            return
        if d.get("kks_dir"):
            self._kks_var.set(d["kks_dir"])
            self.update_char_labels(_load_char_display_map(d["kks_dir"]))
        if d.get("out_dir"):
            self._out_var.set(d["out_dir"])
        elif d.get("kks_dir"):
            self._out_var.set(str(Path(d["kks_dir"]) / "wave"))
        for k, v in d.get("chars", {}).items():
            if k in self._char_vars:
                self._char_vars[k].set(bool(v))

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
                    self._start_btn.config(state=tk.NORMAL)
                    self._stop_btn.config(state=tk.DISABLED)
                    self._status_var.set("完了")
                    return
                self._append_log(item)
        except queue.Empty:
            pass
        if self._running:
            self.after(100, self._drain)

    def _start(self):
        if not UNITYPY_OK:
            messagebox.showerror("エラー", "UnityPy が見つかりません。\npip install UnityPy")
            return
        kks = self._kks_var.get().strip()
        out = self._out_var.get().strip()
        chars = [c for c, v in self._char_vars.items() if v.get()]
        if not kks:
            messagebox.showerror("エラー", "KKSフォルダを指定してください。")
            return
        if not out:
            messagebox.showerror("エラー", "WAV出力先を指定してください。")
            return
        if not chars:
            messagebox.showerror("エラー", "キャラクターを1つ以上選択してください。")
            return
        self._running = True
        self._start_btn.config(state=tk.DISABLED)
        self._stop_btn.config(state=tk.NORMAL)
        self._status_var.set("抽出中...")
        threading.Thread(target=self._worker, args=(kks, out, chars),
                         daemon=True).start()
        self.after(100, self._drain)

    def _stop(self):
        self._running = False
        self._log_queue.put("[停止要求]\n")

    def _worker(self, kks_root: str, out_dir: str, chars: list):
        total = 0
        for char in chars:
            if not self._running:
                self._log_queue.put("[停止しました]\n")
                break
            bundle_dir = Path(kks_root) / "abdata" / "sound" / "data" / "pcm" / char / "h"
            if not bundle_dir.exists():
                self._log_queue.put(f"[skip] {char}: フォルダなし\n")
                continue
            char_out = Path(out_dir) / char
            char_out.mkdir(parents=True, exist_ok=True)
            count = 0
            for bp in sorted(bundle_dir.glob("*.unity3d")):
                if not self._running:
                    break
                self._log_queue.put(f"  [{char}] {bp.name}\n")
                try:
                    env = UnityPy.load(str(bp))
                    for obj in env.objects:
                        if obj.type.name != "AudioClip":
                            continue
                        clip = obj.read()
                        wav_name = clip.m_Name + ".wav"
                        out_path = char_out / wav_name
                        if out_path.exists():
                            continue
                        for audio_data in clip.samples.values():
                            out_path.write_bytes(audio_data)
                            count += 1
                            break
                except Exception as e:
                    self._log_queue.put(f"  [error] {bp.name}: {e}\n")
            self._log_queue.put(f"[完了] {char}: {count} ファイル\n")
            total += count
        self._log_queue.put(f"\n── 合計 {total} ファイル抽出 ──\n")
        self._log_queue.put("__done__")
