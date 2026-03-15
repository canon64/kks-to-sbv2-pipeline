"""
train_ui.py - TrainTab の UI 構築 (_build_ui) と GPU 検出を提供する Mixin
"""

import re
import subprocess
import tkinter as tk
from tkinter import ttk

_MODES = ["train_ms_jp_extra", "train_ms", "train", "custom"]


class TrainUIMixin:
    """UI ウィジェットの構築を担う Mixin。

    利用側クラスが持つべき StringVar/BooleanVar 属性:
        python_exe_var, sbv2_root_var, mode_var, dataset_path_var,
        wav_src_var, csv_src_var, auto_import_var, extra_args_var,
        custom_cmd_var, status_var, epochs_var, eval_interval_var,
        batch_size_var, gpu_var
    利用側クラスが実装すべきメソッド:
        _browse_root, _browse_dataset, _browse_wav, _browse_csv,
        _on_mode_changed, _start_training, _stop_training,
        _open_history, _save_last, _clear_log,
        _detect_gpus, _refresh_gpu_list, _refresh_preview
    """

    def _build_ui(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        top = ttk.Frame(self, padding=8)
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(1, weight=1)
        top.columnconfigure(3, weight=2)

        def lbl(parent, text, row, col, **kw):
            ttk.Label(parent, text=text).grid(
                row=row, column=col, sticky="w", pady=2, **kw)

        r = 0
        lbl(top, "Python", r, 0)
        ttk.Entry(top, textvariable=self.python_exe_var, width=18).grid(
            row=r, column=1, sticky="ew", padx=(4, 12), pady=2)
        lbl(top, "SBV2 Root", r, 2)
        ttk.Entry(top, textvariable=self.sbv2_root_var).grid(
            row=r, column=3, sticky="ew", padx=(4, 4), pady=2)
        ttk.Button(top, text="Browse", command=self._browse_root).grid(
            row=r, column=4, pady=2)
        r += 1

        lbl(top, "Mode", r, 0)
        mode_cb = ttk.Combobox(top, textvariable=self.mode_var,
                               state="readonly", values=_MODES, width=18)
        mode_cb.grid(row=r, column=1, sticky="w", padx=(4, 12), pady=2)
        mode_cb.bind("<<ComboboxSelected>>", lambda _: self._on_mode_changed())
        lbl(top, "Dataset Path (-m)", r, 2)
        self.dataset_entry = ttk.Entry(top, textvariable=self.dataset_path_var)
        self.dataset_entry.grid(row=r, column=3, sticky="ew", padx=(4, 4), pady=2)
        self.dataset_browse_btn = ttk.Button(top, text="Browse",
                                             command=self._browse_dataset)
        self.dataset_browse_btn.grid(row=r, column=4, pady=2)
        r += 1

        lbl(top, "WAV Folder", r, 0)
        self.wav_entry = ttk.Entry(top, textvariable=self.wav_src_var)
        self.wav_entry.grid(row=r, column=1, columnspan=3, sticky="ew",
                            padx=(4, 4), pady=2)
        self.wav_browse_btn = ttk.Button(top, text="Browse", command=self._browse_wav)
        self.wav_browse_btn.grid(row=r, column=4, pady=2)
        r += 1

        lbl(top, "CSV (esd.list)", r, 0)
        self.csv_entry = ttk.Entry(top, textvariable=self.csv_src_var)
        self.csv_entry.grid(row=r, column=1, columnspan=3, sticky="ew",
                            padx=(4, 4), pady=2)
        self.csv_browse_btn = ttk.Button(top, text="Browse", command=self._browse_csv)
        self.csv_browse_btn.grid(row=r, column=4, pady=2)
        r += 1

        self.auto_import_chk = ttk.Checkbutton(
            top, text="Start前にWAV+CSVをDatasetへ取り込む",
            variable=self.auto_import_var)
        self.auto_import_chk.grid(row=r, column=0, columnspan=5, sticky="w", pady=2)
        r += 1

        lbl(top, "Extra Args", r, 0)
        self.extra_entry = ttk.Entry(top, textvariable=self.extra_args_var)
        self.extra_entry.grid(row=r, column=1, columnspan=4, sticky="ew",
                              padx=(4, 0), pady=2)
        r += 1

        params_frame = ttk.Frame(top)
        params_frame.grid(row=r, column=0, columnspan=5, sticky="w", pady=2)
        ttk.Label(params_frame, text="Epochs").pack(side="left")
        ttk.Entry(params_frame, textvariable=self.epochs_var, width=8).pack(
            side="left", padx=(4, 16))
        ttk.Label(params_frame, text="Eval Interval").pack(side="left")
        ttk.Entry(params_frame, textvariable=self.eval_interval_var, width=8).pack(
            side="left", padx=(4, 16))
        ttk.Label(params_frame, text="Batch Size").pack(side="left")
        ttk.Entry(params_frame, textvariable=self.batch_size_var, width=5).pack(
            side="left", padx=(4, 0))
        r += 1

        lbl(top, "GPU", r, 0)
        gpu_values = ["auto"] + self._detect_gpus()
        self._gpu_cb = ttk.Combobox(top, textvariable=self.gpu_var,
                                    values=gpu_values, width=40)
        self._gpu_cb.grid(row=r, column=1, columnspan=3, sticky="w",
                          padx=(4, 4), pady=2)
        ttk.Button(top, text="Refresh", command=self._refresh_gpu_list).grid(
            row=r, column=4, pady=2)
        r += 1

        lbl(top, "Custom Cmd", r, 0)
        self.custom_entry = ttk.Entry(top, textvariable=self.custom_cmd_var)
        self.custom_entry.grid(row=r, column=1, columnspan=4, sticky="ew",
                               padx=(4, 0), pady=2)
        r += 1

        btn_row = ttk.Frame(top)
        btn_row.grid(row=r, column=0, columnspan=5, sticky="ew", pady=(8, 2))
        for text, cmd in [
            ("▶ Start",       self._start_training),
            ("■ Stop",        self._stop_training),
            ("History",       self._open_history),
            ("Save Settings", self._save_last),
            ("Clear Log",     self._clear_log),
        ]:
            ttk.Button(btn_row, text=text, command=cmd).pack(side="left", padx=2)
        r += 1

        lbl(top, "Command", r, 0)
        self.preview_var = tk.StringVar()
        ttk.Entry(top, textvariable=self.preview_var, state="readonly").grid(
            row=r, column=1, columnspan=4, sticky="ew", padx=(4, 0), pady=2)

        log_frame = ttk.LabelFrame(self, text="Output", padding=4)
        log_frame.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 4))
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)

        self.log_text = tk.Text(
            log_frame, wrap="none",
            bg="#1c1c1c", fg="#e0e0e0",
            font=("Consolas", 9),
            insertbackground="white")
        self.log_text.grid(row=0, column=0, sticky="nsew")
        ys = ttk.Scrollbar(log_frame, orient="vertical",
                           command=self.log_text.yview)
        ys.grid(row=0, column=1, sticky="ns")
        xs = ttk.Scrollbar(log_frame, orient="horizontal",
                           command=self.log_text.xview)
        xs.grid(row=1, column=0, sticky="ew")
        self.log_text.configure(yscrollcommand=ys.set, xscrollcommand=xs.set)

        ttk.Label(self, textvariable=self.status_var,
                  relief="sunken", anchor="w").grid(row=2, column=0, sticky="ew")

        for v in (self.python_exe_var, self.sbv2_root_var, self.mode_var,
                  self.dataset_path_var, self.extra_args_var, self.custom_cmd_var):
            v.trace_add("write", lambda *_: self._refresh_preview())

    # ── GPU 検出 ──────────────────────────────────────────────────────────────

    @staticmethod
    def _detect_gpus() -> list:
        """nvidia-smi → rocm-smi の順で GPU 一覧を取得。失敗時は空リスト。"""
        gpus = []
        try:
            out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=index,name", "--format=csv,noheader"],
                text=True, encoding="utf-8", errors="replace",
                timeout=5, stderr=subprocess.DEVNULL)
            for line in out.strip().splitlines():
                parts = line.split(",", 1)
                if len(parts) == 2:
                    gpus.append(f"{parts[0].strip()}: {parts[1].strip()}")
            if gpus:
                return gpus
        except Exception:
            pass
        try:
            out = subprocess.check_output(
                ["rocm-smi", "--showproductname"],
                text=True, encoding="utf-8", errors="replace",
                timeout=5, stderr=subprocess.DEVNULL)
            for line in out.strip().splitlines():
                m = re.search(r"GPU\[\s*(\d+)\].*?:\s*(.+)", line)
                if m:
                    gpus.append(f"{m.group(1)}: {m.group(2).strip()}")
            if gpus:
                return gpus
        except Exception:
            pass
        return gpus

    def _refresh_gpu_list(self):
        gpus = self._detect_gpus()
        values = ["auto"] + gpus
        self._gpu_cb["values"] = values
        if self.gpu_var.get() not in values:
            self.gpu_var.set("auto")
