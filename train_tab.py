"""
train_tab.py - SBV2 Trainer GUI を tk.Frame タブとして実装
"""

import datetime as dt
import json
import os
import queue
import re
import shlex
import shutil
import socket
import subprocess
import threading
import tkinter as tk
import wave
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

# ── 定数 ──────────────────────────────────────────────────────────────────────

_TRAIN_STATE_PATH = Path(__file__).resolve().with_name("sbv2_trainer_state.json")
_TRAIN_HISTORY_MAX = 100
_DEFAULT_SBV2_ROOT = r"D:\F\project_root\ai_tools\SBV2\Style-Bert-VITS2"

_MODES = ["train_ms_jp_extra", "train_ms", "train", "custom"]

_MODE_SCRIPT = {
    "train_ms_jp_extra": "train_ms_jp_extra.py",
    "train_ms": "train_ms.py",
    "train": "train.py",
}

_MODE_TEMPLATE_CONFIG = {
    "train_ms_jp_extra": "configs/config_jp_extra.json",
    "train_ms": "configs/config.json",
    "train": "configs/config.json",
}

_PRETRAINED_SEED_MAP = {
    "train_ms_jp_extra": {
        "dir": "pretrained_jp_extra",
        "files": ["G_0.safetensors", "D_0.safetensors", "WD_0.safetensors"],
    },
    "train_ms": {
        "dir": "pretrained",
        "files": ["G_0.safetensors", "D_0.safetensors", "DUR_0.safetensors"],
    },
    "train": {
        "dir": "pretrained",
        "files": ["G_0.safetensors", "D_0.safetensors"],
    },
}


def _is_resuming(models_dir: Path) -> bool:
    if not models_dir.is_dir():
        return False
    for _ in models_dir.glob("G_*.pth"):
        return True
    return False


# ── TrainTab ───────────────────────────────────────────────────────────────────

class TrainTab(tk.Frame):
    """SBV2 Trainer GUI をタブ埋め込み可能な Frame として実装。"""

    def __init__(self, parent, **kw):
        super().__init__(parent, **kw)

        self.python_exe_var   = tk.StringVar(value="auto")
        self.sbv2_root_var    = tk.StringVar(value=_DEFAULT_SBV2_ROOT)
        self.mode_var         = tk.StringVar(value="train_ms_jp_extra")
        self.dataset_path_var = tk.StringVar(value="Data/my_model")
        self.wav_src_var      = tk.StringVar(value="")
        self.csv_src_var      = tk.StringVar(value="")
        self.auto_import_var  = tk.BooleanVar(value=True)
        self.extra_args_var   = tk.StringVar(value="")
        self.custom_cmd_var   = tk.StringVar(value="")
        self.status_var       = tk.StringVar(value="Ready")

        # ── Train params ──
        self.epochs_var        = tk.StringVar(value="10")
        self.eval_interval_var = tk.StringVar(value="1000")
        self.batch_size_var    = tk.StringVar(value="2")

        # ── GPU ──
        self.gpu_var = tk.StringVar(value="auto")

        self.process       = None
        self.log_queue     = queue.Queue()
        self.reader_thread = None

        self.hist_win  = None
        self.hist_list = None

        self.app_state = {"last": None, "history": []}
        self._load_state()

        self._build_ui()
        self._apply_last_state()
        self._update_mode_ui()

        self.after(100, self._drain_log_queue)

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        top = ttk.Frame(self, padding=8)
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(1, weight=1)
        top.columnconfigure(3, weight=2)

        def lbl(parent, text, row, col, **kw):
            ttk.Label(parent, text=text).grid(row=row, column=col, sticky="w", pady=2, **kw)

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

        # Train params row
        params_frame = ttk.Frame(top)
        params_frame.grid(row=r, column=0, columnspan=5, sticky="w", pady=2)
        ttk.Label(params_frame, text="Epochs").pack(side="left")
        ttk.Entry(params_frame, textvariable=self.epochs_var, width=8).pack(side="left", padx=(4, 16))
        ttk.Label(params_frame, text="Eval Interval").pack(side="left")
        ttk.Entry(params_frame, textvariable=self.eval_interval_var, width=8).pack(side="left", padx=(4, 16))
        ttk.Label(params_frame, text="Batch Size").pack(side="left")
        ttk.Entry(params_frame, textvariable=self.batch_size_var, width=5).pack(side="left", padx=(4, 0))
        r += 1

        # GPU row
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
        ys = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
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

    # ── GPU detection ─────────────────────────────────────────────────────────

    @staticmethod
    def _detect_gpus() -> list:
        """nvidia-smi → rocm-smi の順で GPU 一覧を取得。失敗時は空リスト。"""
        gpus = []
        # NVIDIA
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
        # AMD (ROCm)
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

    # ── Browse helpers ────────────────────────────────────────────────────────

    def _browse_root(self):
        d = filedialog.askdirectory(title="SBV2 Root")
        if d:
            self.sbv2_root_var.set(d)

    def _browse_dataset(self):
        d = filedialog.askdirectory(title="Dataset Path")
        if d:
            self.dataset_path_var.set(d)

    def _browse_wav(self):
        d = filedialog.askdirectory(title="WAV Source Folder")
        if d:
            self.wav_src_var.set(d)

    def _browse_csv(self):
        f = filedialog.askopenfilename(
            title="CSV / esd.list",
            filetypes=[("List/CSV", "*.list *.csv *.txt"), ("All", "*.*")])
        if f:
            self.csv_src_var.set(f)

    # ── Mode handling ─────────────────────────────────────────────────────────

    def _on_mode_changed(self):
        self._update_mode_ui()

    def _update_mode_ui(self):
        if not hasattr(self, "dataset_entry"):
            return
        custom = (self.mode_var.get() == "custom")
        std   = "disabled" if custom else "normal"
        cust  = "normal" if custom else "disabled"
        for w in (self.dataset_entry, self.dataset_browse_btn,
                  self.wav_entry, self.wav_browse_btn,
                  self.csv_entry, self.csv_browse_btn,
                  self.auto_import_chk, self.extra_entry):
            w.configure(state=std)
        self.custom_entry.configure(state=cust)
        self._refresh_preview()

    # ── Command / path helpers ────────────────────────────────────────────────

    def _resolve_python(self, root: str) -> str:
        raw = (self.python_exe_var.get() or "").strip()
        if raw.casefold() in {"", "auto", "venv", "python", "python.exe", "py"}:
            if root:
                rp = Path(root)
                for rel in (".venv/Scripts/python.exe", "venv/Scripts/python.exe",
                            ".venv/bin/python", "venv/bin/python"):
                    c = (rp / rel).resolve()
                    if c.is_file():
                        return str(c)
            venv = os.environ.get("VIRTUAL_ENV", "").strip()
            if venv:
                for rel in ("Scripts/python.exe", "bin/python"):
                    c = (Path(venv) / rel).resolve()
                    if c.is_file():
                        return str(c)
            return "python"
        return raw

    def _abs(self, root: str, rel: str) -> str:
        v = (rel or "").strip()
        if not v:
            return ""
        p = Path(v)
        if p.is_absolute() or not root:
            return str(p)
        return str((Path(root) / p).resolve())

    def _resolve_dataset(self, root: str) -> str:
        v = self.dataset_path_var.get().strip()
        if not v:
            return ""
        d = Path(self._abs(root, v))
        if d.name.casefold() == "wavs":
            d = d.parent
        return str(d.resolve())

    def _build_command(self):
        mode = self.mode_var.get().strip()
        root = self.sbv2_root_var.get().strip()

        if mode == "custom":
            text = self.custom_cmd_var.get().strip()
            if not text:
                raise ValueError("Custom command is empty.")
            return shlex.split(text, posix=False), (root or None)

        if not root:
            raise ValueError("SBV2 Root is required.")

        dataset_path = self._resolve_dataset(root)
        if not dataset_path:
            raise ValueError("Dataset Path (-m) is required.")

        script = str((Path(root) / _MODE_SCRIPT[mode]).resolve())
        config = str(Path(dataset_path) / "config.json")
        py     = self._resolve_python(root)
        extra  = shlex.split(self.extra_args_var.get() or "", posix=False)
        cmd    = [py, script, "-c", config, "-m", dataset_path] + extra
        return cmd, root

    def _refresh_preview(self):
        try:
            cmd, cwd = self._build_command()
            prev = " ".join(self._q(a) for a in cmd)
            if cwd:
                prev = f"[cwd:{cwd}] {prev}"
            self.preview_var.set(prev)
        except Exception as e:
            self.preview_var.set(f"(invalid) {e}")

    @staticmethod
    def _q(v: str) -> str:
        s = str(v)
        if not s:
            return '""'
        if any(c in s for c in ' \t"\'&|()[]{}^=;!+,`~'):
            return f'"{s.replace(chr(34), chr(92) + chr(34))}"'
        return s

    # ── Distributed env ───────────────────────────────────────────────────────

    @staticmethod
    def _find_free_port(preferred: int = 10086) -> int:
        def avail(p):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.bind(("127.0.0.1", p))
                    return True
                except OSError:
                    return False
        if avail(preferred):
            return preferred
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    def _build_env(self, cmd):
        env  = os.environ.copy()

        # GPU selection
        gpu_sel = self.gpu_var.get().strip()
        if gpu_sel and gpu_sel.casefold() != "auto":
            idx = gpu_sel.split(":", 1)[0].strip()
            if idx.isdigit():
                env["CUDA_VISIBLE_DEVICES"] = idx

        name = Path(str(cmd[1]) if len(cmd) > 1 else "").name.lower()
        if not (name.startswith("train_ms") and name.endswith(".py")):
            return env, None
        port = self._find_free_port()
        env.update(MASTER_ADDR="127.0.0.1", MASTER_PORT=str(port))
        env.setdefault("WORLD_SIZE", "1")
        env.setdefault("RANK",       "0")
        env.setdefault("LOCAL_RANK", "0")
        return env, port

    # ── Blocking subprocess ───────────────────────────────────────────────────

    def _run_blocking(self, cmd, cwd: str, tag: str):
        self.log_queue.put(("line", f"[{tag}] " + " ".join(self._q(str(a)) for a in cmd) + "\n"))
        proc = subprocess.Popen(
            cmd, cwd=cwd,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1)
        if proc.stdout:
            for line in proc.stdout:
                self.log_queue.put(("line", line))
        code = proc.wait()
        self.log_queue.put(("line", f"[{tag}] exit={code}\n"))
        if code != 0:
            raise RuntimeError(f"[{tag}] failed (exit {code})")

    # ── File helpers ──────────────────────────────────────────────────────────

    def _read_lines(self, path) -> list:
        for enc in ("utf-8-sig", "utf-8", "cp932"):
            try:
                return Path(path).read_text(encoding=enc).splitlines()
            except UnicodeDecodeError:
                continue
        raise UnicodeDecodeError("text", b"", 0, 1, f"Cannot read: {path}")

    def _normalize_esd(self, lines: list) -> list:
        out = []
        for raw in lines:
            line = str(raw).strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("|", 3)
            if len(parts) < 4:
                continue
            wav  = Path(parts[0].replace("\ufeff", "").strip()).name
            spk  = parts[1].strip() or "spk0"
            lang = parts[2].strip() or "JP"
            text = parts[3].replace("\r", " ").replace("\n", " ").strip()
            if wav:
                out.append(f"{wav}|{spk}|{lang}|{text}")
        return out

    def _speakers_in_list(self, list_path) -> set:
        spk = set()
        p = Path(list_path)
        if not p.is_file():
            return spk
        for line in self._read_lines(p):
            parts = str(line).split("|")
            if len(parts) >= 2:
                s = parts[1].replace("\ufeff", "").strip()
                if s:
                    spk.add(s)
        return spk

    def _wav_paths_from_lists(self, list_paths) -> list:
        wavs = []
        seen = set()
        for lp in list_paths:
            p = Path(lp)
            if not p.is_file():
                continue
            base = p.parent
            for line in self._read_lines(p):
                parts = str(line).split("|", 1)
                if not parts:
                    continue
                rel = parts[0].replace("\ufeff", "").strip()
                if not rel:
                    continue
                wp  = Path(rel) if Path(rel).is_absolute() else (base / rel).resolve()
                key = str(wp).casefold()
                if key not in seen:
                    seen.add(key)
                    wavs.append(wp)
        return wavs

    def _count_missing_aux(self, wavs, suffix: str) -> list:
        missing = []
        for w in wavs:
            aux = Path(str(w) + ".npy") if suffix == ".npy" else w.with_suffix(suffix)
            if not aux.is_file():
                missing.append(aux)
        return missing

    # ── config.yml sync ───────────────────────────────────────────────────────

    def _sync_config_yml(self, root: str, dataset_path: str):
        cfg_path = Path(root) / "config.yml"
        if not cfg_path.is_file():
            return
        dataset       = Path(dataset_path).resolve()
        model_name    = dataset.name
        dataset_posix = dataset.as_posix()
        text  = cfg_path.read_text(encoding="utf-8")
        lines = text.splitlines()
        out   = []
        found_mn, found_dp = False, False
        for line in lines:
            if re.match(r"^\s*model_name\s*:", line):
                out.append(f'model_name: "{model_name}"')
                found_mn = True
            elif re.match(r"^\s*dataset_path\s*:", line):
                out.append(f'dataset_path: "{dataset_posix}"')
                found_dp = True
            else:
                out.append(line)
        if not found_mn:
            out.insert(0, f'model_name: "{model_name}"')
        if not found_dp:
            out.insert(1, f'dataset_path: "{dataset_posix}"')
        new = "\n".join(out).rstrip() + "\n"
        if new != text:
            cfg_path.write_text(new, encoding="utf-8")
            self._log(f"[config.yml] synced model_name={model_name}\n")

    # ── Dataset config rewrite ────────────────────────────────────────────────

    def _rewrite_dataset_config(self, template_path, dest_path, train_list,
                                val_list, dataset_name: str, use_jp_extra: bool):
        src = Path(template_path)
        dst = Path(dest_path)
        if not src.is_file():
            raise FileNotFoundError(f"Config template not found: {src}")
        cfg   = json.loads(src.read_text(encoding="utf-8"))
        data  = cfg.setdefault("data", {})
        model = cfg.setdefault("model", {})

        data["training_files"]   = str(train_list)
        data["validation_files"] = str(val_list)
        data["use_jp_extra"]     = use_jp_extra

        if use_jp_extra:
            model["gin_channels"]            = 512
            model["use_wavlm_discriminator"] = True
            model.setdefault("slm", {})["hidden"] = 768
            ver = str(cfg.get("version", "")).strip()
            if not ver.endswith("JP-Extra"):
                cfg["version"] = (ver + "-JP-Extra") if ver else "2.6.1-JP-Extra"

        if "model_name" in cfg:
            cfg["model_name"] = dataset_name

        # Train params from GUI
        train = cfg.setdefault("train", {})
        try:
            train["epochs"] = int(self.epochs_var.get())
        except ValueError:
            pass
        try:
            train["eval_interval"] = int(self.eval_interval_var.get())
        except ValueError:
            pass
        try:
            train["batch_size"] = int(self.batch_size_var.get())
        except ValueError:
            pass

        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── Pretrained seed setup ─────────────────────────────────────────────────

    def _ensure_pretrained_seeds(self, root: str, dataset_path: str):
        mode      = self.mode_var.get().strip()
        seed_info = _PRETRAINED_SEED_MAP.get(mode)
        if seed_info is None:
            return

        dataset_dir = Path(dataset_path)
        models_dir  = dataset_dir / "models"
        models_dir.mkdir(parents=True, exist_ok=True)

        if _is_resuming(models_dir):
            self._log("[pretrained] Existing checkpoints found → resuming. Skip seed copy.\n")
            return

        g0 = models_dir / "G_0.safetensors"
        if g0.is_file():
            self._log("[pretrained] G_0.safetensors already in models/. Skip seed copy.\n")
            return

        src_dir = Path(root) / seed_info["dir"]
        if not src_dir.is_dir():
            raise FileNotFoundError(
                f"Pretrained directory not found: {src_dir}\n"
                "Please download the pretrained models first.")

        self._log(f"[pretrained] Copying seed files from {src_dir.name}/ → {models_dir} ...\n")
        copied_any = False
        for fname in seed_info["files"]:
            src = src_dir / fname
            dst = models_dir / fname
            if dst.is_file():
                self._log(f"[pretrained]   {fname} already exists, skip.\n")
                continue
            if not src.is_file():
                self._log(f"[pretrained]   WARNING: {fname} not found in {src_dir}, skip.\n")
                continue
            shutil.copy2(str(src), str(dst))
            self._log(f"[pretrained]   Copied {fname}\n")
            copied_any = True

        if copied_any:
            self._log("[pretrained] Done. Trainer will fine-tune from pretrained model.\n")
        else:
            self._log("[pretrained] No new files copied.\n")

    # ── WAV resample ──────────────────────────────────────────────────────────

    def _resample_if_needed(self, root: str, py: str, wav_dir, target_sr: int):
        wav_dir    = Path(wav_dir)
        mismatched = []
        for w in wav_dir.rglob("*.wav"):
            try:
                with wave.open(str(w), "rb") as wf:
                    if wf.getframerate() != target_sr:
                        mismatched.append(w)
            except Exception:
                continue
        if not mismatched:
            self._log(f"[resample] All WAV at {target_sr} Hz. Skip.\n")
            return

        self._log(f"[resample] {len(mismatched)} file(s) need resampling → {target_sr} Hz\n")
        tmp = wav_dir.parent / "_wavs_tmp"
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)

        self._run_blocking(
            [py, str((Path(root) / "resample.py").resolve()),
             "--sr", str(target_sr), "-i", str(wav_dir), "-o", str(tmp)],
            root, "resample")

        for out in tmp.rglob("*.wav"):
            dst = wav_dir / out.relative_to(tmp)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(out), str(dst))

        for ext in ("*.bert.pt", "*.npy"):
            for p in wav_dir.rglob(ext):
                p.unlink(missing_ok=True)
        shutil.rmtree(tmp, ignore_errors=True)
        self._log("[resample] Done.\n")

    # ── Preprocess pipeline ───────────────────────────────────────────────────

    def _ensure_preprocessed(self, root: str, force_preprocess: bool = False):
        dataset_path = self._resolve_dataset(root)
        dataset_dir  = Path(dataset_path)
        wav_dir      = dataset_dir / "wavs"
        if not wav_dir.is_dir():
            raise FileNotFoundError(f"WAV directory not found: {wav_dir}")

        self._sync_config_yml(root, dataset_path)

        mode         = self.mode_var.get().strip()
        use_jp_extra = (mode == "train_ms_jp_extra")

        config_path = dataset_dir / "config.json"
        train_list  = dataset_dir / "train.list"
        val_list    = dataset_dir / "val.list"
        esd_list    = dataset_dir / "esd.list"

        py = self._resolve_python(root)

        tpl_rel  = _MODE_TEMPLATE_CONFIG.get(mode, "configs/config.json")
        template = (Path(root) / tpl_rel).resolve()
        if not template.is_file():
            template = (Path(root) / "configs/config.json").resolve()
        if not template.is_file():
            raise FileNotFoundError(f"Config template not found: {template}")

        self._rewrite_dataset_config(
            template_path=template, dest_path=config_path,
            train_list=train_list, val_list=val_list,
            dataset_name=dataset_dir.name, use_jp_extra=use_jp_extra)

        try:
            target_sr = int(json.loads(
                config_path.read_text(encoding="utf-8")
            ).get("data", {}).get("sampling_rate", 44100))
        except Exception:
            target_sr = 44100
        self._resample_if_needed(root, py, wav_dir, target_sr)

        need_pp = force_preprocess or not train_list.is_file() or not val_list.is_file()
        if not need_pp:
            try:
                cfg    = json.loads(config_path.read_text(encoding="utf-8"))
                spk2id = cfg.get("data", {}).get("spk2id", {})
                spk_keys = set(spk2id.keys()) if isinstance(spk2id, dict) else set()
                list_spk = (self._speakers_in_list(train_list) |
                            self._speakers_in_list(val_list))
                if not spk_keys or any(s not in spk_keys for s in list_spk):
                    need_pp = True
            except Exception:
                need_pp = True

        if need_pp:
            if not esd_list.is_file():
                raise FileNotFoundError(f"esd.list not found: {esd_list}")
            pp_cmd = [
                py, str((Path(root) / "preprocess_text.py").resolve()),
                "--transcription-path", str(esd_list),
                "--train-path",         str(train_list),
                "--val-path",           str(val_list),
                "--config-path",        str(config_path),
                "--correct_path",
            ]
            if use_jp_extra:
                pp_cmd.append("--use_jp_extra")
            self._run_blocking(pp_cmd, root, "preprocess_text")

        list_wavs     = self._wav_paths_from_lists([train_list, val_list])
        missing_bert  = self._count_missing_aux(list_wavs, ".bert.pt")
        missing_style = self._count_missing_aux(list_wavs, ".npy")

        if missing_bert:
            self._log(f"[preprocess] missing bert features: {len(missing_bert)}\n")
            self._run_blocking(
                [py, str((Path(root) / "bert_gen.py").resolve()), "-c", str(config_path)],
                root, "bert_gen")

        if missing_style:
            self._log(f"[preprocess] missing style features: {len(missing_style)}\n")
            self._run_blocking(
                [py, str((Path(root) / "style_gen.py").resolve()), "-c", str(config_path)],
                root, "style_gen")

        if not missing_bert and not missing_style and not need_pp:
            self._log("[preprocess] All artifacts exist. Skipped.\n")
        else:
            self._log("[preprocess] Done.\n")

    # ── Import WAV + CSV ──────────────────────────────────────────────────────

    def _import_dataset(self, root: str):
        if not bool(self.auto_import_var.get()):
            return None
        wav_src = self.wav_src_var.get().strip()
        csv_src = self.csv_src_var.get().strip()
        if not wav_src and not csv_src:
            return None
        if not wav_src or not csv_src:
            raise ValueError("WAV Folder と CSV の両方を入力してください。")
        if not Path(wav_src).is_dir():
            raise FileNotFoundError(f"WAV Folder not found: {wav_src}")
        if not Path(csv_src).is_file():
            raise FileNotFoundError(f"CSV not found: {csv_src}")

        dataset_path = self._resolve_dataset(root)
        if not dataset_path:
            raise ValueError("Dataset Path (-m) を入力してください。")

        dataset_dir = Path(dataset_path)
        dataset_dir.mkdir(parents=True, exist_ok=True)
        wav_dst = dataset_dir / "wavs"
        wav_dst.mkdir(exist_ok=True)
        esd_dst = dataset_dir / "esd.list"

        lines = self._read_lines(csv_src)
        norm  = self._normalize_esd(lines)
        if not norm:
            raise ValueError("CSV の有効行が0件です。")
        esd_dst.write_text("\n".join(norm) + "\n", encoding="utf-8")

        wav_names, seen = [], set()
        for line in norm:
            name = line.split("|", 1)[0].strip()
            k = name.casefold()
            if name and k not in seen:
                seen.add(k)
                wav_names.append(name)

        src_root  = Path(wav_src)
        src_index = None
        copied = skipped = 0
        missing = []

        for name in wav_names:
            dst = wav_dst / name
            if dst.exists():
                skipped += 1
                continue
            direct = src_root / name
            if direct.is_file():
                src = direct
            else:
                if src_index is None:
                    src_index = {}
                    for p in src_root.rglob("*"):
                        if p.is_file():
                            src_index.setdefault(p.name.casefold(), p)
                src = src_index.get(name.casefold()) if src_index else None

            if not src or not Path(src).is_file():
                missing.append(name)
                continue
            shutil.copy2(str(src), str(dst))
            copied += 1

        if missing:
            raise FileNotFoundError(
                f"WAVが不足しています ({len(missing)}件): " + ", ".join(missing[:10]))
        return {"dataset": str(dataset_dir), "copied": copied, "skipped": skipped}

    # ── Start / Stop ──────────────────────────────────────────────────────────

    def _start_training(self):
        if self.process is not None and self.process.poll() is None:
            messagebox.showwarning("Running", "プロセスが実行中です。")
            return
        self.status_var.set("Preparing…")
        threading.Thread(target=self._start_training_worker, daemon=True).start()

    def _start_training_worker(self):
        root = self.sbv2_root_var.get().strip()
        mode = self.mode_var.get().strip()

        try:
            imported = self._import_dataset(root)
            if imported:
                self.log_queue.put(("line",
                    f"[import] copied={imported['copied']} "
                    f"skipped={imported['skipped']} → {imported['dataset']}\n"))
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Import failed", str(e)))
            self.after(0, lambda: self.status_var.set("Ready"))
            return

        if mode != "custom":
            try:
                dataset_path = self._resolve_dataset(root)
                self._ensure_pretrained_seeds(root, dataset_path)
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Pretrained setup failed", str(e)))
                self.after(0, lambda: self.status_var.set("Ready"))
                return

            try:
                self._ensure_preprocessed(root, force_preprocess=bool(imported))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Preprocess failed", str(e)))
                self.after(0, lambda: self.status_var.set("Ready"))
                return

        try:
            cmd, cwd = self._build_command()
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Build command failed", str(e)))
            self.after(0, lambda: self.status_var.set("Ready"))
            return

        self.after(0, lambda: self._do_launch(cmd, cwd))

    def _do_launch(self, cmd, cwd):
        self._log(f"\n[start] {dt.datetime.now():%Y-%m-%d %H:%M:%S}\n")
        self._log(f"[python] {cmd[0]}\n")
        self._log("[cmd] " + " ".join(self._q(str(a)) for a in cmd) + "\n")
        if cwd:
            self._log(f"[cwd] {cwd}\n")

        run_env, port = self._build_env(cmd)
        if port:
            self._log(f"[dist] MASTER_PORT={port}\n")

        try:
            self.process = subprocess.Popen(
                cmd, cwd=cwd, env=run_env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace", bufsize=1)
        except Exception as e:
            self.process = None
            messagebox.showerror("Start failed", str(e))
            self.status_var.set("Ready")
            return

        self.reader_thread = threading.Thread(
            target=self._reader_worker, args=(self.process,), daemon=True)
        self.reader_thread.start()

        snap = self._snapshot()
        self.app_state["last"] = snap
        self._push_history(snap)
        self._write_state()
        self.status_var.set("Training started")

    def _stop_training(self):
        if self.process is None or self.process.poll() is not None:
            messagebox.showinfo("Info", "実行中のプロセスはありません。")
            return
        try:
            self.process.terminate()
            self.status_var.set("Terminate requested")
            self._log("[stop] terminate requested\n")
        except Exception as e:
            messagebox.showerror("Stop failed", str(e))

    # ── Log ───────────────────────────────────────────────────────────────────

    def _log(self, text: str):
        self.log_text.insert("end", text)
        self.log_text.see("end")

    def _clear_log(self):
        self.log_text.delete("1.0", "end")

    def _reader_worker(self, proc):
        try:
            if proc.stdout:
                for line in proc.stdout:
                    self.log_queue.put(("line", line))
        except Exception as e:
            self.log_queue.put(("line", f"[reader-error] {e}\n"))
        finally:
            code = proc.wait()
            self.log_queue.put(("exit", code))

    def _drain_log_queue(self):
        try:
            while True:
                kind, payload = self.log_queue.get_nowait()
                if kind == "line":
                    self._log(payload)
                elif kind == "exit":
                    code = int(payload)
                    self.status_var.set(f"Process exited (code={code})")
                    self._log(f"\n[exit] code={code}\n")
                    self.process = None
        except queue.Empty:
            pass
        self.after(100, self._drain_log_queue)

    # ── State persistence ─────────────────────────────────────────────────────

    def _snapshot(self) -> dict:
        return {
            "python_exe":   self.python_exe_var.get().strip(),
            "sbv2_root":    self.sbv2_root_var.get().strip(),
            "mode":         self.mode_var.get().strip(),
            "dataset_path": self.dataset_path_var.get().strip(),
            "wav_src":      self.wav_src_var.get().strip(),
            "csv_src":      self.csv_src_var.get().strip(),
            "auto_import":  bool(self.auto_import_var.get()),
            "extra_args":      self.extra_args_var.get().strip(),
            "custom_cmd":      self.custom_cmd_var.get().strip(),
            "epochs":          self.epochs_var.get().strip(),
            "eval_interval":   self.eval_interval_var.get().strip(),
            "batch_size":      self.batch_size_var.get().strip(),
            "gpu":             self.gpu_var.get().strip(),
        }

    def _apply_snapshot(self, s: dict):
        if not isinstance(s, dict):
            return
        py = str(s.get("python_exe", "auto")).strip()
        if py.casefold() in {"", "python", "python.exe"}:
            py = "auto"
        self.python_exe_var.set(py)
        self.sbv2_root_var.set(str(s.get("sbv2_root", _DEFAULT_SBV2_ROOT)))
        mode = str(s.get("mode", "train_ms_jp_extra")).strip()
        if mode not in _MODES:
            mode = "train_ms_jp_extra"
        self.mode_var.set(mode)
        dp = str(s.get("dataset_path", s.get("model_name", "Data/my_model"))).strip()
        self.dataset_path_var.set(dp)
        self.wav_src_var.set(str(s.get("wav_src", s.get("wav_source_dir", ""))))
        self.csv_src_var.set(str(s.get("csv_src",
                                        s.get("csv_path", s.get("transcription_csv", "")))))
        self.auto_import_var.set(bool(s.get("auto_import", True)))
        self.extra_args_var.set(str(s.get("extra_args", "")))
        self.custom_cmd_var.set(str(s.get("custom_cmd", s.get("custom_command", ""))))
        self.epochs_var.set(str(s.get("epochs", "10")))
        self.eval_interval_var.set(str(s.get("eval_interval", "1000")))
        self.batch_size_var.set(str(s.get("batch_size", "2")))
        self.gpu_var.set(str(s.get("gpu", "auto")))
        self._update_mode_ui()

    def _sig(self, s) -> str:
        if not isinstance(s, dict):
            return ""
        return json.dumps(s, sort_keys=True, ensure_ascii=False)

    def _push_history(self, snap: dict):
        hist = self.app_state.setdefault("history", [])
        item = {"saved_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "settings": snap}
        if hist and self._sig(hist[0].get("settings")) == self._sig(snap):
            hist[0] = item
        else:
            hist.insert(0, item)
        self.app_state["history"] = hist[:_TRAIN_HISTORY_MAX]
        self._refresh_hist_listbox()

    def _load_state(self):
        self.app_state = {"last": None, "history": []}
        if _TRAIN_STATE_PATH.exists():
            try:
                data = json.loads(_TRAIN_STATE_PATH.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    self.app_state = data
            except Exception:
                pass

    def _write_state(self):
        try:
            tmp = _TRAIN_STATE_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.app_state, ensure_ascii=False, indent=2),
                           encoding="utf-8")
            tmp.replace(_TRAIN_STATE_PATH)
        except Exception:
            pass

    def _save_last(self):
        self.app_state["last"] = self._snapshot()
        self._write_state()
        self.status_var.set("Settings saved.")

    def _apply_last_state(self):
        snap = self.app_state.get("last")
        if isinstance(snap, dict):
            self._apply_snapshot(snap)

    # ── History window ────────────────────────────────────────────────────────

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

    def on_app_close(self) -> bool:
        """親ウィンドウの WM_DELETE_WINDOW から呼ぶ。False を返したら閉じをキャンセル。"""
        if self.process and self.process.poll() is None:
            if not messagebox.askyesno("Running", "プロセスが実行中です。終了しますか？"):
                return False
        self.app_state["last"] = self._snapshot()
        self._write_state()
        return True
