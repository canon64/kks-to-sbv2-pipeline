"""
train_tab.py - SBV2 Trainer GUI を tk.Frame タブとして実装
"""

import datetime as dt
import json
import os
import queue
import shlex
import socket
import subprocess
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox

from .train_history import TrainHistoryMixin
from .train_import import TrainImportMixin
from .train_preprocess import TrainPipelineMixin
from .train_ui import TrainUIMixin

# ── 定数 ──────────────────────────────────────────────────────────────────────

_TRAIN_STATE_PATH  = Path(__file__).resolve().with_name("sbv2_trainer_state.json")
_TRAIN_HISTORY_MAX = 100
_DEFAULT_SBV2_ROOT = ""

_MODES = ["train_ms_jp_extra", "train_ms", "train", "custom"]

_MODE_SCRIPT = {
    "train_ms_jp_extra": "train_ms_jp_extra.py",
    "train_ms": "train_ms.py",
    "train": "train.py",
}


# ── TrainTab ───────────────────────────────────────────────────────────────────

class TrainTab(TrainHistoryMixin, TrainImportMixin, TrainPipelineMixin, TrainUIMixin, tk.Frame):
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

        self.epochs_var        = tk.StringVar(value="10")
        self.eval_interval_var = tk.StringVar(value="1000")
        self.batch_size_var    = tk.StringVar(value="2")

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

    # ── ブラウズ ──────────────────────────────────────────────────────────────

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

    # ── モード制御 ────────────────────────────────────────────────────────────

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

    # ── コマンド / パス ───────────────────────────────────────────────────────

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

    # ── 分散環境 ─────────────────────────────────────────────────────────────

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
        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
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

    # ── サブプロセス同期実行 ──────────────────────────────────────────────────

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

    # ── Start / Stop ─────────────────────────────────────────────────────────

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
                sanitize = imported.get("sanitize") or {}
                if sanitize:
                    self.log_queue.put(("line",
                        "[import:sanitize] "
                        f"raw={sanitize.get('raw_lines', 0)} "
                        f"kept={sanitize.get('normalized_lines', 0)} "
                        f"drop_empty={sanitize.get('skipped_empty_text', 0)} "
                        f"drop_malformed={sanitize.get('skipped_malformed', 0)} "
                        f"fix(??)={sanitize.get('repl_comma_small_tsu', 0)} "
                        f"fix(??)={sanitize.get('repl_ellipsis_small_tsu', 0)} "
                        f"fix(???)={sanitize.get('repl_kanji_kaki', 0)} "
                        f"drop_non_cp932={sanitize.get('drop_non_cp932_chars', 0)}\n"))
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
            pid = self.process.pid
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True)
            self.status_var.set("Terminate requested")
            self._log("[stop] taskkill /F /T sent\n")
        except Exception as e:
            messagebox.showerror("Stop failed", str(e))

    # ── ログ ─────────────────────────────────────────────────────────────────

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

    # ── 状態永続化 ────────────────────────────────────────────────────────────

    def _snapshot(self) -> dict:
        return {
            "python_exe":      self.python_exe_var.get().strip(),
            "sbv2_root":       self.sbv2_root_var.get().strip(),
            "mode":            self.mode_var.get().strip(),
            "dataset_path":    self.dataset_path_var.get().strip(),
            "wav_src":         self.wav_src_var.get().strip(),
            "csv_src":         self.csv_src_var.get().strip(),
            "auto_import":     bool(self.auto_import_var.get()),
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

    # ── アプリ終了フック ──────────────────────────────────────────────────────

    def on_app_close(self) -> bool:
        """親ウィンドウの WM_DELETE_WINDOW から呼ぶ。False を返したら閉じをキャンセル。"""
        if self.process and self.process.poll() is None:
            if not messagebox.askyesno("Running", "プロセスが実行中です。終了しますか？"):
                return False
        self.app_state["last"] = self._snapshot()
        self._write_state()
        return True
