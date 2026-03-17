"""
train_preprocess.py - TrainTab の前処理パイプライン機能を提供する Mixin
"""

import json
import re
import shutil
import wave
from pathlib import Path

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

_MODE_TEMPLATE_CONFIG = {
    "train_ms_jp_extra": "configs/config_jp_extra.json",
    "train_ms": "configs/config.json",
    "train": "configs/config.json",
}


def _is_resuming(models_dir: Path) -> bool:
    if not models_dir.is_dir():
        return False
    for _ in models_dir.glob("G_*.pth"):
        return True
    return False


class TrainPipelineMixin:
    """前処理パイプライン全体を担う Mixin。

    利用側クラスが持つべき属性:
        self.mode_var         : StringVar
        self.epochs_var       : StringVar
        self.eval_interval_var: StringVar
        self.batch_size_var   : StringVar
    利用側クラスが実装すべきメソッド:
        self._log(text: str)
        self._resolve_python(root: str) -> str
        self._resolve_dataset(root: str) -> str
        self._run_blocking(cmd, cwd: str, tag: str)
        self._speakers_in_list(list_path) -> set          (from TrainImportMixin)
        self._wav_paths_from_lists(list_paths) -> list    (from TrainImportMixin)
        self._count_missing_aux(wavs, suffix: str) -> list (from TrainImportMixin)
    """

    # ── config.yml 同期 ────────────────────────────────────────────────────

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

    # ── データセット config 書き換え ──────────────────────────────────────

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

    # ── 事前学習済みシードのセットアップ ─────────────────────────────────

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

    # ── SBV2 依存チェック ────────────────────────────────────────────────

    def _check_sbv2_deps(self, py: str, root: str):
        req = Path(root) / "requirements.txt"
        if not req.is_file():
            self._log("[deps] requirements.txt not found. Skipping.\n")
            return
        self._log("[deps] Checking SBV2 dependencies ...\n")
        # av はビルドが壊れやすいので先にバイナリで入れる（pyannote.audio の依存解決用）
        self._run_blocking(
            [py, "-m", "pip", "install", "--isolated", "-q", "--prefer-binary", "av"],
            root, "pip")
        self._run_blocking(
            [py, "-m", "pip", "install", "--isolated", "-q",
             "--prefer-binary", "-r", str(req)],
            root, "pip")
        self._log("[deps] Done.\n")

    # ── WAV リサンプル ────────────────────────────────────────────────────

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

    # ── 前処理パイプライン ────────────────────────────────────────────────

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
        self._check_sbv2_deps(py, root)

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
