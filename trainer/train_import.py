"""
train_import.py - TrainTab のデータセット取り込み機能を提供する Mixin
"""

import shutil
from pathlib import Path


class TrainImportMixin:
    """WAV + CSV をデータセットへ取り込む Mixin。

    利用側クラスが持つべき属性:
        self.auto_import_var  : BooleanVar
        self.wav_src_var      : StringVar
        self.csv_src_var      : StringVar
    利用側クラスが実装すべきメソッド:
        self._resolve_dataset(root: str) -> str
    """

    # ── テキストファイル読み込み ───────────────────────────────────────────────

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

    # ── WAV + CSV のデータセット取り込み ─────────────────────────────────────

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
