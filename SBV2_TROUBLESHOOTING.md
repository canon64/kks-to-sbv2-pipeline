# Style-Bert-VITS2 インストール トラブルシューティング

## Install-Style-Bert-VITS2.bat が途中で止まる

### 症状
```
fatal: destination path 'Style-Bert-VITS2' already exists and is not an empty directory.
Press any key to continue . . .
```

`Style-Bert-VITS2` フォルダが既に存在する場合、git clone が失敗して bat が終了する。

### 対処：手動でインストールを完了させる

**Step 0: requirements.txt を修正する**

`Style-Bert-VITS2\requirements.txt` をメモ帳で開き、以下の行を書き換えて保存する。

```
変更前: faster-whisper==0.10.1
変更後: faster-whisper
```

**Step 1: コマンドプロンプトを開く**

`Install-Style-Bert-VITS2.bat` があるフォルダ（sbv2フォルダ）でコマンドプロンプトを開く。

```
cd Style-Bert-VITS2
```

**Step 2: PyTorch をインストールする**

```
venv\Scripts\python -m uv pip install "torch<2.4" "torchaudio<2.4" --index-url https://download.pytorch.org/whl/cu118
```

**Step 3: その他の依存パッケージをインストールする**

```
venv\Scripts\python -m uv pip install -r requirements.txt
```

**Step 4: setuptools を安定バージョンに固定する**

```
venv\Scripts\pip install "setuptools==69.5.1" --force-reinstall
```

**Step 5: transformers を互換バージョンに下げる**

```
venv\Scripts\python -m uv pip install "transformers==4.44.2"
```

**Step 6: soxr をインストールする**

```
venv\Scripts\python -m uv pip install soxr
```

**Step 7: モデルをダウンロードする**

```
venv\Scripts\python initialize.py
```

**Step 8: 動作確認**

```
venv\Scripts\python app.py
```

ブラウザが開いて UI が表示されれば成功。

---

## よくあるエラーと対処

### `No module named 'pkg_resources'`
setuptools のバージョン問題。Step 4 を実行する。

### `PyTorch >= 2.4 is required` / `torch.load` 脆弱性エラー
transformers と PyTorch のバージョン非互換。Step 5 を実行する。

### `No module named 'soxr'`
soxr が未インストール。Step 6 を実行する。

### `サーバーに接続できませんでした`（pyopenjtalk）
setuptools のバージョン問題が原因のことが多い。Step 4 を実行する。
