"""
text_normalizer.py - CSVエクスポート時のテキスト変換ルール管理

char_map: {"from_char": "to_char", ...} の辞書。
normalize(text, char_map) で変換を適用する。
ルールは browse_tab の設定UIから追加・削除・編集できる。
"""


def normalize(text: str, char_map: dict) -> str:
    """char_map のルールを順番に適用して返す。"""
    for src, dst in char_map.items():
        text = text.replace(src, dst)
    return text
