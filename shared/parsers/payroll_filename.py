import os
import re
from datetime import datetime, timezone

_PATTERN = re.compile(
    r'^(?P<date>\d{8})_(?P<title>支給明細書|賞与明細書)_(?P<uid>\d+)\.pdf$'
)

def parse_payroll_filename(name: str):
    """
    入力: ファイル名 or パス
    出力: (user_id, year, month, pay_type['salary'|'bonus'])
    """
    base = os.path.basename(name)
    m = _PATTERN.match(base)
    if m:
        d = m.group('date')
        year = int(d[0:4])
        month = int(d[4:6])
        pay_type = 'bonus' if '賞与' in m.group('title') else 'salary'
        user_id = m.group('uid')  # 先頭ゼロ維持
        return user_id, year, month, pay_type
    now = datetime.now(timezone.utc)
    return "unknown", now.year, now.month, "salary"