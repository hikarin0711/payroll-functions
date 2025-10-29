import os, re, logging
from datetime import datetime, timezone
import azure.functions as func
from shared.repos.table_repository import TableRepository

ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
TABLE_NAME = os.getenv("TABLE_NAME", "PayrollMonthly")
_repo = TableRepository(ACCOUNT_NAME, TABLE_NAME)

# 例:
#  - 20251010_支給明細書_0121.pdf  -> pay_type=salary
#  - 20250331_賞与明細書_0121.pdf  -> pay_type=bonus
def _parse_filename(name: str):
    base = os.path.basename(name)
    m = re.match(r'^(?P<date>\d{8})_(?P<title>支給明細書|賞与明細書)_(?P<uid>\d+)\.pdf$', base)
    if m:
        d = m.group('date')
        year = int(d[0:4])
        month = int(d[4:6])
        pay_type = 'bonus' if '賞与' in m.group('title') else 'salary'
        user_id = m.group('uid')  # 先頭ゼロを保持
        return user_id, year, month, pay_type
    # フォールバック（想定外名）
    now = datetime.now(timezone.utc)
    return "unknown", now.year, now.month, "salary"

def main(myblob: func.InputStream):
    try:
        blob_path = myblob.name
        filename = os.path.basename(blob_path)
        user_id, year, month, pay_type = _parse_filename(filename)
        logging.info(f"[ingest] %s -> user=%s %04d-%02d type=%s", blob_path, user_id, year, month, pay_type)

        _repo.upsert_payroll(
            user_id=user_id,
            year=year,
            month=month,
            pay_type=pay_type,
            blob_path=blob_path,
            filename=filename,
            total_gross=0,
            total_deduction=0,
            other_payment=0,
            transfer_amount=0,
            status="stub"
        )
    except Exception:
        logging.exception("ingest failed")
        raise
