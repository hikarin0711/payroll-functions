import os, logging
import azure.functions as func
from shared.repos.table_repository import TableRepository
from shared.parsers.payroll_filename import parse_payroll_filename
from shared.di_reader import analyze_pay_slip_from_bytes

ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
TABLE_NAME = os.getenv("TABLE_NAME", "PayrollMonthly")
_repo = TableRepository(ACCOUNT_NAME, TABLE_NAME)

def main(myblob: func.InputStream):
    try:
        blob_path = myblob.name
        filename = os.path.basename(blob_path)
        user_id, year, month, pay_type = parse_payroll_filename(filename)
        logging.info(f"[ingest] %s -> user=%s %04d-%02d type=%s", blob_path, user_id, year, month, pay_type)

        # PDFバイトをそのままDIへ送信して4値を抽出
        pdf_bytes = myblob.read()
        fields = analyze_pay_slip_from_bytes(pdf_bytes)
        _repo.upsert_payroll(
            user_id=user_id,
            year=year,
            month=month,
            pay_type=pay_type,
            blob_path=blob_path,
            filename=filename,
            total_gross=fields.get("total_gross", 0),
            total_deduction=fields.get("total_deduction", 0),
            other_payment=fields.get("other_payment", 0),
            transfer_amount=fields.get("transfer_amount", 0),
            status="parsed"
        )
    except Exception:
        logging.exception("ingest failed")
        raise
