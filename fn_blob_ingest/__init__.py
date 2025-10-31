import os, logging
import azure.functions as func
from shared.repos.table_repository import TableRepository
from shared.parsers.payroll_filename import parse_payroll_filename
from shared.di_reader import analyze_pay_slip_from_bytes
from shared.validators.payroll_rules import check_transfer_consistency

ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
TABLE_NAME = os.getenv("TABLE_NAME", "PayrollMonthly")

# Create the TableRepository instance
_repo = TableRepository(ACCOUNT_NAME, TABLE_NAME)

def main(myblob: func.InputStream):
    """
    Entry point for the function.
    
    Args:
        myblob (func.InputStream): Input blob stream defined in function.json.
    
    Returns:
        None: This function returns no value.
    """
    try:
        blob_path = myblob.name
        filename = os.path.basename(blob_path)
        user_id, year, month, pay_type = parse_payroll_filename(filename)
        logging.info(f"[ingest] %s -> user=%s %04d-%02d type=%s", blob_path, user_id, year, month, pay_type)

        # PDFバイトをそのままDIへ送信して4値を抽出
        pdf_bytes = myblob.read()
        fields = analyze_pay_slip_from_bytes(pdf_bytes)

        # Field elements validation
        ok, info = check_transfer_consistency(fields)
        if not ok:
            if "error" in info:
                logging.warning("[consistency] invalid number format user=%s %04d-%02d type=%s blob=%s",
                                user_id, year, month, pay_type, blob_path)
            else:
                logging.warning("[consistency] mismatch user=%s %04d-%02d type=%s expected=%s transfer=%s diff=%s blob=%s",
                                user_id, year, month, pay_type,
                                str(info["expected"]), str(info["transfer"]), str(info["diff"]), blob_path)

        # Upsert to Table Strage
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

    # 例外時処理
    except Exception:
        logging.exception("ingest failed")
        raise
