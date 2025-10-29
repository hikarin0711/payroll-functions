from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.data.tables import TableServiceClient

class TableRepository:
    def __init__(self, account_name: str, table_name: str):
        if not account_name:
            raise ValueError("STORAGE_ACCOUNT_NAME is empty")
        if not table_name:
            raise ValueError("TABLE_NAME is empty")
        endpoint = f"https://{account_name}.table.core.windows.net"
        cred = DefaultAzureCredential()
        service = TableServiceClient(endpoint=endpoint, credential=cred)
        self._table = service.create_table_if_not_exists(table_name)

    def upsert_payroll(
        self, *,
        user_id: str,
        year: int,
        month: int,
        pay_type: str,
        blob_path: str,
        filename: str,
        total_gross: int,
        total_deduction: int,
        other_payment: int,
        transfer_amount: int,
        status: str = "stub"
    ) -> None:
        entity = {
            "PartitionKey": user_id,
            "RowKey": f"{year:04d}-{month:02d}:{pay_type}",
            "sourceBlobPath": blob_path,
            "filename": filename,
            "ingestedAtUtc": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "totalGross": int(total_gross),
            "totalDeduction": int(total_deduction),
            "otherPayment": int(other_payment),
            "transferAmount": int(transfer_amount)
        }
        self._table.upsert_entity(entity, mode="merge")
