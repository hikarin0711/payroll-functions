from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.data.tables import TableServiceClient

class TableRepository:
    def __init__(self, account_name: str, table_name: str):
        """
        Initialize the repository for Azure Table Storage.

        This constructor prepares a Table client using DefaultAzureCredential and
        ensures that the target table exists.

        Args:
            account_name (str): Storage account name.
            table_name (str): Table name.

        Raises:
            ValueError: If `account_name` or `table_name` is empty.
            azure.identity.CredentialUnavailableError: No usable credential in the
                DefaultAzureCredential chain.
            azure.core.exceptions.ClientAuthenticationError: Authentication failed.
            azure.core.exceptions.HttpResponseError: Service returned 4xx/5xx during
                table creation or access.
            azure.core.exceptions.ServiceRequestError: Network or DNS failure.

        Notes:
            - Endpoint format: ``https://{account_name}.table.core.windows.net``.
            - Uses DefaultAzureCredential (environment, managed identity, etc.).
            - Creates the table if it does not exist.
        """
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
        """
        Upsert one payroll record into Azure Table Storage.

        Writes a single entity identified by PartitionKey = user_id and
        RowKey = ``"{year:04d}-{month:02d}:{pay_type}"``. Uses MERGE upsert mode:
        non-specified properties are preserved if the entity already exists.

        Args:
            user_id (str): Logical user identifier. Becomes PartitionKey.
            year (int): Payroll year (e.g., 2025).
            month (int): Payroll month (1–12).
            pay_type (str): Kind of payment (e.g., "salary", "bonus").
            blob_path (str): Source blob path for traceability.
            filename (str): Original filename.
            total_gross (int): Gross amount before deductions.
            total_deduction (int): Total deductions.
            other_payment (int): Other additions.
            transfer_amount (int): Net transfer amount.
            status (str): Processing status flag (e.g., "stub", "parsed").

        Returns:
            None

        Raises:
            azure.core.exceptions.ClientAuthenticationError: Authentication failed.
            azure.core.exceptions.ResourceNotFoundError: Table does not exist and
                cannot be created or accessed.
            azure.core.exceptions.HttpResponseError: Service returned 4xx/5xx on upsert.
            azure.core.exceptions.ServiceRequestError: Network or DNS failure.
            ValueError: Provided values cannot be serialized to Table entity types.

        Notes:
            - Upsert mode is ``merge``. To overwrite the whole entity, use ``replace``.
            - Timestamps are written in UTC ISO 8601 for auditing.
            - Integer coercion is applied to amount fields to avoid type drift.
        """
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
