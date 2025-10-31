import os
import time
import requests
from typing import Dict, Any

ENDPOINT = os.getenv("DI_ENDPOINT", "").rstrip("/")
KEY      = os.getenv("DI_KEY", "")
MODEL_ID = os.getenv("DI_MODEL_ID", "")

class DIConfigError(RuntimeError):
    """
    Configuration error for Document Intelligence settings.

    Raised when one or more mandatory environment variables are missing or blank. 
    The required variables are: ``DI_ENDPOINT``, ``DI_KEY``, and ``DI_MODEL_ID``.
    """
    pass

def _check_env():
    """
    Validate required Document Intelligence environment variables.

    Ensures that ``DI_ENDPOINT``, ``DI_KEY``, and ``DI_MODEL_ID`` are present.

    Raises:
        DIConfigError: If any required variable is missing or empty.
    """
    if not ENDPOINT or not KEY or not MODEL_ID:
        raise DIConfigError("DI_ENDPOINT/DI_KEY/DI_MODEL_ID が未設定")

def _poll_operation(op_url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """
    Poll a Document Intelligence analyze operation until completion.

    Sends GET requests to the given operation URL until the job succeeds,
    fails, or is canceled. Honors the ``Retry-After`` response header when
    present.

    Args:
        op_url (str): Operation URL returned in the ``Operation-Location`` header.
        headers (dict): HTTP headers.

    Returns:
        dict: The ``analyzeResult`` object on success.

    Raises:
        requests.HTTPError: If the polling request returns a 4xx/5xx status.
        RuntimeError: If the operation status is ``failed`` or ``canceled``.
        ValueError: If the response body is not a valid JSON object.
    """
    while True:
        r = requests.get(op_url, headers=headers, timeout=30)
        r.raise_for_status()
        body = r.json()
        st = body.get("status")
        if st in ("succeeded", "completed"):
            return body["analyzeResult"]
        if st in ("failed", "canceled"):
            raise RuntimeError(f"Document Intelligence analyze failed: {body}")
        retry = r.headers.get("Retry-After")
        time.sleep(int(retry) if retry and retry.isdigit() else 2)

def _num_from_field(f: Dict[str, Any]) -> int:
    """
    Extract an integer amount from a Document Intelligence field object.

    The function tries, in order:
      1) ``valueNumber``,
      2) ``valueCurrency.amount``,
      3) sanitized ``content`` (commas removed, full-width digits normalized).

    If extraction fails, returns ``0``.

    Args:
        f (Dict[str, Any]): Field object from the model output.

    Returns:
        int: Parsed whole amount (e.g., JPY) as an integer; ``0`` on failure.
    """
    if not f:
        return 0
    if isinstance(f.get("valueNumber"), (int, float)):
        return int(f["valueNumber"])
    vc = f.get("valueCurrency")
    if isinstance(vc, dict) and isinstance(vc.get("amount"), (int, float)):
        return int(vc["amount"])
    content = f.get("content")
    if isinstance(content, str):
        s = content.strip().replace(",", "")
        # 全角→半角
        s = s.translate(str.maketrans("０１２３４５６７８９－", "0123456789-"))
        try:
            return int(float(s))
        except Exception:
            return 0
    return 0

def analyze_pay_slip_from_bytes(pdf_bytes: bytes, content_type: str = "application/pdf") -> Dict[str, int]:
    """
    Analyze payroll fields from in-memory PDF bytes using a custom DI model.

    Submits the raw PDF bytes to the Document Intelligence Analyze endpoint and
    polls the operation until completion. The result is normalized to a dict of
    whole-number amounts.

    Args:
        pdf_bytes (bytes): Raw PDF payload to analyze.
        content_type (str): MIME type of the payload. Defaults to ``application/pdf``.

    Returns:
        Dict[str, int]: A mapping with keys:
            - ``total_gross`` (int)
            - ``total_deduction`` (int)
            - ``other_payment`` (int)
            - ``transfer_amount`` (int)

    Raises:
        DIConfigError: Missing endpoint, key, or model ID configuration.
        requests.HTTPError: Non-2xx response from the service.
        KeyError: Missing ``Operation-Location`` header in the initial response.
        RuntimeError: Analyze operation failed or was canceled.
        requests.RequestException: Network or timeout error.
    """
    _check_env()
    url = f"{ENDPOINT}/documentintelligence/documentModels/{MODEL_ID}:analyze"
    params = {"api-version": "2024-11-30"}
    headers = {
        "Ocp-Apim-Subscription-Key": KEY,
        "Content-Type": content_type,
    }
    # 解析開始（バイト送信）
    resp = requests.post(url, params=params, headers=headers, data=pdf_bytes, timeout=60)
    resp.raise_for_status()
    op_url = resp.headers["Operation-Location"]
    result = _poll_operation(op_url, {"Ocp-Apim-Subscription-Key": KEY})

    # 学習済みカスタムの fields から抽出
    fields = (result.get("documents") or [{}])[0].get("fields") or {}
    return {
        "total_gross":     _num_from_field(fields.get("total_gross")),
        "total_deduction": _num_from_field(fields.get("total_deduction")),
        "other_payment":   _num_from_field(fields.get("other_payment")),
        "transfer_amount": _num_from_field(fields.get("transfer_amount")),
    }

def analyze_pay_slip_from_url(sas_url: str) -> Dict[str, int]:
    """
    Analyze payroll fields from a blob via SAS URL.

    Sends the source URL (with SAS) to the Analyze endpoint and polls until the
    operation completes. The result is normalized to whole-number amounts.

    Args:
        sas_url (str): Publicly accessible URL with SAS token to the PDF blob.

    Returns:
        Dict[str, int]: A mapping with keys:
            - ``total_gross`` (int)
            - ``total_deduction`` (int)
            - ``other_payment`` (int)
            - ``transfer_amount`` (int)

    Raises:
        DIConfigError: Missing endpoint, key, or model ID configuration.
        requests.HTTPError: Non-2xx response from the service.
        KeyError: Missing ``Operation-Location`` header in the initial response.
        RuntimeError: Analyze operation failed or was canceled.
        requests.RequestException: Network or timeout error.
    """
    _check_env()
    url = f"{ENDPOINT}/documentintelligence/documentModels/{MODEL_ID}:analyze"
    params = {"api-version": "2024-11-30"}
    headers = {"Ocp-Apim-Subscription-Key": KEY, "Content-Type": "application/json"}
    resp = requests.post(url, params=params, headers=headers, json={"urlSource": sas_url}, timeout=30)
    resp.raise_for_status()
    op_url = resp.headers["Operation-Location"]
    result = _poll_operation(op_url, {"Ocp-Apim-Subscription-Key": KEY})

    fields = (result.get("documents") or [{}])[0].get("fields") or {}
    return {
        "total_gross":     _num_from_field(fields.get("total_gross")),
        "total_deduction": _num_from_field(fields.get("total_deduction")),
        "other_payment":   _num_from_field(fields.get("other_payment")),
        "transfer_amount": _num_from_field(fields.get("transfer_amount")),
    }
