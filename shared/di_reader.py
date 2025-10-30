import os
import time
import requests
from typing import Dict, Any

ENDPOINT = os.getenv("DI_ENDPOINT", "").rstrip("/")
KEY      = os.getenv("DI_KEY", "")
MODEL_ID = os.getenv("DI_MODEL_ID", "")

class DIConfigError(RuntimeError):
    pass

def _check_env():
    if not ENDPOINT or not KEY or not MODEL_ID:
        raise DIConfigError("DI_ENDPOINT/DI_KEY/DI_MODEL_ID が未設定")

def _poll_operation(op_url: str, headers: Dict[str, str]) -> Dict[str, Any]:
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
    入力: PDFのバイト列
    出力: 4項目の整数値 {total_gross, total_deduction, other_payment, transfer_amount}
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
    入力: SAS付きURL
    出力: 4項目の整数値
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
