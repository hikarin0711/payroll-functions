from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

def check_transfer_consistency(fields) -> tuple[bool, dict]:
    """
    Validate the extracted field elements.
    
    Args:
        fields(dict): Fields extracted by Azure Document Intelligence.
    
    Returns:
        tuple[bool, dict]: (is_valid, fields_dict)
    """
    to_dec = lambda v: Decimal(str(0 if v is None else v)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    try:
        tg, td, op, tr = map(
            to_dec,
            (fields.get("total_gross", 0),
             fields.get("total_deduction", 0),
             fields.get("other_payment", 0),
             fields.get("transfer_amount", 0))
        )
    except InvalidOperation as e:
        # 数値化不能（例: "1,2a" など）
        return False, {"error": "invalid_number_format", "detail": str(e)}
    
    expected = tg - td + op
    ok = (expected == tr)
    return ok, {"expected": expected, "transfer": tr, "diff": expected - tr}