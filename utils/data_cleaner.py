def clean_value(value):
    if isinstance(value, dict):
        return value.get("display_value", "")
    return str(value) if value else None
