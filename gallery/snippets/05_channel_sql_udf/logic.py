import re


def mask_email(email: str | None) -> str:
    """Masks an email address for privacy (e.g., 'user@example.com' -> 'u***@example.com')."""
    if not email or "@" not in email:
        return email or ""
    
    prefix, domain = email.split("@", 1)
    if len(prefix) <= 1:
        return f"{prefix}***@{domain}"
    
    return f"{prefix[0]}***@{domain}"


def make_phone_masker(visible_digits: int = 4, prefix: str = "+1-***-***-"):
    """Factory: returns a phone-masking UDF with configurable visible digits and prefix.
    
    The `params:` map in the UDF registration is resolved at compile time,
    then this factory is called with those values. The returned closure
    is registered as the actual UDF.
    """
    def mask_phone(phone: str | None) -> str:
        if not phone:
            return ""
        cleaned = re.sub(r'[\s\-\(\)]', '', phone)
        if len(cleaned) <= visible_digits:
            return prefix + cleaned
        return prefix + cleaned[-visible_digits:]
    return mask_phone
