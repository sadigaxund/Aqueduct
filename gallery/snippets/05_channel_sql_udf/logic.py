# logic.py

def mask_email(email: str | None) -> str:
    """Masks an email address for privacy (e.g., 'user@example.com' -> 'u***@example.com')."""
    if not email or "@" not in email:
        return email or ""
    
    prefix, domain = email.split("@", 1)
    if len(prefix) <= 1:
        return f"{prefix}***@{domain}"
    
    return f"{prefix[0]}***@{domain}"
