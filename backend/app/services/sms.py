"""
UniSMS HTTP client.
Docs: https://unismsapi.com/docs/sms
Auth: HTTP Basic — API key as username, empty password.
"""
import base64
import logging
import re

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

# ── Phone number normalisation ────────────────────────────────────────────────

_PH_LOCAL_RE = re.compile(r"^0(9\d{9})$")        # 09XXXXXXXXX
_PH_SHORT_RE = re.compile(r"^(\+?63)(9\d{9})$")  # 639... or +639...


def normalise_ph_number(raw: str) -> str | None:
    """
    Convert any common Philippine number format to E.164 (+639XXXXXXXXX).
    Returns None if the number cannot be normalised.
    """
    digits = re.sub(r"[\s\-\(\)]", "", (raw or "").strip())
    m = _PH_LOCAL_RE.match(digits)
    if m:
        return f"+63{m.group(1)}"
    m = _PH_SHORT_RE.match(digits)
    if m:
        return f"+63{m.group(2)}"
    # Already E.164 with non-PH country code — pass through as-is
    if digits.startswith("+") and len(digits) >= 10:
        return digits
    return None


# ── UniSMS client ─────────────────────────────────────────────────────────────

def _auth_header() -> str:
    """Basic auth: base64(API_KEY:)  — empty password."""
    raw = f"{settings.unisms_api_key}:"
    encoded = base64.b64encode(raw.encode()).decode()
    return f"Basic {encoded}"


def send_sms(phone: str, message: str) -> bool:
    """
    Send a single SMS via UniSMS.
    Returns True on success, False on any failure.
    Never raises — SMS is best-effort; a failed SMS must never crash a request.
    """
    if not settings.unisms_api_key:
        logger.debug("[sms] UNISMS_API_KEY not set — skipping SMS to %s", phone)
        return False

    e164 = normalise_ph_number(phone)
    if not e164:
        logger.warning("[sms] Cannot normalise phone number '%s' — skipping", phone)
        return False

    # Truncate to 160 chars (single SMS segment)
    content = message[:160]

    payload: dict = {"recipient": e164, "content": content}
    if settings.unisms_sender_id:
        payload["sender_id"] = settings.unisms_sender_id

    try:
        with httpx.Client(timeout=15.0) as client:
            response = client.post(
                settings.unisms_base_url,
                headers={
                    "Authorization": _auth_header(),
                    "Content-Type": "application/json",
                },
                json=payload,
            )

        if response.status_code == 201:
            ref = response.json().get("reference_id", "")
            logger.info("[sms] Sent to %s via sender=%s (ref=%s)", e164, settings.unisms_sender_id or "default", ref)
            return True

        logger.warning(
            "[sms] UniSMS rejected message to %s — HTTP %d: %s",
            e164, response.status_code, response.text[:200],
        )
        return False

    except httpx.TimeoutException:
        logger.warning("[sms] Timeout sending to %s", e164)
        return False
    except Exception as exc:
        logger.warning("[sms] Unexpected error sending to %s: %s", e164, exc)
        return False
