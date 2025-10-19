import uuid
import hashlib
import unicodedata
from datetime import datetime


def generate_uuid() -> str:
    """Gera UUIDv4 (pode ser trocado futuramente por UUIDv7)"""
    return str(uuid.uuid4())


def normalize_text(text: str) -> str:
    """Remove acentos, espaços duplos e caracteres especiais"""
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text.strip().lower().replace(" ", "_")


def generate_record_key(
    period: str, date: str, description: str, amount: float, vendor: str = ""
) -> str:
    """Gera hash determinístico SHA256 para garantir idempotência"""
    desc = normalize_text(description)
    vendor_code = normalize_text(vendor)
    base = f"{period}|{date}|{desc}|{amount:.2f}|{vendor_code}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def utc_now() -> str:
    """Retorna o horário UTC atual em formato ISO"""
    return datetime.utcnow().isoformat()
