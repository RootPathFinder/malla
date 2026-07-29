"""Message formatting and length chunking for both meshes."""

from __future__ import annotations


def format_from_meshcore(sender: str, text: str, prefix: str = "[MC]") -> str:
    sender = (sender or "").strip() or "unknown"
    body = (text or "").strip()
    return f"{prefix} {sender}: {body}".strip()


def format_from_meshtastic(sender: str, text: str, prefix: str = "[MT]") -> str:
    sender = (sender or "").strip() or "unknown"
    body = (text or "").strip()
    return f"{prefix} {sender}: {body}".strip()


def chunk_text(text: str, max_chars: int) -> list[str]:
    """Split text into chunks of at most max_chars, with [i/n] markers when needed."""
    if max_chars < 16:
        raise ValueError("max_chars must be >= 16")
    text = text or ""
    if len(text) <= max_chars:
        return [text] if text else []

    # Reserve room for " [12/34]" suffix (up to 8 chars for reasonable sizes)
    marker_budget = 8
    body_limit = max_chars - marker_budget
    if body_limit < 8:
        body_limit = max_chars // 2

    pieces: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= body_limit:
            pieces.append(remaining)
            break
        # Prefer splitting on whitespace near the limit
        split_at = remaining.rfind(" ", 0, body_limit + 1)
        if split_at < body_limit // 2:
            split_at = body_limit
        pieces.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()

    total = len(pieces)
    if total == 1:
        return pieces
    return [f"{part} [{idx}/{total}]" for idx, part in enumerate(pieces, start=1)]
