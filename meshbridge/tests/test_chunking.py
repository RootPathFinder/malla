from meshbridge.chunking import chunk_text, format_from_meshcore, format_from_meshtastic


def test_format_prefixes():
    assert format_from_meshcore("Alice", "hi") == "[MC] Alice: hi"
    assert format_from_meshtastic("!abcd", "yo", "[MT]") == "[MT] !abcd: yo"


def test_chunk_short_unchanged():
    assert chunk_text("hello", 133) == ["hello"]


def test_chunk_long_splits_with_markers():
    text = ("word " * 40).strip()
    chunks = chunk_text(text, 60)
    assert len(chunks) > 1
    assert all(len(c) <= 60 for c in chunks)
    assert "[1/" in chunks[0]
    assert f"[{len(chunks)}/{len(chunks)}]" in chunks[-1]


def test_chunk_empty():
    assert chunk_text("", 133) == []
