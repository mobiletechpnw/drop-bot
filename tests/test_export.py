"""Unit tests for the web dashboard's Excel export.

These guard the two regressions that made the online "Export to Excel" button
500 on real data:

  * illegal control characters in buyer/item/tracking text making openpyxl's
    ``wb.save()`` raise ``IllegalCharacterError``, and
  * a non-ASCII guild name (emoji, accents) making the ``Content-Disposition``
    header un-encodable as latin-1 when Starlette sends the response.

They exercise the pure builders directly, so no database or HTTP layer is
needed.
"""
import io

import openpyxl

import webapp


def _order(name, user_id, items, total, confirmed=False, tracking=""):
    return {
        "name": name,
        "user_id": user_id,
        "items": items,
        "total": total,
        "confirmed": confirmed,
        "tracking": tracking,
    }


def _rows(data):
    wb = openpyxl.load_workbook(io.BytesIO(data))
    ws = wb.active
    return [[c.value for c in row] for row in ws.iter_rows()]


# ── Workbook building ─────────────────────────────────────────────────────────

def test_build_xlsx_basic_layout():
    orders = [
        _order("Alice", 111, [
            {"display": "Card A", "qty": 2, "subtotal": 20.0},
            {"display": "Card B", "qty": 1, "subtotal": 10.0},
        ], total=30.0, confirmed=True, tracking="TRK1"),
    ]
    rows = _rows(webapp._build_drop_xlsx(12, orders))
    assert rows[0] == ["Buyer", "User ID", "Item", "Qty", "Subtotal",
                       "Order Total", "Paid", "Tracking #"]
    # First item row carries the buyer-level fields, the second leaves them blank.
    assert rows[1][:2] == ["Alice", "111"]
    assert rows[1][2] == "Card A"
    assert rows[1][5] == 30.0 and rows[1][6] == "Yes" and rows[1][7] == "TRK1"
    # Continuation rows leave the buyer-level columns blank (openpyxl reads an
    # empty cell back as None).
    assert not rows[2][0] and not rows[2][1]
    assert rows[2][2] == "Card B"


def test_build_xlsx_handles_empty_drop():
    data = webapp._build_drop_xlsx(1, [])
    rows = _rows(data)
    assert len(rows) == 1  # header only, no crash on empty columns


def test_build_xlsx_strips_illegal_characters():
    # Control characters that are illegal in XML previously raised
    # IllegalCharacterError and 500'd the export.
    orders = [
        _order("Bob\x0b", 222, [
            {"display": "Pikachu \x07 Holo", "qty": 1, "subtotal": 5.0},
        ], total=5.0, tracking="TR\x00K"),
    ]
    rows = _rows(webapp._build_drop_xlsx(3, orders))  # must not raise
    assert rows[1][0] == "Bob"
    assert rows[1][2] == "Pikachu  Holo"
    assert rows[1][7] == "TRK"


# ── Content-Disposition header ────────────────────────────────────────────────

def test_disposition_is_latin1_safe_for_emoji_name():
    header = webapp._attachment_disposition("Sérver_🎉_Drop_12.xlsx")
    # Starlette encodes header values as latin-1; this must not raise.
    header.encode("latin-1")
    assert header.startswith("attachment; filename=")
    assert "filename*=UTF-8''" in header


def test_disposition_ascii_name_passthrough():
    header = webapp._attachment_disposition("MyServer_Drop_5.xlsx")
    assert 'filename="MyServer_Drop_5.xlsx"' in header


def test_disposition_falls_back_when_name_all_nonascii():
    header = webapp._attachment_disposition("🎉🎊🎈")
    header.encode("latin-1")
    assert 'filename="export.xlsx"' in header
