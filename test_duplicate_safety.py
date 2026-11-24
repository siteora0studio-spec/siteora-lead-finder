import tempfile
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from io import BytesIO

from app import (
    Database,
    build_frame,
    build_workbook,
    eligible_outreach_frame,
    find_duplicates,
)


with tempfile.TemporaryDirectory() as td:
    db = Database(str(Path(td) / "duplicate_test.db"))

    # Create two verified businesses with the same normalized name and city.
    with db.conn() as c:
        for key in ("dup:1", "dup:2"):
            c.execute(
                """INSERT INTO businesses
                   (key, name, norm_name, category, city, country,
                    website_status, website_locked, website_lookup_done,
                    score, score_reasons, verification, do_not_contact,
                    created_at, updated_at)
                   VALUES (?, 'Example Barbers', 'example barbers',
                           'Barbershop', 'Sydney', 'Australia',
                           'none', 1, 1, 90, '', 'verified', 0,
                           '2026-01-01', '2026-01-01')""",
                (key,),
            )

        ids = [
            row["id"]
            for row in c.execute(
                "SELECT id FROM businesses WHERE key IN ('dup:1','dup:2') ORDER BY id"
            )
        ]

        for bid in ids:
            c.execute(
                """INSERT INTO emails
                   (business_id, email, confidence, created_at)
                   VALUES (?, ?, 'high', '2026-01-01')""",
                (bid, f"contact{bid}@example.com"),
            )

    first, second = ids

    # Test 1: duplicate detector finds the pair.
    snapshot = db.snapshot()
    pairs = find_duplicates(snapshot)
    assert any(
        {pair["a"], pair["b"]} == {first, second}
        for pair in pairs
    )
    print("PASS: unresolved duplicate pair detected")

    # Test 2: both verified leads are blocked from qualification.
    frame = build_frame(snapshot, require_email=True, min_score=40)
    duplicate_rows = frame[frame["id"].isin(ids)]
    assert len(duplicate_rows) == 2
    assert not duplicate_rows["qualified"].any()
    assert duplicate_rows["flags"].str.contains(
        "Possible duplicate", regex=False
    ).all()
    print("PASS: unresolved verified duplicates cannot qualify")

    # Test 3: unresolved duplicates are excluded from outreach.
    outreach = eligible_outreach_frame(frame)
    assert not outreach["id"].isin(ids).any()
    print("PASS: unresolved duplicates excluded from outreach")

    # Test 4: unresolved duplicates appear in the Excel Review leads sheet.
    workbook_bytes = build_workbook(frame, pd.DataFrame())
    workbook = load_workbook(BytesIO(workbook_bytes), read_only=True)
    review = workbook["Review leads"]
    review_rows = list(review.iter_rows(values_only=True))
    review_names = [row[0] for row in review_rows[1:]]
    assert review_names.count("Example Barbers") == 2
    workbook.close()
    print("PASS: unresolved duplicates included in Excel Review leads")

    # Test 5: dismiss the duplicate pair and rebuild the frame.
    db.dismiss_duplicate(first, second)
    dismissed_snapshot = db.snapshot()
    assert not find_duplicates(dismissed_snapshot)
    dismissed_frame = build_frame(
        dismissed_snapshot, require_email=True, min_score=40
    )
    dismissed_rows = dismissed_frame[dismissed_frame["id"].isin(ids)]
    assert dismissed_rows["qualified"].all()
    assert not dismissed_rows["flags"].str.contains(
        "Possible duplicate", regex=False
    ).any()
    assert len(eligible_outreach_frame(dismissed_frame)) == 2
    print("PASS: dismissed duplicates can qualify and enter outreach")

print("ALL DUPLICATE SAFETY TESTS PASSED")
