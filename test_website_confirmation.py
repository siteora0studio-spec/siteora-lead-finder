import tempfile
from pathlib import Path
from app import Database

with tempfile.TemporaryDirectory() as td:
    db = Database(str(Path(td) / "website_test.db"))

    with db.conn() as c:
        c.execute(
            """INSERT INTO businesses
               (key, name, norm_name, website_locked, created_at, updated_at)
               VALUES (?, ?, ?, 1, ?, ?)""",
            ("test:1", "Example Business", "example business",
             "2026-01-01", "2026-01-01")
        )
        bid = c.execute(
            "SELECT id FROM businesses WHERE key='test:1'"
        ).fetchone()["id"]

        c.execute(
            """INSERT INTO sources
               (business_id, url, source_type, domain, site_state, created_at)
               VALUES (?, ?, 'website', ?, 'unchecked', ?)""",
            (bid, "https://www.example.com", "example.com", "2026-01-01")
        )

    # Test 1: Matching domain, including www, should be accepted.
    db.confirm_website(bid, "www.example.com")

    with db.conn() as c:
        source = c.execute(
            "SELECT site_state, state_locked FROM sources WHERE business_id=?",
            (bid,)
        ).fetchone()

    assert source["site_state"] == "verified"
    assert source["state_locked"] == 1
    print("PASS: matching website confirmed")

    # Reset source for rejection tests.
    with db.conn() as c:
        c.execute(
            """UPDATE sources SET site_state='unchecked', state_locked=0
               WHERE business_id=?""", (bid,)
        )
        c.execute(
            "UPDATE businesses SET website_locked=1 WHERE id=?", (bid,)
        )

    # Test 2: Unrelated domain must be rejected.
    try:
        db.confirm_website(bid, "unrelated.com")
        raise AssertionError("Unrelated domain was accepted")
    except ValueError:
        pass

    with db.conn() as c:
        source = c.execute(
            "SELECT site_state, state_locked FROM sources WHERE business_id=?",
            (bid,)
        ).fetchone()
        locked = c.execute(
            "SELECT website_locked FROM businesses WHERE id=?", (bid,)
        ).fetchone()[0]

    assert source["site_state"] == "unchecked"
    assert source["state_locked"] == 0
    assert locked == 1
    print("PASS: unrelated domain rejected without changing data")

    # Test 3: Invalid domain must be rejected.
    try:
        db.confirm_website(bid, "not-a-domain")
        raise AssertionError("Invalid domain was accepted")
    except ValueError:
        pass

    print("PASS: invalid domain rejected")

print("ALL WEBSITE CONFIRMATION TESTS PASSED")
