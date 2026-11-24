import os
import tempfile
from pathlib import Path

import app

with tempfile.TemporaryDirectory(prefix="siteora_merge_test_") as td:
    db_path = str(Path(td) / "merge_test.db")
    db = app.Database(db_path)

    # Create two fake businesses.
    with db.conn() as c:
        for key, name in [
            ("name:test-primary", "Test Primary"),
            ("name:test-secondary", "Test Secondary"),
        ]:
            c.execute(
                """INSERT INTO businesses
                   (key, name, norm_name, city, country, created_at, updated_at,
                    do_not_contact)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (key, name, name.lower(), "Test City", "Test Country",
                 app.now_iso(), app.now_iso(), 0)
            )

        ids = {
            r["name"]: r["id"]
            for r in c.execute(
                "SELECT id, name FROM businesses WHERE name LIKE 'Test %'"
            )
        }
        primary = ids["Test Primary"]
        secondary = ids["Test Secondary"]

        # Same email on both businesses: secondary is manually confirmed.
        c.execute(
            """INSERT INTO emails
               (business_id,email,kind,confidence,source_url,confirmed,created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (primary, "same@example.test", "snippet_other", "low",
             "https://primary.example.test", 0, app.now_iso())
        )
        c.execute(
            """INSERT INTO emails
               (business_id,email,kind,confidence,source_url,confirmed,created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (secondary, "same@example.test", "site_page", "manual",
             "https://secondary.example.test", 1, app.now_iso())
        )
        c.execute(
            """INSERT INTO emails
               (business_id,email,kind,confidence,source_url,created_at)
               VALUES (?,?,?,?,?,?)""",
            (secondary, "other@example.test", "site_page", "high",
             "https://secondary.example.test", app.now_iso())
        )

        # Duplicate and distinct phones.
        for bid, phone, confidence in [
            (primary, "111-111-111", "low"),
            (secondary, "111-111-111", "high"),
            (secondary, "222-222-222", "medium"),
        ]:
            c.execute(
                """INSERT INTO phones
                   (business_id,phone,confidence,source_url,created_at)
                   VALUES (?,?,?,?,?)""",
                (bid, phone, confidence, "https://example.test", app.now_iso())
            )

        # Duplicate and distinct sources.
        for bid, url, title in [
            (primary, "https://example.test/page", "Primary title"),
            (secondary, "https://example.test/page", "Secondary title"),
            (secondary, "https://example.test/other", "Other page"),
        ]:
            c.execute(
                """INSERT INTO sources
                   (business_id,url,source_type,title,created_at)
                   VALUES (?,?,?,?,?)""",
                (bid, url, "directory", title, app.now_iso())
            )

        # Duplicate and distinct social links.
        for bid, url, platform in [
            (primary, "https://social.example.test/account", "social"),
            (secondary, "https://social.example.test/account", "social"),
            (secondary, "https://social.example.test/other", "social"),
        ]:
            c.execute(
                """INSERT INTO socials
                   (business_id,platform,url,source_url,created_at)
                   VALUES (?,?,?,?,?)""",
                (bid, platform, url, "https://example.test", app.now_iso())
            )

        # History must be transferred.
        c.execute(
            """INSERT INTO verification_log
               (business_id,created_at,check_type,result,detail)
               VALUES (?,?,?,?,?)""",
            (secondary, app.now_iso(), "test", "passed", "test history")
        )
        c.execute(
            """INSERT INTO outreach_log
               (business_id,created_at,to_status,note)
               VALUES (?,?,?,?)""",
            (secondary, app.now_iso(), "Contacted", "test outreach")
        )

        # Dismissal relationship should be cleaned up.
        c.execute(
            "INSERT INTO dup_dismissed(a,b) VALUES(?,?)",
            (min(primary, secondary), max(primary, secondary))
        )

    # Merge the fake secondary into the fake primary.
    db.merge(primary, secondary)

    with db.conn() as c:
        assert c.execute(
            "SELECT COUNT(*) FROM businesses WHERE id=?", (secondary,)
        ).fetchone()[0] == 0, "Secondary business was not removed"

        assert c.execute(
            "SELECT COUNT(*) FROM businesses WHERE id=?", (primary,)
        ).fetchone()[0] == 1, "Primary business missing"

        email = c.execute(
            "SELECT * FROM emails WHERE business_id=? AND email=?",
            (primary, "same@example.test")
        ).fetchone()
        assert email is not None
        assert email["confidence"] == "manual", "Manual confidence was not preserved"
        assert email["confirmed"] == 1, "Manual confirmation was not preserved"

        assert c.execute(
            "SELECT COUNT(*) FROM emails WHERE business_id=?", (primary,)
        ).fetchone()[0] == 2, "Email records were lost"

        assert c.execute(
            "SELECT COUNT(*) FROM phones WHERE business_id=?", (primary,)
        ).fetchone()[0] == 2, "Phone records were lost"

        phone = c.execute(
            "SELECT confidence FROM phones WHERE business_id=? AND phone=?",
            (primary, "111-111-111")
        ).fetchone()
        assert phone["confidence"] == "high", "Best phone confidence was not kept"

        assert c.execute(
            "SELECT COUNT(*) FROM sources WHERE business_id=?", (primary,)
        ).fetchone()[0] == 2, "Source records were lost"

        assert c.execute(
            "SELECT COUNT(*) FROM socials WHERE business_id=?", (primary,)
        ).fetchone()[0] == 2, "Social records were lost"

        assert c.execute(
            "SELECT COUNT(*) FROM verification_log WHERE business_id=?", (primary,)
        ).fetchone()[0] == 2, "Verification history was not transferred"

        assert c.execute(
            "SELECT COUNT(*) FROM outreach_log WHERE business_id=?", (primary,)
        ).fetchone()[0] == 1, "Outreach history was not transferred"

        assert c.execute(
            "SELECT COUNT(*) FROM dup_dismissed WHERE a=? OR b=?",
            (primary, primary)
        ).fetchone()[0] == 0, "Duplicate dismissal was not cleaned up"

    # Confirm transaction rollback on an intentional exception.
    try:
        with db.conn() as c:
            c.execute(
                "UPDATE businesses SET notes=? WHERE id=?",
                ("This must roll back", primary)
            )
            raise RuntimeError("intentional rollback test")
    except RuntimeError:
        pass

    with db.conn() as c:
        notes = c.execute(
            "SELECT notes FROM businesses WHERE id=?", (primary,)
        ).fetchone()["notes"]
        assert notes != "This must roll back", "Transaction rollback failed"

    print("PASS: duplicate email consolidation")
    print("PASS: manual email confirmation and confidence preserved")
    print("PASS: distinct emails and phones retained")
    print("PASS: strongest phone confidence retained")
    print("PASS: source URLs and social links retained")
    print("PASS: verification and outreach history transferred")
    print("PASS: duplicate business removed")
    print("PASS: transaction rollback")
    print("ALL MERGE TESTS PASSED")
