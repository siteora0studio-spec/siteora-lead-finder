import sqlite3
import tempfile
from pathlib import Path

from app import Database

with tempfile.TemporaryDirectory() as td:
    db_path = Path(td) / "test_siteora.db"
    db = Database(str(db_path))

    # Insert a test business into the temporary database.
    with db.conn() as c:
        c.execute(
            """INSERT INTO businesses
               (key, name, norm_name, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("test:1", "Original Business", "original business",
             "2026-01-01", "2026-01-01")
        )

    # Create a valid backup.
    valid_backup = db.backup_bytes()

    # Change the temporary database after taking the backup.
    with db.conn() as c:
        c.execute(
            "UPDATE businesses SET name=? WHERE key=?",
            ("Changed Business", "test:1")
        )

    # Test 1: Valid backup restores the original data.
    db.restore_bytes(valid_backup)
    with db.conn() as c:
        name = c.execute(
            "SELECT name FROM businesses WHERE key='test:1'"
        ).fetchone()[0]
    assert name == "Original Business", f"Unexpected name: {name}"
    print("PASS: valid backup restores original data")

    # Test 2: Invalid file is rejected without changing current data.
    try:
        db.restore_bytes(b"this is not a database")
        raise AssertionError("Invalid file was accepted")
    except ValueError:
        pass

    with db.conn() as c:
        name = c.execute(
            "SELECT name FROM businesses WHERE key='test:1'"
        ).fetchone()[0]
    assert name == "Original Business"
    print("PASS: invalid file rejected; existing data preserved")

    # Test 3: SQLite file with the wrong schema is rejected.
    incomplete_path = Path(td) / "incomplete.db"
    conn = sqlite3.connect(str(incomplete_path))
    conn.execute("CREATE TABLE unrelated (id INTEGER)")
    conn.commit()
    conn.close()

    try:
        db.restore_bytes(incomplete_path.read_bytes())
        raise AssertionError("Incomplete database was accepted")
    except ValueError:
        pass

    with db.conn() as c:
        name = c.execute(
            "SELECT name FROM businesses WHERE key='test:1'"
        ).fetchone()[0]
    assert name == "Original Business"
    print("PASS: incomplete Siteora backup rejected; existing data preserved")

    # Test 4: Corrupt SQLite content is rejected.
    corrupt_backup = bytearray(valid_backup)
    corrupt_backup[-20:] = b"X" * 20

    try:
        db.restore_bytes(bytes(corrupt_backup))
        raise AssertionError("Corrupt database was accepted")
    except ValueError:
        pass

    with db.conn() as c:
        name = c.execute(
            "SELECT name FROM businesses WHERE key='test:1'"
        ).fetchone()[0]
    assert name == "Original Business"
    print("PASS: corrupt backup rejected; existing data preserved")

print("ALL RESTORE TESTS PASSED")
