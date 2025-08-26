import sqlite3
import argparse
from datetime import datetime

DB_FILE = "cloud_data.db"

def normalize(value: str) -> str:
    # Trim spaces and make case-insensitive for duplicate checks
    return " ".join(value.split()).lower()

def connect():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn

def init_db(conn, reset: bool = False):
    c = conn.cursor()
    if reset:
        c.execute("DROP TABLE IF EXISTS data")

    c.execute("""
    CREATE TABLE IF NOT EXISTS data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        value TEXT NOT NULL,
        value_norm TEXT NOT NULL UNIQUE,
        created_at TEXT DEFAULT (datetime('now'))
    )
    """)
    conn.commit()

def classify(conn, raw_value: str) -> str:
    """Return 'false_positive' | 'redundant' | 'unique'."""
    if raw_value is None:
        return "false_positive"
    v = raw_value.strip()
    if v == "":
        return "false_positive"

    v_norm = normalize(v)
    c = conn.cursor()
    c.execute("SELECT 1 FROM data WHERE value_norm = ? LIMIT 1", (v_norm,))
    exists = c.fetchone() is not None
    return "redundant" if exists else "unique"

def insert_unique(conn, raw_value: str):
    cls = classify(conn, raw_value)
    if cls == "false_positive":
        print("⚠️ False positive ignored (empty/invalid input)")
        return

    if cls == "redundant":
        print(f"❌ Redundant entry ignored: {raw_value}")
        return

    v = raw_value.strip()
    v_norm = normalize(v)
    try:
        conn.execute(
            "INSERT INTO data (value, value_norm, created_at) VALUES (?, ?, ?)",
            (v, v_norm, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
        print(f"✅ Inserted: {v}")
    except sqlite3.IntegrityError:
        # Race-condition safe guard; treat as redundant if uniqueness blocked
        print(f"❌ Redundant entry ignored: {raw_value}")

def show_data(conn):
    rows = conn.execute("SELECT id, value, created_at FROM data ORDER BY id").fetchall()
    if not rows:
        print("\n📂 Database is empty.")
        return
    print("\n📂 Current Database Entries:")
    for r in rows:
        print(f"({r[0]}, '{r[1]}', {r[2]})")

def main():
    parser = argparse.ArgumentParser(description="Data Redundancy Removal System")
    parser.add_argument("--reset", action="store_true", help="Drop and recreate the table (start fresh)")
    args = parser.parse_args()

    conn = connect()
    init_db(conn, reset=args.reset)

    print("📌 Data Redundancy Removal System")
    print("Type a value to insert, or commands: 'show', 'reset', 'exit'")

    while True:
        user_input = input("Enter data: ").strip()

        if user_input.lower() == "exit":
            break
        if user_input.lower() == "show":
            show_data(conn)
            continue
        if user_input.lower() == "reset":
            init_db(conn, reset=True)
            print("🔄 Database reset. (Table dropped & recreated)")
            show_data(conn)
            continue

        insert_unique(conn, user_input)
        show_data(conn)

    conn.close()

if __name__ == "__main__":
    main()



