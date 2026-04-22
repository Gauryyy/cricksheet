import sqlite3

DB_NAME = "rbac.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS roles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        role_name TEXT UNIQUE
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        role_id INTEGER
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS permissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        permission_name TEXT UNIQUE
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS role_permissions (
        role_id INTEGER,
        permission_id INTEGER
    )
    """)

    conn.commit()
    conn.close()

def seed_data():
    conn = get_connection()
    cursor = conn.cursor()

    roles = ["user", "engineer", "admin"]
    for role in roles:
        cursor.execute("INSERT OR IGNORE INTO roles (role_name) VALUES (?)", (role,))

    permissions = ["view_logs", "run_pipeline"]

    for perm in permissions:
        cursor.execute("INSERT OR IGNORE INTO permissions (permission_name) VALUES (?)", (perm,))

    role_permissions = {
        "user": ["view_logs"],
        "engineer": ["view_logs", "run_pipeline"],
        "admin": ["view_logs", "run_pipeline"]
    }

    for role, perms in role_permissions.items():
        cursor.execute("SELECT id FROM roles WHERE role_name=?", (role,))
        role_id = cursor.fetchone()[0]

        for perm in perms:
            cursor.execute("SELECT id FROM permissions WHERE permission_name=?", (perm,))
            perm_id = cursor.fetchone()[0]

            cursor.execute("INSERT OR IGNORE INTO role_permissions VALUES (?, ?)", (role_id, perm_id))

    conn.commit()
    conn.close()