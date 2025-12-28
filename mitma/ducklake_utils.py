import duckdb

DUCKLAKE_PATH = "ducklake:/usr/local/airflow/include/my_ducklake.ducklake"
DUCKLAKE_FILE = "/usr/local/airflow/include/my_ducklake.ducklake"

def connect_ducklake():
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute(f"ATTACH '{DUCKLAKE_PATH}' AS mobility_ducklake")
    con.execute("USE mobility_ducklake")
    return con

def close_ducklake(con):
    try:
        con.execute("USE memory")
        con.execute("DETACH mobility_ducklake")
    except:
        pass
    con.close()

def table_exists(con, table_name: str) -> bool:
    result = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_name = '{table_name}'
    """).fetchone()[0]
    return result > 0

