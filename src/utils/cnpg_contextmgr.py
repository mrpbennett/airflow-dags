from contextlib import contextmanager

import psycopg2


@contextmanager
def query_db(query=None):
    """
    Context manager to provide a database connection or execute a query.
    Usage:
        with query_db() as conn:
            # use conn
        with query_db(query="SELECT * FROM faker_users LIMIT 2") as cur:
            users = cur.fetchall()
        with query_db(query="INSERT INTO ...") as cur:
            pass  # DML queries
    """
    conn = psycopg2.connect(
        host="192.168.7.80", database="main", user="paul", password="password"
    )
    try:
        if query is not None:
            cur = conn.cursor()
            try:
                cur.execute(query)
                if cur.description is None:  # Not a SELECT query
                    conn.commit()
                yield cur
            finally:
                cur.close()
        else:
            yield conn
    finally:
        conn.close()
