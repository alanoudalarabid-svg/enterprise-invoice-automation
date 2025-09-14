# test_mysql.py
from etisalat_invoice import get_mysql_connection
from mysql.connector import Error

def test_mysql_connection():
    conn = None
    try:
        conn = get_mysql_connection()
        print("MySQL Server Info:", conn.get_server_info())
        print("✅ MySQL connection successful!")
        return True
    except Error as e:
        print("❌ MySQL connection failed:", str(e))
        return False
    finally:
        if conn and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    test_mysql_connection()
