# test_mongo.py
from etisalat_invoice import get_mongodb_client

def test_mongo_connection():
    try:
        client = get_mongodb_client()
        print("MongoDB Server Info:", client.server_info())
        print("✅ MongoDB connection successful!")
        return True
    except Exception as e:
        print("❌ MongoDB connection failed:", str(e))
        return False
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    test_mongo_connection()
