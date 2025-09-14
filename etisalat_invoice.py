# etisalat_invoice.py
import pdfplumber
import re
from datetime import datetime
from pymongo import MongoClient
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
from typing import Dict, Optional
import time
import logging
from pymongo.write_concern import WriteConcern

# Load environment variables
load_dotenv()
MYSQL_PWD = os.getenv("MySQL_DB_PASSWORD")
MONGODB_PWD = os.getenv("MongoDB_DB_PASSWORD")

# Database connections setup
def get_mysql_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password=MYSQL_PWD,
        database='etisalat_e_invoices',
        autocommit=False,
        pool_reset_session=True
    )

def get_mongodb_client():
    return MongoClient(f"mongodb+srv://invoicestorage:{MONGODB_PWD}@cluster0.he3xpg6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

# PDF Processing Functions
def extract_invoice_data(pdf_path: str) -> Optional[Dict]:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())
        
        data = extract_core_fields(text)
        tables = extract_call_tables(text)
        
        if not data or not tables:
            return None

        return {
            "metadata": {
                "pdf_name": os.path.basename(pdf_path),
                "processing_date": datetime.utcnow()
            },
            "invoice_data": data,
            "usage_data": tables
        }
    except Exception as e:
        print(f"Error processing {pdf_path}: {str(e)}")
        return None

def extract_core_fields(text: str) -> Dict:
    data = {}
    
    # Account Number
    account_match = re.search(r"Account Number[:\s-]*(\d{3}\s?-\s?\d{7})", text)
    if account_match:
        data["account_number"] = account_match.group(1).replace(" ", "").replace("-", "")

    # Bill Period
    bill_match = re.search(
        r"Bill period[\s:-]*(\d{1,2} [A-Za-z]{3} \d{4}.*?\d{1,2} [A-Za-z]{3} \d{4})",
        text, re.DOTALL
    )
    if bill_match:
        data["bill_period"] = " ".join(bill_match.group(1).split())

    # Current Charges
    charges_match = re.search(
        r"Current month charges \(including VAT\)\D*(\d{1,3}(?:,\d{3})*\.\d+)",
        text
    )
    data["current_charges"] = float(charges_match.group(1).replace(",", "")) if charges_match else None

    # Total Due
    total_due_match = re.search(
        r"Total Amount Due\D*(\d{1,3}(?:,\d{3})*\.\s*\d+)",
        text, re.DOTALL
    )
    if total_due_match:
        total_due_str = total_due_match.group(1).replace("\n", "").replace(" ", "").replace(",", "")
        data["total_due"] = float(total_due_str)

    return data

def extract_call_tables(text: str) -> Dict:
    """Extracts the three call type tables from the invoice text"""
    # Find the National Calls And Usages section
    national_calls_section = re.search(
        r"National Calls And Usages.*?(?=C O N V E N I E N T W A Y S T O P A Y)",
        text,
        re.DOTALL
    )

    if not national_calls_section:
        return None

    section_text = national_calls_section.group(0)

    # Initialize the three tables we want to extract
    tables = {
        "Calls to Mobile": {"summary": None, "records": []},
        "Calls to Special Number": {"summary": None, "records": []},
        "Calls To Telephone": {"summary": None, "records": []}
    }

    # Extract the summary lines for each call type
    summary_pattern = r"(?i)(Calls to (?:Mobile|Special Number|Telephone))\s+([\d:]+)\s+([\d.]+)"
    summaries = re.findall(summary_pattern, section_text)

    for category, duration, amount in summaries:
        if category in tables:
            tables[category]["summary"] = {
                "total_duration": duration,
                "total_amount": float(amount)
            }

    # Extract all call records
    record_pattern = r"(\d{1,2} [A-Za-z]{3} \d{4})\s+(\d{2}:\d{2}:\d{2})\s+[ÌÍ]?(\d+)[ÌÍ]?\s+(\d{2}:\d{2}:\d{2})\s+([\d.]+)"
    records = re.findall(record_pattern, section_text)

    # Define a list of UAE mobile prefixes
    mobile_prefixes = ["050", "052", "054", "055", "056", "058"]

    # Categorize each record
    for record in records:
        date, time, to_number, duration, amount = record
        amount_float = float(amount)

        # Check for mobile prefix
        is_mobile = any(to_number.startswith(prefix) for prefix in mobile_prefixes)

        if amount_float > 0:
            category = "Calls to Special Number"
        elif is_mobile:
            category = "Calls to Mobile"
        else:
            category = "Calls To Telephone"

        tables[category]["records"].append({
            "date": date,
            "time": time,
            "to_number": to_number,
            "duration": duration,
            "amount": amount_float
        })

    return tables

# Database Operations
def save_to_mysql(invoice_data: Dict, proc_time: float = None, cursor=None) -> Optional[int]:
    """Save invoice data to MySQL database including processing time"""
    conn = None
    close_connection = False
    
    try:
        if cursor is None:
            conn = get_mysql_connection()
            cursor = conn.cursor(buffered=True)
            close_connection = True
        
        # Insert invoice with processing time
        invoice_query = """
            INSERT INTO invoices 
            (account_number, bill_period, current_charges, total_due, pdf_name, processed_at, processing_time_seconds) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
        cursor.execute(invoice_query, (
            invoice_data['invoice_data']['account_number'],
            invoice_data['invoice_data']['bill_period'],
            invoice_data['invoice_data']['current_charges'],
            invoice_data['invoice_data']['total_due'],
            invoice_data['metadata']['pdf_name'],
            datetime.now(),
            proc_time
        ))
        
        invoice_id = cursor.lastrowid
        
        # Insert usage details
        usage_sql = """
            INSERT INTO usage_details 
            (invoice_id, category, date, time, to_number, duration, amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
        
        for category, details in invoice_data['usage_data'].items():
            for record in details['records']:
                cursor.execute(usage_sql, (
                    invoice_id,
                    category,
                    datetime.strptime(record['date'], '%d %b %Y').date(),
                    record['time'],
                    record['to_number'],
                    record['duration'],
                    record['amount']
                ))
        
        if close_connection:
            conn.commit()
        return invoice_id
        
    except Error as e:
        logging.error(f"MySQL Error: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return None
    finally:
        if close_connection and conn and conn.is_connected():
            cursor.close()
            conn.close()

def save_to_mongodb(invoice_data: Dict, client=None):
    """Save invoice data to MongoDB with optional existing client"""
    close_client = False
    try:
        if client is None:
            client = get_mongodb_client()
            close_client = True
        
        db = client["invoices_db"].with_options(
            write_concern=WriteConcern(w=1, j=True))
        collection = db["etisalat_invoices"]
        
        processing_date = invoice_data["metadata"]["processing_date"]
        if isinstance(processing_date, str):
            processing_date = datetime.fromisoformat(processing_date)

        document = {
            "_id": invoice_data["metadata"]["pdf_name"],
            "metadata": {
                "pdf_name": invoice_data["metadata"]["pdf_name"],
                "processing_date": processing_date
            },
            "invoice_data": invoice_data["invoice_data"],
            "usage_data": invoice_data["usage_data"]
        }
        
        result = collection.update_one(
            {"_id": document["_id"]},
            {"$set": document},
            upsert=True
        )
        
        if not result.acknowledged:
            logging.error(f"MongoDB write unacknowledged for {document['_id']}")
            return None

        return document["_id"]
    
    except Exception as e:
        logging.error(f"MongoDB Error: {str(e)}", exc_info=True)
        return None
    finally:
        if close_client and client:
            client.close()

def process_single_invoice(pdf_path: str, start_time: float = None) -> bool:
    """
    Processes a single invoice with atomic transaction handling.
    Returns True only if both MySQL and MongoDB operations succeed.
    """
    
    mysql_conn = None
    mongo_client = None
    
    try:
        # Data Extraction
        extracted_data = extract_invoice_data(pdf_path)
        if not extracted_data:
            logging.error(f"Data extraction failed for {os.path.basename(pdf_path)}")
            return False
        
        processing_time = time.time() - start_time if start_time else None
        extracted_data['processing_time'] = processing_time
        
        # MySQL Transaction
        mysql_conn = get_mysql_connection()
        mysql_conn.start_transaction()
        
        # Save to MySQL
        mysql_id = save_to_mysql(extracted_data, processing_time, cursor=mysql_conn.cursor())
        if not mysql_id:
            mysql_conn.rollback()
            logging.error(f"MySQL insertion failed for {os.path.basename(pdf_path)}")
            return False
        
        # Save to MongoDB
        mongo_client = get_mongodb_client()
        mongo_id = save_to_mongodb(extracted_data, client=mongo_client)
        if not mongo_id:
            mysql_conn.rollback()
            logging.error(f"MongoDB failed after MySQL success for {os.path.basename(pdf_path)}")
            return False
        
        # Final commit
        mysql_conn.commit()
        logging.info(
            f"Successfully processed {os.path.basename(pdf_path)} "
            f"(MySQL ID: {mysql_id}, MongoDB ID: {mongo_id})"
        )
        return True
        
    except Exception as e:
        if mysql_conn and mysql_conn.is_connected():
            mysql_conn.rollback()
        logging.error(
            f"Critical error processing {os.path.basename(pdf_path)}: {str(e)}\n"
            f"Traceback: {traceback.format_exc()}"
        )
        return False
    finally:
        if mysql_conn and mysql_conn.is_connected():
            mysql_conn.close()
        if mongo_client:
            mongo_client.close()
