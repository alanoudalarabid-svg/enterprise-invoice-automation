from flask import Flask, render_template, request, jsonify
import os
from etisalat_invoice import process_single_invoice
import time
from datetime import datetime
from werkzeug.utils import secure_filename
import logging
import traceback
from logging.handlers import RotatingFileHandler
import shutil
from etisalat_invoice import  get_mysql_connection, get_mongodb_client
from dotenv import load_dotenv
from uuid import uuid4
import hashlib

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY') 

# Configuration
UPLOAD_FOLDER = 'invoices'
PROCESSED_FOLDER = 'processed_invoices'
ALLOWED_EXTENSIONS = {'pdf'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024  # 10MB per file

# Configure error logging
handler = RotatingFileHandler('processing_errors.log', maxBytes=100000, backupCount=3)
handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s - [%(filename)s:%(lineno)d]'
))
app.logger.addHandler(handler)
logging.getLogger().setLevel(logging.ERROR)

# Ensure folders exist
for folder in [UPLOAD_FOLDER, PROCESSED_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def move_to_processed(filepath):
    """Move processed file to processed folder"""
    try:
        filename = os.path.basename(filepath)
        dest_path = os.path.join(app.config['PROCESSED_FOLDER'], filename)
        shutil.move(filepath, dest_path)
        return True
    except Exception as e:
        app.logger.error(f"File move error: {str(e)} | File: {filepath}")
        return False

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_invoice():
    """Handle file upload"""
    try:
        if 'file' not in request.files:
            app.logger.error("Upload attempt with no file part")
            return jsonify({'success': False}), 400
        
        file = request.files['file']
        if file.filename == '':
            app.logger.error("Upload attempt with empty filename")
            return jsonify({'success': False}), 400

        if not (file and allowed_file(file.filename)):
            app.logger.error(f"Invalid file type attempt: {file.filename}")
            return jsonify({'success': False}), 400

        temp_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        
        '''      
        filename = secure_filename(file.filename)
        filepath = os.path.join(temp_dir, filename)
        '''

        original_filename = secure_filename(file.filename)
        # Create unique filename to avoid overwriting
        unique_suffix = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')  # e.g., 20250415153045678900
        filename_with_uid = f"{original_filename}_{unique_suffix}"
        filepath = os.path.join(temp_dir, filename_with_uid)
        
        file.save(filepath)
        app.logger.info(f"File uploaded successfully: {original_filename} (Saved as: {filename_with_uid})")
        
        return jsonify({
            'success': True,
            'filename': original_filename,  # used for DB and UI
            'filepath': filepath            # used internally for processing
        })

    except Exception as e:
        app.logger.error(f"Upload error: {str(e)} | File: {file.filename if 'file' in locals() else 'unknown'}")
        return jsonify({'success': False}), 500
    

@app.route('/process_single', methods=['POST'])
def process_single():
    data = request.get_json()
    filename = data.get('filename')
    filepath = data.get('filepath')
    
    if not filename or not filepath:
        app.logger.error("Process request missing filename/filepath")
        return jsonify({'success': False}), 400
    
    try:
        if not os.path.exists(filepath):
            app.logger.error(f"File not found: {filename} | Path: {filepath}")
            return jsonify({'success': False}), 404
        
        start_time = time.time()
        try:
            success = process_single_invoice(filepath, start_time)
        except Exception as e:
            app.logger.error(f"Processing failed: {str(e)} | Trace: {traceback.format_exc()}")
            return jsonify({'success': False}), 500
        
        if success:
            verified = False
            for _ in range(3):
                mysql_ok = verify_mysql_entry(filename)
                mongo_ok = verify_mongodb_entry(filename)
                
                if mysql_ok and mongo_ok:
                    verified = True
                    move_to_processed(filepath)
                    app.logger.info(f"Successfully processed: {filename}")
                    break
                time.sleep(0.5)

            if verified:
                return jsonify({'success': True})
            
            app.logger.warning(f"Delayed verification for: {filename}")
            return jsonify({'success': False}), 202

        app.logger.error(f"Processing failed for: {filename}")
        return jsonify({'success': False}), 500
        
    except Exception as e:
        app.logger.error(f"Processing error: {str(e)} | File: {filename}")
        return jsonify({'success': False}), 500

@app.route('/log_error', methods=['POST'])
def log_client_error():
    """Endpoint for client-side error logging"""
    try:
        error_data = request.get_json()
        app.logger.error(
            f"CLIENT_ERROR - File: {error_data.get('filename', 'unknown')} | "
            f"Stage: {error_data.get('stage', 'unknown')} | "
            f"Error: {error_data.get('error', 'No details')} | "
            f"Timestamp: {error_data.get('timestamp', 'unknown')}"
        )
        return jsonify({'success': True})
    except Exception as e:
        app.logger.error(f"Error logging failed: {str(e)}")
        return jsonify({'success': False}), 500

def verify_mysql_entry(filename):
    """Check MySQL database entry"""
    conn = None
    try:
        conn = get_mysql_connection()
        # Use buffered cursor to prevent unread results
        cursor = conn.cursor(buffered=True)
        cursor.execute("SELECT id FROM invoices WHERE pdf_name = %s", (filename,))
        
        # Explicitly consume all results
        result = cursor.fetchone() is not None
        while cursor.fetchone() is not None:  # Clear any remaining results
            pass
            
        if not result:
            app.logger.warning(f"MySQL verification failed for: {filename}")
        return result
    except Exception as e:
        app.logger.error(f"MySQL verification error: {str(e)} | File: {filename}")
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def verify_mongodb_entry(filename):
    """Check MongoDB database entry"""
    try:
        client = get_mongodb_client()
        db = client["invoices_db"]
        collection = db["etisalat_invoices"]
        result = collection.find_one({"_id": filename})
        if not result:
            app.logger.warning(f"MongoDB verification failed for: {filename}")
        return result is not None
    except Exception as e:
        app.logger.error(f"MongoDB verification error: {str(e)} | File: {filename}")
        return False
    finally:
        if 'client' in locals() and client:
            client.close()

@app.route('/cancel_batch', methods=['POST'])
def cancel_batch():
    try:
        data = request.get_json()
        filenames = data.get('filenames', [])
        temp_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'temp')
        
        for filename in filenames:
            filepath = os.path.join(temp_dir, filename)
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                    app.logger.info(f"Cancelled file removed: {filename}")
                except Exception as e:
                    app.logger.error(f"Cancel cleanup failed: {str(e)} | File: {filename}")
        
        return jsonify({'success': True})
    except Exception as e:
        app.logger.error(f"Cancel batch error: {str(e)}")
        return jsonify({'success': False}), 500
    
@app.route('/debug_files', methods=['GET'])
def debug_files():
    try:
        files = {
            'upload_folder': {
                'path': os.path.abspath(app.config['UPLOAD_FOLDER']),
                'contents': os.listdir(app.config['UPLOAD_FOLDER'])
            },
            'processed_folder': {
                'path': os.path.abspath(app.config['PROCESSED_FOLDER']),
                'contents': os.listdir(app.config['PROCESSED_FOLDER'])
            }
        }
        return jsonify({'success': True, 'data': files})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/verify_processing', methods=['POST'])
def verify_processing():
    data = request.get_json()
    filename = data.get('filename')
    strict_check = data.get('strict_check', False)
    
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Thorough verification query
        query = """
            SELECT id, processed_at, account_number 
            FROM invoices 
            WHERE pdf_name = %s
            ORDER BY processed_at DESC 
            LIMIT 1
        """
        cursor.execute(query, (filename,))
        result = cursor.fetchone()
        
        if strict_check and result:
            # Additional verification - check related records exist
            cursor.execute("""
                SELECT 1 FROM usage_details 
                WHERE invoice_id = %s 
                LIMIT 1
            """, (result['id'],))
            has_usage_records = cursor.fetchone() is not None
            
            return jsonify({
                'success': True,
                'exists_in_db': has_usage_records,
                'full_verification': True
            })
        
        return jsonify({
            'success': True,
            'exists_in_db': result is not None,
            'full_verification': False
        })
        
    except Exception as e:
        app.logger.error(f"Verification error: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Database verification failed'
        }), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/log_error', methods=['POST'])
def log_error():
    try:
        data = request.get_json()
        app.logger.error(
            f"CLIENT ERROR - File: {data.get('filename', 'unknown')} | "
            f"Stage: {data.get('stage', 'unknown')} | "
            f"Error: {data.get('error', 'No details')} | "
            f"Timestamp: {data.get('timestamp', 'unknown')}"
        )
        return jsonify({'success': True})
    except Exception as e:
        app.logger.error(f"Error logging failed: {str(e)}")
        return jsonify({'success': False}), 500
    
if __name__ == "__main__":
    app.run(debug=True)