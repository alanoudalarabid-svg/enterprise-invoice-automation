# process_invoices.py
import os
import time
from datetime import datetime
from etisalat_invoice import process_single_invoice

def process_invoice_batch(folder_path: str = "invoices"):
    pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]
    total_files = len(pdf_files)
    processed_count = 0
    
    if not pdf_files:
        print("No PDF files found in the invoices folder")
        return
    
    print(f"Starting batch processing of {total_files} invoices at {datetime.now()}")
    
    for idx, pdf_name in enumerate(pdf_files, 1):
        start_time = time.time()
        pdf_path = os.path.join(folder_path, pdf_name)
        
        print(f"\nProcessing {pdf_name} ({idx}/{total_files})")
        print(f"Remaining: {total_files - idx} invoices")
        
        # Process the invoice and measure time
        success = process_single_invoice(pdf_path, start_time)
        proc_time = time.time() - start_time
        
        status = "SUCCESS" if success else "FAILED"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"[{timestamp}] Processed {pdf_name} in {proc_time:.2f}s - {status}")
        
        if success:
            processed_count += 1
    
    print(f"\nBatch complete. Successfully processed {processed_count}/{total_files} invoices")

if __name__ == "__main__":
    process_invoice_batch()