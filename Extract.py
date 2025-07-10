import time
import os
import sys
import json
import threading
import pandas as pd
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.edge.options import Options as EdgeOptions

# --- Global Configuration ---
DRIVER_FOLDER_NAME = "Driver"
DRIVER_NAME = "msedgedriver.exe"
REPORTS_FOLDER_NAME = "Reports"
BASE_URL = "rtmcarroceria.fiat.com.br/bom/Functions/AllactivitiesList.aspx?idPlant=19"
JSON_CREDENTIALS_FILE = "Usuario.json"

# --- Reports to Download ---
REPORTS_TO_DOWNLOAD = [
    ("32", "Relatorio 32"),
    ("27", "Relatorio 27"),
    ("61", "Relatorio 61"),
]

def download_report(report_id, new_filename_base, driver_path, reports_path, credentials):
    """
    Handles only the download and renaming process for a single report.
    The conversion to Excel is handled separately after all downloads are complete.
    """
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Starting download for Report ID: {report_id}")

    temp_download_path = os.path.join(reports_path, f"temp_{report_id}_{threading.get_ident()}")
    os.makedirs(temp_download_path, exist_ok=True)

    edge_options = EdgeOptions()
    prefs = {"download.default_directory": temp_download_path}
    edge_options.add_experimental_option("prefs", prefs)
    edge_options.add_argument("--headless")
    edge_options.add_argument("--disable-gpu")

    authenticated_url = f"https://{credentials['Usuario']}:{credentials['Senha']}@{BASE_URL}"
    service = webdriver.edge.service.Service(driver_path)
    driver = webdriver.Edge(service=service, options=edge_options)

    try:
        print(f"[{thread_name}] Opening URL...")
        driver.get(authenticated_url)
        main_page_wait = WebDriverWait(driver, 40)

        print(f"[{thread_name}] Waiting for the dropdown menu...")
        dropdown = main_page_wait.until(EC.presence_of_element_located((By.ID, "ddlProcedures")))
        
        select_object = Select(dropdown)
        if report_id not in [opt.get_attribute("value") for opt in select_object.options]:
            print(f"[{thread_name}] ERROR: Report ID '{report_id}' not found in dropdown. Skipping.")
            return

        print(f"[{thread_name}] Selecting option '{report_id}'...")
        select_object.select_by_value(report_id)

        print(f"[{thread_name}] Waiting for the 'Files' button...")
        files_button = main_page_wait.until(EC.element_to_be_clickable((By.ID, "dgActivities_cmdListFiles_0")))
        files_button.click()

        print(f"[{thread_name}] Waiting for the final 'Download' button...")
        final_download_button = main_page_wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Download")))
        final_download_button.click()

        print(f"[{thread_name}] Waiting for download to complete...")
        download_wait_timeout = 120
        time_waited = 0
        downloaded_filename = None
        while time_waited < download_wait_timeout:
            downloaded_files = os.listdir(temp_download_path)
            if downloaded_files:
                filename = downloaded_files[0]
                if not filename.endswith('.crdownload') and not filename.endswith('.tmp'):
                    downloaded_filename = filename
                    print(f"[{thread_name}] Download completed. New file: {downloaded_filename}")
                    break
            time.sleep(1)
            time_waited += 1

        if not downloaded_filename:
            raise TimeoutException(f"[{thread_name}] Download did not complete within the {download_wait_timeout} second timeout period.")

        original_filepath = os.path.join(temp_download_path, downloaded_filename)
        file_extension = os.path.splitext(downloaded_filename)[1]
        
        final_filename = f"{new_filename_base}{file_extension}"
        final_filepath = os.path.join(reports_path, final_filename)

        if os.path.exists(final_filepath):
            os.remove(final_filepath)
        shutil.move(original_filepath, final_filepath)
        print(f"[{thread_name}] File successfully moved and renamed to: {final_filename}")

    except TimeoutException as e:
        print(f"\n[{thread_name}] ERROR: A timeout occurred. {e}")
    except NoSuchElementException as e:
        print(f"\n[{thread_name}] ERROR: An element was not found. {e}")
    except Exception as e:
        print(f"\n[{thread_name}] ERROR: An unexpected error occurred. {e}")
    finally:
        print(f"[{thread_name}] Closing browser and cleaning up temp folder.")
        driver.quit()
        if os.path.exists(temp_download_path):
            shutil.rmtree(temp_download_path)

def convert_csv_to_excel(reports_path):
    """
    Scans the reports folder for .csv files and converts them to .xlsx.
    """
    print("\n--- Starting CSV to Excel Conversion Process ---")
    files_to_convert = [f for f in os.listdir(reports_path) if f.lower().endswith('.csv')]
    
    if not files_to_convert:
        print("No .csv files found to convert.")
        return

    for filename in files_to_convert:
        csv_filepath = os.path.join(reports_path, filename)
        excel_filepath = os.path.join(reports_path, os.path.splitext(filename)[0] + '.xlsx')
        
        try:
            print(f"Converting '{filename}' to Excel...")
            # --- FIX: Added low_memory=False to handle DtypeWarning and improve speed ---
            df = pd.read_csv(csv_filepath, delimiter=',', encoding='utf-16', low_memory=False)
            df.to_excel(excel_filepath, index=False)
            print(f"Successfully converted to {os.path.basename(excel_filepath)}")
            os.remove(csv_filepath)
            print(f"Original CSV file '{filename}' removed.")
        except Exception as e:
            print(f"ERROR: Could not convert '{filename}'. Reason: {e}")


if __name__ == "__main__":
    try:
        base_path = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
        driver_path = os.path.join(base_path, DRIVER_FOLDER_NAME, DRIVER_NAME)
        reports_path = os.path.join(base_path, REPORTS_FOLDER_NAME)
    except Exception as e:
        print(f"FATAL: Could not determine script paths. Error: {e}")
        exit()

    if not os.path.exists(driver_path):
        print(f"FATAL: Driver not found at {driver_path}")
        exit()

    os.makedirs(reports_path, exist_ok=True)

    try:
        credentials_path = os.path.join(base_path, JSON_CREDENTIALS_FILE)
        with open(credentials_path, 'r', encoding='utf-8') as f:
            credentials = json.load(f)
        if 'Usuario' not in credentials or 'Senha' not in credentials:
            raise KeyError("The JSON file must contain the keys 'Usuario' and 'Senha'.")
    except Exception as e:
        print(f"FATAL: Could not load credentials. Error: {e}")
        exit()

    # --- Phase 1: Download all reports in parallel ---
    print("--- Starting Parallel Report Download ---")
    threads = []
    for report_id, report_name in REPORTS_TO_DOWNLOAD:
        thread = threading.Thread(
            target=download_report,
            args=(report_id, report_name, driver_path, reports_path, credentials),
            name=f"Report-{report_id}"
        )
        threads.append(thread)
        thread.start()
        time.sleep(2)

    for thread in threads:
        thread.join()
    print("\n--- All download tasks have finished. ---")

    # --- Phase 2: Convert all downloaded CSVs to Excel ---
    convert_csv_to_excel(reports_path)
    
    print("\n--- Full process completed. ---")
