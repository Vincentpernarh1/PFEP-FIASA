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
BASE_URL_RELATORIO_61 = "rtmcarroceria.fiat.com.br/bom/Elab/elab61.aspx?idPlant=19&idElaborationType=61"
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
    authenticated_url_relatorio_61 = f"https://{credentials['Usuario']}:{credentials['Senha']}@{BASE_URL}"
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











# relatorio 61 files included



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
from datetime import date
from dateutil.relativedelta import relativedelta

# --- Global Configuration ---
DRIVER_FOLDER_NAME = "Driver"
DRIVER_NAME = "msedgedriver.exe"
REPORTS_FOLDER_NAME = "Reports"
BASE_URL = "rtmcarroceria.fiat.com.br/bom/Functions/AllactivitiesList.aspx?idPlant=19"
BASE_URL_RELATORIO_61 = "rtmcarroceria.fiat.com.br/bom/Elab/elab61.aspx?idPlant=19&idElaborationType=61"
JSON_CREDENTIALS_FILE = "Usuario.json"
JSON_MODELS_FILE = "Modelos.json" # New file for Report 61

# --- Reports to Download (Only Report 61 is relevant for this run) ---
REPORTS_TO_DOWNLOAD = [
    # ("32", "Relatorio 32"),
    # ("27", "Relatorio 27"),
    ("61", "Relatorio 61"),
]

# def download_report(report_id, new_filename_base, driver_path, reports_path, credentials):
#     """
#     Handles only the download and renaming process for a single standard report.
#     --- THIS FUNCTION IS TEMPORARILY DISABLED TO FOCUS ON REPORT 61 ---
#     """
#     pass


def wait_and_get_downloaded_file(download_path, timeout):
    """
    Waits for a single file to complete downloading in the specified path and returns its full path.
    Returns None if the download times out.
    """
    seconds = 0
    while seconds < timeout:
        # List all items in the directory
        files = os.listdir(download_path)
        # Filter out temporary download files
        completed_files = [f for f in files if not f.endswith(('.crdownload', '.tmp'))]
        if completed_files:
            # Return the full path of the first completed file found
            return os.path.join(download_path, completed_files[0])
        time.sleep(1)
        seconds += 1
    return None


def process_report_61(new_filename_base, driver_path, reports_path, credentials, base_path):
    """
    Handles the special multi-step generation and download for Report 61.
    """
    thread_name = "Report-61"
    print(f"[{thread_name}] Starting special process for Report 61")

    # --- Load Models from JSON ---
    modelos_json_path = os.path.join(base_path, JSON_MODELS_FILE)
    try:
        with open(modelos_json_path, 'r', encoding='utf-8') as f:
            models_to_process = json.load(f)
        if not isinstance(models_to_process, dict):
            raise TypeError("JSON file content must be a dictionary (key-value pairs).")
        # Store model names to use for renaming files later
        processed_model_names = list(models_to_process.keys())
        print(f"[{thread_name}] Loaded {len(processed_model_names)} models from {JSON_MODELS_FILE}")
    except FileNotFoundError:
        print(f"[{thread_name}] ERROR: {JSON_MODELS_FILE} not found at {modelos_json_path}. Cannot proceed.")
        return
    except (json.JSONDecodeError, TypeError) as e:
        print(f"[{thread_name}] ERROR: Invalid format in {JSON_MODELS_FILE}. {e}")
        return

    # --- Setup Selenium Driver ---
    temp_download_path = os.path.join(reports_path, f"temp_61_{threading.get_ident()}")
    os.makedirs(temp_download_path, exist_ok=True)

    edge_options = EdgeOptions()
    prefs = {"download.default_directory": temp_download_path}
    edge_options.add_experimental_option("prefs", prefs)
    # --- Commented out headless mode for development/debugging ---
    # edge_options.add_argument("--headless")
    # edge_options.add_argument("--disable-gpu")
    edge_options.add_argument("--log-level=3")
    edge_options.add_argument("--inprivate") # Use private mode to avoid cache issues

    authenticated_url = f"https://{credentials['Usuario']}:{credentials['Senha']}@{BASE_URL_RELATORIO_61}"
    service = webdriver.edge.service.Service(driver_path)
    driver = webdriver.Edge(service=service, options=edge_options)

    try:
        print(f"[{thread_name}] Opening URL for Report 61 generation...")
        driver.get(authenticated_url)
        wait = WebDriverWait(driver, 60)

        # --- Step 1: Set the date to 6 months in the future ---
        print(f"[{thread_name}] Setting the date to 6 months from now.")
        # On July 20, 2025, this will be January 20, 2026
        future_date = date.today() + relativedelta(months=+6)
        date_string = f"{future_date.month}/{future_date.day}/{future_date.year}"

        date_input = wait.until(EC.presence_of_element_located((By.ID, "MainContent_txtDateFilter2_txtDate")))
        driver.execute_script(f"arguments[0].value = '{date_string}';", date_input)
        print(f"[{thread_name}] Date set to {date_string}")

        # --- Step 2: Loop through models and confirm each one to generate a report ---
        for i, (model_name, model_text) in enumerate(models_to_process.items()):
            print(f"[{thread_name}] Generating report for model {i+1}/{len(models_to_process)}: {model_name}")
            try:
                model_dropdown = Select(wait.until(EC.element_to_be_clickable((By.ID, "MainContent_ddlModel"))))
                model_dropdown.select_by_visible_text(model_text)

                confirm_button = driver.find_element(By.ID, "MainContent_cmdConfirm")
                confirm_button.click()

                wait.until(EC.text_to_be_present_in_element((By.ID, "MainContent_lblMessage"), "Elaboration correctly executed"))
                print(f"[{thread_name}] -> Report for {model_name} confirmed successfully.")
                time.sleep(1)

            except (NoSuchElementException, TimeoutException):
                print(f"[{thread_name}] WARNING: Model '{model_name}' could not be processed. Skipping.")
                processed_model_names.remove(model_name) # Remove from list if generation fails
                continue

        # --- Step 3: Navigate to results list ---
        print(f"[{thread_name}] All reports generated. Navigating to the results page to download.")
        results_link = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#MainContent_lblMessage > a.actlink")))
        results_link.click()

        # --- Step 4: Wait for reports to be ready, refreshing the page periodically ---
        num_reports_to_download = len(processed_model_names)
        print(f"[{thread_name}] On results page. Waiting for {num_reports_to_download} reports to become available...")
        
        # NEW: Loop with a 15-minute timeout that refreshes the page
        max_wait_minutes = 15
        start_time = time.time()
        while True:
            if time.time() - start_time > max_wait_minutes * 60:
                print(f"[{thread_name}] ERROR: Waited >{max_wait_minutes} mins, but not all reports are ready. Proceeding with what is available.")
                break
            
            # Count how many reports are ready by finding the "Files" links
            ready_links = driver.find_elements(By.XPATH, "//a[starts-with(@id, 'dgElaborationRequests_cmdListFiles_')]")
            print(f"[{thread_name}] Status: {len(ready_links)}/{num_reports_to_download} reports are ready.")
            
            if len(ready_links) >= num_reports_to_download:
                print(f"[{thread_name}] ✅ All reports are ready for download.")
                break
            else:
                print(f"[{thread_name}] Not all reports are ready. Refreshing in 5 seconds... 🔄")
                time.sleep(5)
                driver.refresh()
                # Wait for the main table to be present after refresh
                wait.until(EC.presence_of_element_located((By.ID, "dgElaborationRequests")))

        # --- Step 5: Download each available report ---
        reports_on_page = driver.find_elements(By.XPATH, "//a[starts-with(@id, 'dgElaborationRequests_cmdListFiles_')]")
        num_to_actually_download = len(reports_on_page)
        
        for i in range(num_to_actually_download):
            model_name_for_download = processed_model_names[i]
            print(f"[{thread_name}] Downloading file {i+1}/{num_to_actually_download} for model: '{model_name_for_download}'")
            try:
                for f in os.listdir(temp_download_path):
                    os.remove(os.path.join(temp_download_path, f))

                files_link = wait.until(EC.element_to_be_clickable((By.ID, f"dgElaborationRequests_cmdListFiles_{i}")))
                files_link.click()

                download_link = wait.until(EC.element_to_be_clickable((By.ID, "dgFiles_hlkDownloadFile_0")))
                download_link.click()

                newly_downloaded_path = wait_and_get_downloaded_file(temp_download_path, 120)

                if newly_downloaded_path:
                    file_extension = os.path.splitext(newly_downloaded_path)[1]
                    final_filename = f"{model_name_for_download}{file_extension}"
                    final_filepath = os.path.join(reports_path, final_filename)

                    if os.path.exists(final_filepath):
                        os.remove(final_filepath)
                    shutil.move(newly_downloaded_path, final_filepath)
                    print(f"[{thread_name}] -> 💾 File successfully saved as: {final_filename}")
                else:
                    print(f"[{thread_name}] -> ⚠️ WARNING: Download timed out for model '{model_name_for_download}'.")

                driver.back()
                wait.until(EC.presence_of_element_located((By.ID, "dgElaborationRequests_cmdListFiles_0")))

            except Exception as e:
                print(f"[{thread_name}] -> ❌ ERROR during download for '{model_name_for_download}': {e}")
                driver.get(driver.current_url)
                continue

    except Exception as e:
        print(f"\n[{thread_name}] ❌ FATAL ERROR during Report 61 processing: {e}")
    finally:
        print(f"[{thread_name}] Closing browser and cleaning up temp folder for Report 61.")
        driver.quit()
        if os.path.exists(temp_download_path):
            shutil.rmtree(temp_download_path)

# def convert_csv_to_excel(reports_path):
#     """
#     Scans the reports folder for .csv files and converts them to .xlsx.
#     --- THIS FUNCTION IS TEMPORARILY DISABLED TO FOCUS ON REPORT 61 ---
#     """
#     pass


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

    # --- Focus only on processing Report 61 ---
    print("\n--- Starting Processing for Report 61 ---")
    process_report_61(
        "Relatorio 61",
        driver_path,
        reports_path,
        credentials,
        base_path
    )
    print("\n--- Report 61 processing finished. ---")
    
    # --- Other phases are disabled for this focused run ---
    
    print("\n--- Full process completed. ---")



























    # New code for testing