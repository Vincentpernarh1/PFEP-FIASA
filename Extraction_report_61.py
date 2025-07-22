import time
import os
import sys
import json
import glob
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
import threading

# --- Global Configuration ---
DRIVER_FOLDER_NAME = "Driver"
DRIVER_NAME = "msedgedriver.exe"
REPORTS_FOLDER_NAME = "Reports"
MODELS_SUBFOLDER_NAME = "Modelos" # New subfolder for individual reports
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


def wait_and_get_downloaded_file(download_path, timeout):
    """
    Waits for a single file to complete downloading in the specified path and returns its full path.
    Returns None if the download times out.
    """
    seconds = 0
    while seconds < timeout:
        files = os.listdir(download_path)
        completed_files = [f for f in files if not f.endswith(('.crdownload', '.tmp'))]
        if completed_files:
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
    edge_options.add_argument("--inprivate")

    authenticated_url = f"https://{credentials['Usuario']}:{credentials['Senha']}@{BASE_URL_RELATORIO_61}"
    service = webdriver.edge.service.Service(driver_path)
    driver = webdriver.Edge(service=service, options=edge_options)

    try:
        print(f"[{thread_name}] Opening URL for Report 61 generation...")
        driver.get(authenticated_url)
        wait = WebDriverWait(driver, 60)

        # --- Step 1: Set the date to 6 months in the future ---
        print(f"[{thread_name}] Setting the date to 6 months from now.")
        future_date = date.today() + relativedelta(months=+6)
        date_string = f"{future_date.month}/{future_date.day}/{future_date.year}"

        date_input = wait.until(EC.presence_of_element_located((By.ID, "MainContent_txtDateFilter2_txtDate")))
        driver.execute_script(f"arguments[0].value = '{date_string}';", date_input)
        print(f"[{thread_name}] Date set to {date_string}")

        # --- Step 2: Loop through models and confirm each one to generate a report ---
        successful_models = []
        for model_name, model_text in models_to_process.items():
            print(f"[{thread_name}] Generating report for model: {model_name}")
            try:
                model_dropdown = Select(wait.until(EC.element_to_be_clickable((By.ID, "MainContent_ddlModel"))))
                model_dropdown.select_by_visible_text(model_text)
                driver.find_element(By.ID, "MainContent_cmdConfirm").click()
                wait.until(EC.text_to_be_present_in_element((By.ID, "MainContent_lblMessage"), "Elaboration correctly executed"))
                print(f"[{thread_name}] -> Report for {model_name} confirmed successfully.")
                successful_models.append(model_name)
                time.sleep(1)
            except (NoSuchElementException, TimeoutException):
                print(f"[{thread_name}] WARNING: Model '{model_name}' could not be processed. Skipping.")
                continue
        
        if not successful_models:
            print(f"[{thread_name}] No models were successfully processed. Aborting.")
            return

        # --- Step 3: Navigate to results list ---
        print(f"[{thread_name}] All reports generated. Navigating to the results page to download.")
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#MainContent_lblMessage > a.actlink"))).click()

        # --- Step 4: Wait for reports to be ready, refreshing the page periodically ---
        num_reports_to_download = len(successful_models)
        print(f"[{thread_name}] On results page. Waiting for {num_reports_to_download} reports to become available...")
        max_wait_minutes = 15
        start_time = time.time()
        while True:
            if time.time() - start_time > max_wait_minutes * 60:
                print(f"[{thread_name}] ERROR: Waited >{max_wait_minutes} mins. Proceeding with what is available.")
                break
            ready_links = driver.find_elements(By.XPATH, "//a[starts-with(@id, 'dgElaborationRequests_cmdListFiles_')]")
            print(f"[{thread_name}] Status: {len(ready_links)}/{num_reports_to_download} reports are ready.")
            if len(ready_links) >= num_reports_to_download:
                print(f"[{thread_name}] ✅ All reports are ready for download.")
                break
            else:
                print(f"[{thread_name}] Not all reports are ready. Refreshing in 5 seconds... 🔄")
                time.sleep(5)
                driver.refresh()
                wait.until(EC.presence_of_element_located((By.ID, "dgElaborationRequests")))

        # --- Step 5: Download each available report ---
        reports_on_page = driver.find_elements(By.XPATH, "//a[starts-with(@id, 'dgElaborationRequests_cmdListFiles_')]")
        for i in range(len(reports_on_page)):
            model_name_for_download = successful_models[i]
            print(f"[{thread_name}] Downloading file for model: '{model_name_for_download}'")
            try:
                for f in os.listdir(temp_download_path):
                    os.remove(os.path.join(temp_download_path, f))
                wait.until(EC.element_to_be_clickable((By.ID, f"dgElaborationRequests_cmdListFiles_{i}"))).click()
                wait.until(EC.element_to_be_clickable((By.ID, "dgFiles_hlkDownloadFile_0"))).click()
                newly_downloaded_path = wait_and_get_downloaded_file(temp_download_path, 120)
                if newly_downloaded_path:
                    file_extension = os.path.splitext(newly_downloaded_path)[1]
                    final_filename = f"{model_name_for_download}{file_extension}"
                    # **MODIFIED: Save to the 'Modelos' subfolder**
                    modelos_folder_path = os.path.join(reports_path, MODELS_SUBFOLDER_NAME)
                    final_filepath = os.path.join(modelos_folder_path, final_filename)
                    if os.path.exists(final_filepath):
                        os.remove(final_filepath)
                    shutil.move(newly_downloaded_path, final_filepath)
                    print(f"[{thread_name}] -> 💾 File successfully saved to Models folder as: {final_filename}")
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
        print(f"[{thread_name}] Closing browser and cleaning up for Report 61.")
        driver.quit()
        if os.path.exists(temp_download_path):
            shutil.rmtree(temp_download_path)

def merge_models(reports_path):
    """Merges all individual model CSV files into a single master CSV file."""
    print("\n--- Starting Model File Merge Process ---")
    modelos_folder_path = os.path.join(reports_path, MODELS_SUBFOLDER_NAME)

    if not os.path.isdir(modelos_folder_path):
        print(f"WARNING: '{MODELS_SUBFOLDER_NAME}' subfolder not found. Skipping merge.")
        return

    csv_files = glob.glob(os.path.join(modelos_folder_path, "*.csv"))
    if not csv_files:
        print("No model CSV files found in the 'Modelos' subfolder to merge.")
        return

    print(f"Found {len(csv_files)} model files to merge.")
    df_list = []
    for file in csv_files:
        try:
            df = pd.read_csv(file, delimiter=',', encoding='utf-16', low_memory=False)
            df_list.append(df)
        except Exception as e:
            print(f"ERROR: Could not read file '{os.path.basename(file)}'. Reason: {e}")
            continue

    if not df_list:
        print("Could not read any model files. Merge aborted.")
        return

    merged_df = pd.concat(df_list, ignore_index=True)
    output_filename = "Todos Modelos.csv"
    output_filepath = os.path.join(reports_path, output_filename)
    try:
        merged_df.to_csv(output_filepath, index=False, encoding='utf-16')
        print(f"✅ Successfully merged all models into: {output_filename}")
    except Exception as e:
        print(f"ERROR: Could not save merged file. Reason: {e}")

def convert_csv_to_excel(reports_path):
    """Converts the merged 'Todos Modelos.csv' file to .xlsx format."""
    print("\n--- Starting Final CSV to Excel Conversion ---")
    csv_filename = "Todos Modelos.csv"
    csv_filepath = os.path.join(reports_path, csv_filename)

    if not os.path.exists(csv_filepath):
        print(f"Merged file '{csv_filename}' not found. Skipping conversion.")
        return

    excel_filename = "Todos Modelos.xlsx"
    excel_filepath = os.path.join(reports_path, excel_filename)

    try:
        print(f"Converting '{csv_filename}' to Excel format...")
        df = pd.read_csv(csv_filepath, delimiter=',', encoding='utf-16', low_memory=False)
        df.to_excel(excel_filepath, index=False)
        print(f"✅ Successfully converted to {excel_filename}")
        os.remove(csv_filepath)
        print(f"Intermediate CSV file '{csv_filename}' removed.")
    except Exception as e:
        print(f"ERROR: Could not convert '{csv_filename}'. Reason: {e}")


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
    # Create the 'Modelos' subfolder for individual reports
    os.makedirs(os.path.join(reports_path, MODELS_SUBFOLDER_NAME), exist_ok=True)

    try:
        credentials_path = os.path.join(base_path, JSON_CREDENTIALS_FILE)
        with open(credentials_path, 'r', encoding='utf-8') as f:
            credentials = json.load(f)
        if 'Usuario' not in credentials or 'Senha' not in credentials:
            raise KeyError("The JSON file must contain 'Usuario' and 'Senha' keys.")
    except Exception as e:
        print(f"FATAL: Could not load credentials. Error: {e}")
        exit()

    # --- Phase 1: Process Report 61 (Download individual models) ---
    print("\n--- Starting Processing for Report 61 ---")
    process_report_61(
        "Relatorio 61",
        driver_path,
        reports_path,
        credentials,
        base_path
    )
    print("\n--- Report 61 processing finished. ---")

    # --- Phase 2: Merge the downloaded model files ---
    merge_models(reports_path)

    # --- Phase 3: Convert the final merged CSV to Excel ---
    convert_csv_to_excel(reports_path)

    print("\n--- ✅ Full process completed. ---")