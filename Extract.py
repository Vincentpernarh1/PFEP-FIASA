import time
import os
import sys
import json
import glob
import pandas as pd
import shutil
import threading
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
MODELS_SUBFOLDER_NAME_61 = "Modelos_61"  # Subfolder for individual Report 61 files
MODELS_SUBFOLDER_NAME_29 = "Modelos_29"  # Subfolder for individual Report 29 files
OTHER_REPORTS_SUBFOLDER_NAME = "Outros_relatorios" # Subfolder for report 32
BASE_URL = "rtmcarroceria.fiat.com.br/bom/Functions/AllactivitiesList.aspx?idPlant=19"
BASE_URL_RELATORIO_61 = "rtmcarroceria.fiat.com.br/bom/Elab/elab61.aspx?idPlant=19&idElaborationType=61"
BASE_URL_RELATORIO_29 = "rtmcarroceria.fiat.com.br/bom/Elab/elab29.aspx?idPlant=19&idElaborationType=29" # New URL for Report 29
JSON_CREDENTIALS_FILE = "Usuario.json"
JSON_MODELS_FILE = "Modelos.json"

# --- Reports to Download ---
REPORTS_TO_DOWNLOAD = [
    ("32", "Relatorio 32"),
    ("29", "Relatorio 29"),
    ("61", "Relatorio 61"),
]

# --- Helper Function ---
def wait_and_get_downloaded_file(download_path, timeout):
    """
    Waits for a single file to complete downloading and returns its full path.
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

# --- Standard Report Download Function (for Report 32) ---
def download_standard_report(report_id, new_filename_base, driver_path, reports_path, credentials):
    """
    Handles the download for standard reports like Report 32.
    """
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Starting download for Standard Report ID: {report_id}")
    temp_download_path = os.path.join(reports_path, f"temp_{report_id}_{threading.get_ident()}")
    os.makedirs(temp_download_path, exist_ok=True)
    edge_options = EdgeOptions()
    prefs = {"download.default_directory": temp_download_path}
    edge_options.add_experimental_option("prefs", prefs)
    edge_options.add_argument("--inprivate")
    edge_options.add_argument("--log-level=3")
    authenticated_url = f"https://{credentials['Usuario']}:{credentials['Senha']}@{BASE_URL}"
    service = webdriver.edge.service.Service(driver_path)
    driver = webdriver.Edge(service=service, options=edge_options)
    try:
        driver.get(authenticated_url)
        wait = WebDriverWait(driver, 60)
        Select(wait.until(EC.presence_of_element_located((By.ID, "ddlProcedures")))).select_by_value(report_id)
        wait.until(EC.element_to_be_clickable((By.ID, "dgActivities_cmdListFiles_0"))).click()
        wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Download"))).click()
        downloaded_filepath = wait_and_get_downloaded_file(temp_download_path, 120)
        if not downloaded_filepath:
            raise TimeoutException("Download did not complete within the timeout period.")
        file_extension = os.path.splitext(downloaded_filepath)[1]
        final_filename = f"{new_filename_base}{file_extension}"
        final_filepath = os.path.join(reports_path, final_filename)
        if os.path.exists(final_filepath):
            os.remove(final_filepath)
        shutil.move(downloaded_filepath, final_filepath)
        print(f"[{thread_name}] ✅ File successfully saved as: {final_filename}")
    except Exception as e:
        print(f"\n[{thread_name}] ❌ ERROR: An unexpected error occurred. {e}")
    finally:
        driver.quit()
        if os.path.exists(temp_download_path):
            shutil.rmtree(temp_download_path)

# --- Special Processing Functions for Report 29 ---
def process_report_29(new_filename_base, driver_path, reports_path, credentials, base_path):
    """
    Handles the special multi-step generation and download for Report 29.
    """
    thread_name = "Report-29"
    print(f"\n--- [{thread_name}] Starting special process for Report 29 ---")
    modelos_json_path = os.path.join(base_path, JSON_MODELS_FILE)
    try:
        with open(modelos_json_path, 'r', encoding='utf-8') as f:
            models_to_process = json.load(f)
        all_models_list = list(models_to_process.items())
        chunk_size = 5
        model_chunks = [all_models_list[i:i + chunk_size] for i in range(0, len(all_models_list), chunk_size)]
        print(f"[{thread_name}] Loaded {len(all_models_list)} models, split into {len(model_chunks)} sequential chunks.")
    except Exception as e:
        print(f"[{thread_name}] ERROR: Could not load {JSON_MODELS_FILE}. {e}")
        return

    temp_download_path = os.path.join(reports_path, f"temp_{thread_name}_{os.getpid()}")
    os.makedirs(temp_download_path, exist_ok=True)
    edge_options = EdgeOptions()
    prefs = {"download.default_directory": temp_download_path}
    edge_options.add_experimental_option("prefs", prefs)
    edge_options.add_argument("--log-level=3")
    edge_options.add_argument("--inprivate")
    authenticated_url = f"https://{credentials['Usuario']}:{credentials['Senha']}@{BASE_URL_RELATORIO_29}"
    service = webdriver.edge.service.Service(driver_path)
    driver = webdriver.Edge(service=service, options=edge_options)
    try:
        for chunk_index, current_chunk in enumerate(model_chunks):
            print(f"\n[{thread_name}] --- Processing Chunk {chunk_index + 1}/{len(model_chunks)} ---")
            driver.get(authenticated_url)
            wait = WebDriverWait(driver, 60)
            wait.until(EC.presence_of_element_located((By.ID, "MainContent_ddlModel")))
            future_date = date.today() + relativedelta(months=+6)
            date_string = f"{future_date.month}/{future_date.day}/{future_date.year}"
            driver.execute_script(f"arguments[0].value = '{date_string}';", wait.until(EC.presence_of_element_located((By.ID, "MainContent_txtDateFilter2_txtDate"))))
            chunk_successful_models = []
            for model_name, model_text in current_chunk:
                try:
                    Select(wait.until(EC.element_to_be_clickable((By.ID, "MainContent_ddlModel")))).select_by_visible_text(model_text)
                    driver.find_element(By.ID, "MainContent_cmdConfirm").click()
                    wait.until(EC.text_to_be_present_in_element((By.ID, "MainContent_lblMessage"), "Elaboration correctly executed"))
                    chunk_successful_models.append(model_name)
                    time.sleep(1)
                except (NoSuchElementException, TimeoutException):
                    print(f"[{thread_name}] WARNING: Model '{model_name}' could not be processed. Skipping.")
            if not chunk_successful_models:
                print(f"[{thread_name}] No models in chunk {chunk_index + 1} processed. Skipping chunk.")
                continue
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#MainContent_lblMessage > a.actlink"))).click()
            num_reports_in_chunk = len(chunk_successful_models)
            print(f"[{thread_name}] On results page. Waiting for {num_reports_in_chunk} reports to finish...")
            max_wait_minutes = 15
            start_time = time.time()
            while True:
                if time.time() - start_time > max_wait_minutes * 60:
                    print(f"[{thread_name}] ERROR: Waited >{max_wait_minutes} mins. Proceeding with what is available.")
                    break
                ready_reports_count = 0
                try:
                    wait.until(EC.presence_of_element_located((By.XPATH, f"//tr[.//a[@id='dgElaborationRequests_cmdListFiles_{num_reports_in_chunk - 1}']]")))
                    activity_cells = driver.find_elements(By.XPATH, f"//tr[.//a[starts-with(@id, 'dgElaborationRequests_cmdListFiles_')]][position() <= {num_reports_in_chunk}]/td[4]")
                    for cell in activity_cells:
                        if "gold" not in cell.get_attribute("style").lower():
                            ready_reports_count += 1
                    if ready_reports_count >= num_reports_in_chunk:
                        print(f"[{thread_name}] ✅ All {num_reports_in_chunk} reports for this chunk are ready.")
                        break
                except TimeoutException:
                    pass
                try:
                    wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Apply Filter']"))).click()
                except Exception:
                    driver.refresh()
                time.sleep(5)
            for i in range(num_reports_in_chunk):
                model_name_for_download = chunk_successful_models[i]
                print(f"[{thread_name}] Downloading file for model: '{model_name_for_download}'")
                try:
                    for f in os.listdir(temp_download_path): os.remove(os.path.join(temp_download_path, f))
                    wait.until(EC.element_to_be_clickable((By.ID, f"dgElaborationRequests_cmdListFiles_{i}"))).click()
                    wait.until(EC.element_to_be_clickable((By.ID, "dgFiles_hlkDownloadFile_0"))).click()
                    newly_downloaded_path = wait_and_get_downloaded_file(temp_download_path, 120)
                    if newly_downloaded_path:
                        final_filename = f"{model_name_for_download}{os.path.splitext(newly_downloaded_path)[1]}"
                        modelos_folder_path = os.path.join(reports_path, MODELS_SUBFOLDER_NAME_29)
                        shutil.move(newly_downloaded_path, os.path.join(modelos_folder_path, final_filename))
                        print(f"[{thread_name}] -> 💾 File successfully saved as: {final_filename}")
                    else:
                        print(f"[{thread_name}] -> ⚠️ WARNING: Download timed out for model '{model_name_for_download}'.")
                    driver.back()
                    wait.until(EC.element_to_be_clickable((By.ID, "dgElaborationRequests_cmdListFiles_0")))
                except Exception as e:
                    print(f"[{thread_name}] -> ❌ ERROR during download for '{model_name_for_download}': {e}")
                    driver.get(driver.current_url)
    except Exception as e:
        print(f"\n[{thread_name}] ❌ FATAL ERROR during Report 29 processing: {e}")
    finally:
        print(f"[{thread_name}] Process finished. Closing browser.")
        driver.quit()
        if os.path.exists(temp_download_path):
            shutil.rmtree(temp_download_path)
        print(f"--- [{thread_name}] ✅ Special process for Report 29 completed. ---")

def merge_models_29(reports_path, base_path):
    """Merges all individual Report 29 CSV files into a single master CSV."""
    print("\n--- Starting Report 29 Model File Merge Process ---")
    modelos_folder_path = os.path.join(reports_path, MODELS_SUBFOLDER_NAME_29)
    try:
        with open(os.path.join(base_path, JSON_MODELS_FILE), 'r', encoding='utf-8') as f:
            models_data = json.load(f)
    except Exception as e:
        print(f"ERROR: Could not load {JSON_MODELS_FILE}. Reason: {e}")
        return
    csv_files = glob.glob(os.path.join(modelos_folder_path, "*.csv"))
    if not csv_files:
        print("No Report 29 model CSV files found to merge.")
        return
    df_list = []
    for file in csv_files:
        try:
            model_name = os.path.splitext(os.path.basename(file))[0]
            model_text = models_data.get(model_name)
            model_code = model_text.split()[0] if model_text else "UNKNOWN"
            df = pd.read_csv(file, delimiter=',', encoding='utf-16', low_memory=False)
            df['Model'] = model_code
            df_list.append(df)
        except Exception as e:
            print(f"ERROR: Could not process file '{os.path.basename(file)}'. Reason: {e}")
    if not df_list:
        print("Could not read any Report 29 model files. Merge aborted.")
        return
    merged_df = pd.concat(df_list, ignore_index=True)
    output_filepath = os.path.join(reports_path, "Todos Modelos_29.csv")
    merged_df.to_csv(output_filepath, index=False, encoding='utf-16')
    print(f"✅ Successfully merged all Report 29 models into: Todos Modelos_29.csv")

def process_merged_report_29(reports_path):
    """Converts merged 'Todos Modelos_29.csv', adding PartNumber and chave columns."""
    print("\n--- Starting Final Processing for Report 29 ---")
    csv_filepath = os.path.join(reports_path, "Todos Modelos_29.csv")
    if not os.path.exists(csv_filepath):
        print("Merged file 'Todos Modelos_29.csv' not found. Skipping.")
        return
    excel_filepath = os.path.join(reports_path, "Todos Modelos_29.xlsx")
    try:
        df = pd.read_csv(csv_filepath, delimiter=',', encoding='utf-16', low_memory=False)
        df.to_excel(excel_filepath, index=False, engine='xlsxwriter')
        print(f"✅ Successfully created {os.path.basename(excel_filepath)}.")
        os.remove(csv_filepath)
        print("Intermediate CSV file 'Todos Modelos_29.csv' removed.")
    except Exception as e:
        print(f"ERROR: Could not process 'Todos Modelos_29.csv'. Reason: {e}")

# --- Report 61 Special Processing Functions (UNCHANGED) ---
def process_report_61(new_filename_base, driver_path, reports_path, credentials, base_path):
    """
    Handles the special multi-step generation and download for Report 61.
    """
    thread_name = "Report-61"
    print(f"\n--- [{thread_name}] Starting special process for Report 61 ---")
    modelos_json_path = os.path.join(base_path, JSON_MODELS_FILE)
    try:
        with open(modelos_json_path, 'r', encoding='utf-8') as f:
            models_to_process = json.load(f)
        all_models_list = list(models_to_process.items())
        chunk_size = 5
        model_chunks = [all_models_list[i:i + chunk_size] for i in range(0, len(all_models_list), chunk_size)]
        print(f"[{thread_name}] Loaded {len(all_models_list)} models, split into {len(model_chunks)} sequential chunks.")
    except Exception as e:
        print(f"[{thread_name}] ERROR: Could not load {JSON_MODELS_FILE}. {e}")
        return
    temp_download_path = os.path.join(reports_path, f"temp_{thread_name}_{os.getpid()}")
    os.makedirs(temp_download_path, exist_ok=True)
    edge_options = EdgeOptions()
    prefs = {"download.default_directory": temp_download_path}
    edge_options.add_experimental_option("prefs", prefs)
    edge_options.add_argument("--log-level=3")
    edge_options.add_argument("--inprivate")
    authenticated_url = f"https://{credentials['Usuario']}:{credentials['Senha']}@{BASE_URL_RELATORIO_61}"
    service = webdriver.edge.service.Service(driver_path)
    driver = webdriver.Edge(service=service, options=edge_options)
    try:
        for chunk_index, current_chunk in enumerate(model_chunks):
            print(f"\n[{thread_name}] --- Processing Chunk {chunk_index + 1}/{len(model_chunks)} ---")
            driver.get(authenticated_url)
            wait = WebDriverWait(driver, 60)
            wait.until(EC.presence_of_element_located((By.ID, "MainContent_ddlModel")))
            future_date = date.today() + relativedelta(months=+6)
            date_string = f"{future_date.month}/{future_date.day}/{future_date.year}"
            driver.execute_script(f"arguments[0].value = '{date_string}';", wait.until(EC.presence_of_element_located((By.ID, "MainContent_txtDateFilter2_txtDate"))))
            chunk_successful_models = []
            for model_name, model_text in current_chunk:
                try:
                    Select(wait.until(EC.element_to_be_clickable((By.ID, "MainContent_ddlModel")))).select_by_visible_text(model_text)
                    driver.find_element(By.ID, "MainContent_cmdConfirm").click()
                    wait.until(EC.text_to_be_present_in_element((By.ID, "MainContent_lblMessage"), "Elaboration correctly executed"))
                    chunk_successful_models.append(model_name)
                    time.sleep(1)
                except (NoSuchElementException, TimeoutException):
                    print(f"[{thread_name}] WARNING: Model '{model_name}' could not be processed. Skipping.")
            if not chunk_successful_models:
                print(f"[{thread_name}] No models in chunk {chunk_index + 1} processed. Skipping chunk.")
                continue
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#MainContent_lblMessage > a.actlink"))).click()
            num_reports_in_chunk = len(chunk_successful_models)
            print(f"[{thread_name}] On results page. Waiting for {num_reports_in_chunk} reports to finish...")
            max_wait_minutes = 15
            start_time = time.time()
            while True:
                if time.time() - start_time > max_wait_minutes * 60:
                    print(f"[{thread_name}] ERROR: Waited >{max_wait_minutes} mins. Proceeding with what is available.")
                    break
                ready_reports_count = 0
                try:
                    wait.until(EC.presence_of_element_located((By.XPATH, f"//tr[.//a[@id='dgElaborationRequests_cmdListFiles_{num_reports_in_chunk - 1}']]")))
                    activity_cells = driver.find_elements(By.XPATH, f"//tr[.//a[starts-with(@id, 'dgElaborationRequests_cmdListFiles_')]][position() <= {num_reports_in_chunk}]/td[4]")
                    for cell in activity_cells:
                        if "gold" not in cell.get_attribute("style").lower():
                            ready_reports_count += 1
                    if ready_reports_count >= num_reports_in_chunk:
                        print(f"[{thread_name}] ✅ All {num_reports_in_chunk} reports for this chunk are ready.")
                        break
                except TimeoutException:
                    pass
                try:
                    wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Apply Filter']"))).click()
                except Exception:
                    driver.refresh()
                time.sleep(5)
            for i in range(num_reports_in_chunk):
                model_name_for_download = chunk_successful_models[i]
                print(f"[{thread_name}] Downloading file for model: '{model_name_for_download}'")
                try:
                    for f in os.listdir(temp_download_path): os.remove(os.path.join(temp_download_path, f))
                    wait.until(EC.element_to_be_clickable((By.ID, f"dgElaborationRequests_cmdListFiles_{i}"))).click()
                    wait.until(EC.element_to_be_clickable((By.ID, "dgFiles_hlkDownloadFile_0"))).click()
                    newly_downloaded_path = wait_and_get_downloaded_file(temp_download_path, 120)
                    if newly_downloaded_path:
                        final_filename = f"{model_name_for_download}{os.path.splitext(newly_downloaded_path)[1]}"
                        modelos_folder_path = os.path.join(reports_path, MODELS_SUBFOLDER_NAME_61)
                        shutil.move(newly_downloaded_path, os.path.join(modelos_folder_path, final_filename))
                        print(f"[{thread_name}] -> 💾 File successfully saved as: {final_filename}")
                    else:
                        print(f"[{thread_name}] -> ⚠️ WARNING: Download timed out for model '{model_name_for_download}'.")
                    driver.back()
                    wait.until(EC.element_to_be_clickable((By.ID, "dgElaborationRequests_cmdListFiles_0")))
                except Exception as e:
                    print(f"[{thread_name}] -> ❌ ERROR during download for '{model_name_for_download}': {e}")
                    driver.get(driver.current_url)
    except Exception as e:
        print(f"\n[{thread_name}] ❌ FATAL ERROR during Report 61 processing: {e}")
    finally:
        print(f"[{thread_name}] Process finished. Closing browser.")
        driver.quit()
        if os.path.exists(temp_download_path):
            shutil.rmtree(temp_download_path)
        print(f"--- [{thread_name}] ✅ Special process for Report 61 completed. ---")

def merge_models_61(reports_path, base_path):
    """Merges all individual Report 61 CSV files into a single master CSV."""
    print("\n--- Starting Report 61 Model File Merge Process ---")
    modelos_folder_path = os.path.join(reports_path, MODELS_SUBFOLDER_NAME_61)
    try:
        with open(os.path.join(base_path, JSON_MODELS_FILE), 'r', encoding='utf-8') as f:
            models_data = json.load(f)
    except Exception as e:
        print(f"ERROR: Could not load {JSON_MODELS_FILE}. Reason: {e}")
        return
    csv_files = glob.glob(os.path.join(modelos_folder_path, "*.csv"))
    if not csv_files:
        print("No Report 61 model CSV files found to merge.")
        return
    df_list = []
    for file in csv_files:
        try:
            model_name = os.path.splitext(os.path.basename(file))[0]
            model_text = models_data.get(model_name)
            model_code = model_text.split()[0] if model_text else "UNKNOWN"
            df = pd.read_csv(file, delimiter=',', encoding='utf-16', low_memory=False)
            df['Model'] = model_code
            df_list.append(df)
        except Exception as e:
            print(f"ERROR: Could not process file '{os.path.basename(file)}'. Reason: {e}")
    if not df_list:
        print("Could not read any Report 61 model files. Merge aborted.")
        return
    merged_df = pd.concat(df_list, ignore_index=True)
    output_filepath = os.path.join(reports_path, "Todos Modelos_61.csv")
    merged_df.to_csv(output_filepath, index=False, encoding='utf-16')
    print(f"✅ Successfully merged all Report 61 models into: Todos Modelos_61.csv")

def process_merged_report_61(reports_path):
    """Converts merged 'Todos Modelos_61.csv', adding PartNumber and chave columns."""
    print("\n--- Starting Final Processing for Report 61 ---")
    csv_filepath = os.path.join(reports_path, "Todos Modelos_61.csv")
    if not os.path.exists(csv_filepath):
        print("Merged file 'Todos Modelos_61.csv' not found. Skipping.")
        return
    excel_filepath = os.path.join(reports_path, "Todos Modelos_61.xlsx")
    try:
        df = pd.read_csv(csv_filepath, delimiter=',', encoding='utf-16', low_memory=False)
        df.rename(columns={'vcCode': 'PartNumber'}, inplace=True)
        df['PartNumber'] = pd.to_numeric(df['PartNumber'], errors='coerce')
        df.dropna(subset=['PartNumber'], inplace=True)
        df['PartNumber'] = df['PartNumber'].astype(int)
        df['chave'] = df['PartNumber'].astype(str) + '_' + df['Model'].astype(str)
        df.to_excel(excel_filepath, index=False, engine='xlsxwriter')
        print(f"✅ Successfully created {os.path.basename(excel_filepath)}.")
        os.remove(csv_filepath)
        print("Intermediate CSV file 'Todos Modelos_61.csv' removed.")
    except Exception as e:
        print(f"ERROR: Could not process 'Todos Modelos_61.csv'. Reason: {e}")

# --- Post-Processing Functions ---
def process_other_reports(main_reports_path, other_reports_subfolder_path):
    """
    Moves report 32 to its subfolder and converts it to Excel.
    """
    print(f"\n--- Processing Other Reports (32) ---")
    reports_to_process = { "Relatorio 32.csv": "Relatorio 32.xlsx" }
    found_any = False
    for csv_name, excel_name in reports_to_process.items():
        source_path = os.path.join(main_reports_path, csv_name)
        if os.path.exists(source_path):
            found_any = True
            try:
                destination_excel_path = os.path.join(other_reports_subfolder_path, excel_name)
                print(f"Converting '{csv_name}' to Excel...")
                df = pd.read_csv(source_path, delimiter=',', encoding='utf-16', low_memory=False)
                df.to_excel(destination_excel_path, index=False)
                print(f"-> Successfully created '{excel_name}' in '{OTHER_REPORTS_SUBFOLDER_NAME}'.")
                os.remove(source_path)
            except Exception as e:
                print(f"-> ERROR: Could not process '{csv_name}'. Reason: {e}")
    if not found_any:
        print("No reports for 'Outros_relatorios' were found to process.")

def Create_Compare_Table(reports_path, base_path):
    print("\n--- Running Create_Compare_Table (Placeholder) ---")
    return 0

# --- Main Execution Block ---
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

    # --- Create all necessary directories ---
    os.makedirs(reports_path, exist_ok=True)
    os.makedirs(os.path.join(reports_path, MODELS_SUBFOLDER_NAME_61), exist_ok=True)
    os.makedirs(os.path.join(reports_path, MODELS_SUBFOLDER_NAME_29), exist_ok=True)
    other_reports_folder_path = os.path.join(reports_path, OTHER_REPORTS_SUBFOLDER_NAME)
    os.makedirs(other_reports_folder_path, exist_ok=True)

    try:
        with open(os.path.join(base_path, JSON_CREDENTIALS_FILE), 'r', encoding='utf-8') as f:
            credentials = json.load(f)
    except Exception as e:
        print(f"FATAL: Could not load credentials. Error: {e}")
        exit()

    # --- Phase 1: Download all reports concurrently ---
    print("--- 🚀 Starting All Report Downloads Concurrently ---")
    
    all_threads = []
    
    for report_id, report_name in REPORTS_TO_DOWNLOAD:
        thread = None
        if report_id == "61":
            thread = threading.Thread(
                target=process_report_61,
                args=(report_name, driver_path, reports_path, credentials, base_path),
                name=f"Report-{report_id}"
            )
        elif report_id == "29":
            thread = threading.Thread(
                target=process_report_29,
                args=(report_name, driver_path, reports_path, credentials, base_path),
                name=f"Report-{report_id}"
            )
        elif report_id == "32":
            thread = threading.Thread(
                target=download_standard_report,
                args=(report_id, report_name, driver_path, reports_path, credentials),
                name=f"Report-{report_id}"
            )
        
        if thread:
            all_threads.append(thread)
            thread.start()
            time.sleep(2) # Stagger thread starts slightly

    # Wait for all download threads to complete
    for thread in all_threads:
        thread.join()
    
    print("\n--- ✅ All download tasks have finished. ---")

    # --- Phase 2: Post-Processing (runs sequentially) ---
    print("\n--- 🔄 Starting Post-Processing ---")
    
    # Process Report 61 files
    merge_models_61(reports_path, base_path)
    process_merged_report_61(reports_path)
    
    # Process Report 29 files
    merge_models_29(reports_path, base_path)
    process_merged_report_29(reports_path)
    
    # Process Other Reports (32)
    process_other_reports(reports_path, other_reports_folder_path)

    # Final placeholder function
    Create_Compare_Table(reports_path, base_path)

    print("\n--- ✨ Full process completed. ---")