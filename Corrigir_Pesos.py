import pandas as pd
import os
import json
import time
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# credentials = {
#   "Usuario": "SC74349",
#   "Senha": "Caneca050315"
# }
# pns_for_scraping = ["77994840","34343424"]
def E_PER(pns_for_scraping, credentials):
    """
    Scrapes the E-PER website to find the weights of given part numbers.

    This function logs into the website, iterates through a list of part numbers,
    searches for each one, extracts its weight in grams, converts it to kilograms,
    and returns the results.

    Args:
        pns_for_scraping (list): A list of part number strings to search for.
        credentials (dict): A dictionary with "Usuario" and "Senha" for login.

    Returns:
        dict: A dictionary mapping part numbers to their found weights in kg.
              Example: {'779948400': 0.123, 'PN456': 1.8}
    """
    print("\n--- Sending PNs to E_PER for web scraping ---")
    if not pns_for_scraping:
        print("No part numbers needed for scraping.")
        return {}

    print(f"Found {len(pns_for_scraping)} part numbers with weight 1 to check:")
    for pn in pns_for_scraping:
        print(f"  - {pn}")

    username = credentials["Usuario"]
    password = credentials["Senha"]
    scraped_weights = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False) # Use headless=True for background execution
        page = browser.new_page()
        
        try:
            # --- Login ---
            print("Logging into E-PER...")
            page.goto("https://eper-ltm.parts.fiat.com/navi?EU=1&eperLogin=0&sso=false&COUNTRY=076&RMODE=DEFAULT&SEARCH_TYPE=codpart&KEY=HOME")
            page.fill("input[name='username']", username)
            page.fill("input[name='password']", password)
            page.select_option("select[name='loginType']", "Fiat AUTO/MyUser/Link.e.entry")
            page.click("input[type='button']")
            page.wait_for_load_state("networkidle", timeout=60000) # Wait for login to complete
            print("Login successful.")

            pns_for_scraping.append("77994840")
            
            # --- Loop through each Part Number ---
            for pn in pns_for_scraping:
                weight_found = False
                print(f"\nSearching for PN: {pn}...")
                try:
                    # Navigate to the search page and enter the part number
                    page.fill("input[id='fPNumber']", pn)
                    page.keyboard.press("Enter")
                    
                    # Wait for the results to load
                    page.wait_for_load_state("networkidle", timeout=50000)
                    time.sleep(2)

                    labels = page.locator("td.part_details_label")
                    values = page.locator("td.part_details_value")

                    for i in range(labels.count()):

                        label_text = labels.nth(i).inner_text().strip()

                        if "Peso em gramas:" in label_text:
                            peso_value = values.nth(i).inner_text().strip()
                            # print("Peso em gramas:", peso_value)
                            peso_kg = float(peso_value.replace(',', '.')) / 1000
                            scraped_weights[pn] = peso_kg
                            print(f"  ✅ Weight found for {pn}: {peso_value} g -> {peso_kg} kg")

                        else :
                            continue  
                        print(f"  ⚠️ Label 'Peso em gramas:' not found in the details for PN: {pn}")  
                        weight_found = True
                        break
                
                    
                except PlaywrightTimeoutError:
                    print(f"  ❌ Timeout error while searching for PN: {pn}. It might not exist or the page took too long to load.")
                except Exception as e:
                    print(f"  ❌ An unexpected error occurred for PN {pn}: {e}")
                
                # A short delay to prevent overwhelming the server
                time.sleep(1)

        except Exception as e:
            print(f"❌ A critical error occurred during the Playwright session: {e}")
        finally:
            print("\n--- E_PER scraping complete ---")
            browser.close()
            
    return scraped_weights



# E_PER(pns_for_scraping, credentials)
