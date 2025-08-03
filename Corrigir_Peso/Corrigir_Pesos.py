from playwright import sync_api
import json

with open("Usuario.json", "r") as file:
    data = json.load(file)  # Parses JSON into a Python dict

    username = data["Usuario"]
    password = data["Senha"]

def main():
   
    with sync_api.sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto("https://eper-ltm.parts.fiat.com/navi?EU=1&eperLogin=0&sso=false&COUNTRY=076&RMODE=DEFAULT&SEARCH_TYPE=codpart&KEY=HOME&PART_COD=77994840&CHASSIS_NO&MVS")
        page.fill("input[name='username']", username)
        page.fill("input[name='password']", password)
        page.select_option("select[name='loginType']", "Fiat AUTO/MyUser/Link.e.entry")
        page.click("input[type='button']")
        page.fill("input[id='fPNumber']", "779948400")
        page.wait_for_selector("span.Btn_Box_Ricerca")
        page.keyboard.press("Enter")

        
        try:
            page.wait_for_load_state("networkidle")  # Or use "domcontentloaded"
            labels = page.locator("td.part_details_label")
            values = page.locator("td.part_details_value")

            for i in range(labels.count()):
                label_text = labels.nth(i).inner_text().strip()
                if "Peso em gramas:" in label_text:
                    peso_value = values.nth(i).inner_text().strip()
                    print("Peso em gramas:", peso_value)
                    break
                else:
                    print("Label 'Peso em gramas' not found.")
        except Exception as e:
            print("Error:", e)

        # browser.close()

if __name__ == "__main__":
    main()