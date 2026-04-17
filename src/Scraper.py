from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
import pandas as pd
import time
import os
import shutil

def scrape_matches():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")


    browser_path = shutil.which("chromium-browser") or shutil.which("chromium") or shutil.which("google-chrome")
    if browser_path:
        options.binary_location = browser_path

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()),
        options=options
    )

    url = "https://www.flashscore.com/football/mauritania/super-d1/results/"
    driver.get(url)
    time.sleep(8)

    matches = []
    rows = driver.find_elements(By.CSS_SELECTOR, "div.event__match")

    for row in rows:
        try:
            home = row.find_element(By.CSS_SELECTOR, ".event__homeParticipant").text
            away = row.find_element(By.CSS_SELECTOR, ".event__awayParticipant").text
            date_info = row.find_element(By.CSS_SELECTOR, ".event__time").text
            
            try:
                score_home = row.find_element(By.CSS_SELECTOR, ".event__score--home").text
                score_away = row.find_element(By.CSS_SELECTOR, ".event__score--away").text
            except:
                continue

            if score_home == "" or score_away == "":
                continue

            matches.append({
                'season': '2025/2026',
                'date': date_info.split(' ')[0],
                'home_team': home,
                'away_team': away,
                'home_goals': int(score_home),
                'away_goals': int(score_away)
            })
        except Exception:
            continue

    driver.quit()
    return matches

def main():
    FILE = "scraped_current_season.csv"
    
    data_list = scrape_matches()
    if not data_list:
        print("Aucune donnee extraite.")
        return

    new_df = pd.DataFrame(data_list)

    if os.path.exists(FILE):
        old_df = pd.read_csv(FILE)
        combined = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.drop_duplicates(subset=['date', 'home_team', 'away_team'], keep='last')
    
    combined.to_csv(FILE, index=False)

    print("-" * 30)
    print(f"Fichier : {FILE}")
    print(f"Total matchs : {len(combined)}")
    print("-" * 30)

if __name__ == "__main__":
    main()
