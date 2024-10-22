from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import logging
import time
from datetime import datetime

def setup_chrome_driver():
    """Setup Chrome driver with appropriate options"""
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Specify Chrome binary location if needed
    chrome_options.binary_location = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def scrape_commodity_data(url):
    """
    Scrape commodity data and return as pandas DataFrame
    
    Args:
        url (str): URL to scrape
        
    Returns:
        pd.DataFrame: Scraped commodity data
    """
    driver = None
    retry_count = 3
    retry_delay = 5
    
    for attempt in range(retry_count):
        try:
            driver = setup_chrome_driver()
            driver.get(url)
            
            # Wait for table to load
            wait = WebDriverWait(driver, 30)
            table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            time.sleep(2)  # Ensure table is fully loaded
            
            # Extract data
            rows = table.find_elements(By.TAG_NAME, "tr")
            commodity_data = []
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if cells:
                    # Extract text from first cell (handling possible link)
                    first_cell_text = cells[0].text if cells[0].text else (
                        cells[0].find_elements(By.TAG_NAME, "a")[0].text 
                        if cells[0].find_elements(By.TAG_NAME, "a") 
                        else ""
                    )
                    
                    row_data = {
                        'PRODUCT_NAME': first_cell_text,
                        'CLEARING': cells[1].text if len(cells) > 1 else "",
                        'GLOBEX': cells[2].text if len(cells) > 2 else "",
                        'FLOOR': cells[3].text if len(cells) > 3 else "",
                        'CLEARPORT': cells[4].text if len(cells) > 4 else "",
                        'EXCH': cells[5].text if len(cells) > 5 else "",
                        'ASSET_CLASS': cells[6].text if len(cells) > 6 else "",
                        'PRODUCT_GROUP': cells[7].text if len(cells) > 7 else "",
                        'CATEGORY': cells[8].text if len(cells) > 8 else "",
                        'SUB_CATEGORY': cells[9].text if len(cells) > 9 else "",
                        'CLEARED_AS': cells[10].text if len(cells) > 10 else "",
                        'VOLUME': cells[11].text if len(cells) > 11 else "",
                        'OPEN_INTEREST': cells[12].text if len(cells) > 12 else ""
                    }
                    commodity_data.append(row_data)
            
            if commodity_data:
                return pd.DataFrame(commodity_data)
            else:
                raise Exception("No data found in table")
                
        except Exception as e:
            print(f"Error during scraping attempt {attempt + 1}: {str(e)}")
            if attempt < retry_count - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise Exception("All retry attempts failed")
        
        finally:
            if driver:
                driver.quit()

def get_commodity_data(url):
    """
    Main function to get commodity data as DataFrame
    
    Returns:
        pd.DataFrame: Scraped commodity data
    """
    #url = "https://www.cmegroup.com/markets/products.html#subGroups=1&sortDirection=desc&sortField=oi"
    try:
        df = scrape_commodity_data(url)
        print(f"Successfully scraped {len(df)} rows of data")
        return df
    except Exception as e:
        print(f"Error getting commodity data: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error