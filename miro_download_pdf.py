import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
import time

# Miro API credentials
MIRO_ACCESS_TOKEN = 'your_miro_api_access_token'


# Function to get all Miro boards
def get_miro_boards():
    headers = {
        'Authorization': f'Bearer {MIRO_ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    response = requests.get('https://api.miro.com/v2/boards', headers=headers)

    if response.status_code == 200:
        boards = response.json()['data']
        return boards
    else:
        print(f"Failed to retrieve boards: {response.status_code} - {response.text}")
        return []


# Function to export a board to PDF via Selenium
def export_board_to_pdf(board_url, download_dir):
    # Initialize the Selenium WebDriver
    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)

    # Navigate to the board URL
    driver.get(board_url)

    # Add time to manually log in if needed
    time.sleep(15)  # Adjust this to suit your login time

    # Find and click the "Export" button
    export_button = driver.find_element(By.XPATH, '//button[@data-testid="export-pdf-button"]')
    export_button.click()

    # Wait for download (adjust time based on your needs)
    time.sleep(30)

    driver.quit()


if __name__ == "__main__":
    # Get all boards
    boards = get_miro_boards()

    # Directory to save PDFs
    download_directory = "/path/to/save/pdfs"

    for board in boards:
        board_url = board['viewLink']
        board_name = board['name']
        print(f"Exporting board: {board_name}")

        # Export the board to PDF
        export_board_to_pdf(board_url, download_directory)

        print(f"Board '{board_name}' exported successfully!")
