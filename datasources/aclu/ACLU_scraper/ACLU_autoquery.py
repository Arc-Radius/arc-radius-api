import os
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


URL = "https://www.aclu.org/legislative-attacks-on-lgbtq-rights"
DOWNLOAD_DIR = Path(__file__).resolve().parent / "downloads"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


def make_driver(download_dir: Path) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    # If you want to see the browser, comment this out:
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,900")

    # Download settings
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=opts)

    # Allow downloads in headless mode (Selenium 4 + Chrome)
    driver.execute_cdp_cmd(
        "Page.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": str(download_dir)},
    )
    return driver


def wait_for_download(download_dir: Path, timeout_s: int = 60) -> Path:
    """Wait for a new non-.crdownload file to appear."""
    end = time.time() + timeout_s
    while time.time() < end:
        files = list(download_dir.glob("*"))
        # Ignore temporary Chrome partial downloads
        completed = [f for f in files if f.is_file() and not f.name.endswith(".crdownload")]
        if completed:
            # pick the newest
            return max(completed, key=lambda p: p.stat().st_mtime)
        time.sleep(0.5)
    raise TimeoutError(f"No completed download found in {download_dir} within {timeout_s}s")


def main():
    # Clean old downloads (optional)
    for f in DOWNLOAD_DIR.glob("*"):
        try:
            f.unlink()
        except OSError:
            pass

    driver = make_driver(DOWNLOAD_DIR)
    wait = WebDriverWait(driver, 30)

    try:
        driver.get(URL)

        # Some sites have cookie banners; try to dismiss if present (safe no-op if not)
        for selector in [
            "button[aria-label='Accept']",        # generic
            "button:has-text('Accept')",          # not supported by Selenium CSS; kept for reference
            "button[mode='primary']",
        ]:
            try:
                btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                btn.click()
                break
            except Exception:
                pass

        # Find the link/button by its visible text
        download_el = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//a[contains(normalize-space(.), 'Download CSV of data') or "
                           "contains(@aria-label, 'Download CSV') or "
                           "contains(@title, 'Download CSV')]")
            )
        )

        # Scroll into view before clicking (helps with overlays)
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", download_el)
        time.sleep(0.2)
        download_el.click()

        csv_path = wait_for_download(DOWNLOAD_DIR, timeout_s=90)
        print(f"✅ Downloaded: {csv_path}")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
