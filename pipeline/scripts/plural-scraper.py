"""
Plural Policy Bill Scraper
Scrapes all bills from a tagged bills page with pagination support.

Requirements:
    pip install playwright pandas
    playwright install chromium

Usage:
    python scrape_plural_policy.py
"""

import asyncio
import csv
import json
from datetime import datetime
from playwright.async_api import async_playwright

# Configuration
URL = "https://pluralpolicy.com/app/tagged-bills/32114"
OUTPUT_FILE = "pro_lgbtq_bills.csv"


async def scrape_bills():
    all_bills = []

    async with async_playwright() as p:
        # Launch browser (set headless=True to run without UI)
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        # Increase default timeout
        page.set_default_timeout(60000)  # 60 seconds

        print(f"Navigating to {URL}...")

        # Use 'load' instead of 'networkidle' (less strict)
        await page.goto(URL, wait_until="load", timeout=60000)

        # Wait for bills to load
        print("Waiting for bill cards to appear...")
        await page.wait_for_selector(".card.list-item", timeout=60000)

        # Extra wait for dynamic content
        await asyncio.sleep(3)

        # Get total bill count
        try:
            total_text = await page.locator(".total-count span").inner_text()
            total_bills = int(total_text.replace(
                " bills", "").replace(",", ""))
            print(f"Found {total_bills} total bills to scrape")
        except:
            print("Could not determine total bill count, continuing anyway...")

        page_num = 1

        while True:
            print(f"Scraping page {page_num}...")

            # Wait for cards to be visible
            await page.wait_for_selector(".card.list-item", timeout=30000)
            await asyncio.sleep(2)  # Brief pause for content to fully render

            # Extract bills from current page
            bills = await page.evaluate(r"""
                () => {
                    const bills = [];
                    document.querySelectorAll('.card.list-item').forEach(card => {
                        bills.push({
                            status: card.querySelector('[data-name="bill-status"]')?.innerText?.trim() || '',
                            state: card.querySelector('[data-name="bill-state"]')?.innerText?.trim() || '',
                            bill_number: card.querySelector('[data-name="bill-number"]')?.innerText?.trim() || '',
                            description: card.querySelector('[data-name="bill-name"]')?.innerText?.trim() || '',
                            sponsors: card.querySelector('[data-name="bill-sponsors"]')?.innerText?.replace(/\\n/g, ', ')?.trim() || '',
                            committee: card.querySelector('.footnote .col-md-8')?.innerText?.trim() || '',
                            session: card.querySelector('.session')?.innerText?.split('\\n')[0]?.trim() || '',
                            latest_action: (card.querySelector('.session')?.innerText?.match(/LATEST ACTION:\s*(.+)/i)?.[1] || '').trim(),
                            bill_id: card.id || ''
                        });
                    });
                    return bills;
                }
            """)

            all_bills.extend(bills)
            print(f"  Collected {len(bills)} bills (Total: {len(all_bills)})")

            # Check if there's a Next button that's not disabled
            next_button = page.locator(
                "li.page-item:has-text('Next'):not(.disabled) button")

            if await next_button.count() > 0:
                await next_button.click()
                # Wait for page transition
                await asyncio.sleep(2)
                page_num += 1
            else:
                print("Reached last page")
                break

        await browser.close()

    return all_bills


def save_to_csv(bills, filename):
    """Save bills to CSV file"""
    if not bills:
        print("No bills to save")
        return

    fieldnames = ['status', 'state', 'bill_number', 'description', 'sponsors',
                  'committee', 'session', 'latest_action', 'bill_id']

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(bills)

    print(f"Saved {len(bills)} bills to {filename}")


def save_to_json(bills, filename):
    """Save bills to JSON file"""
    json_filename = filename.replace('.csv', '.json')
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(bills, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(bills)} bills to {json_filename}")


async def main():
    print("=" * 50)
    print("Plural Policy Bill Scraper")
    print("=" * 50)

    start_time = datetime.now()

    # Scrape all bills
    bills = await scrape_bills()

    # Save results
    save_to_csv(bills, OUTPUT_FILE)
    save_to_json(bills, OUTPUT_FILE)

    elapsed = datetime.now() - start_time
    print(f"\nCompleted in {elapsed.seconds} seconds")
    print(f"Total bills scraped: {len(bills)}")


if __name__ == "__main__":
    asyncio.run(main())
