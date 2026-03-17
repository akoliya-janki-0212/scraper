import argparse
import csv
import os
import random
import time
import traceback
from datetime import datetime
from urllib.parse import quote_plus

from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from gscrapperci import (
    setup_driver,
    handle_captcha,
    get_product_options,
    normalize_url_path_slug,
)


def build_search_url(keyword):
    return f"https://www.google.com/search?q={quote_plus(keyword)}&udm=28&gl=US&hl=en&pws=0"


def get_text_safe(element, by, selector):
    try:
        return element.find_element(by, selector).text.strip()
    except Exception:
        return ""


def get_attr_safe(element, by, selector, attr):
    try:
        return element.find_element(by, selector).get_attribute(attr)
    except Exception:
        return ""


def normalize_name_key(name):
    return " ".join((name or "").lower().split())


def scroll_results_to_bottom(driver, max_products=0, idle_rounds=5, max_rounds=120):
    last_count = -1
    stable = 0
    for _ in range(max_rounds):
        cards = driver.find_elements(By.CLASS_NAME, "MtXiu")
        count = len(cards)
        if max_products > 0 and count >= max_products:
            break
        if count == last_count:
            stable += 1
        else:
            stable = 0
            last_count = count
        if stable >= idle_rounds:
            break
        # Incremental scrolling works better with lazy-loaded result cards.
        driver.execute_script("window.scrollBy(0, Math.max(700, window.innerHeight * 0.85));")
        time.sleep(random.uniform(0.8, 1.5))


def collect_all_products(driver, keyword, search_url, max_products=0):
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "dURPMd")))
    scroll_results_to_bottom(driver, max_products=max_products)

    # mains = driver.find_element(By.CLASS_NAME, "dURPMd")
    products = driver.find_elements(By.CLASS_NAME, "MtXiu")
    if max_products > 0:
        products = products[:max_products]

    collected = []
    for idx, product in enumerate(products, start=1):
        cid = product.get_attribute("id") or ""
        product_name = get_text_safe(product, By.XPATH, ".//div[contains(@class,'gkQHve')]")
        seller = get_text_safe(product, By.XPATH, ".//span[contains(@class,'WJMUdc')]")
        if not cid:
            continue
        collected.append(
            {
                "product_id": str(idx),
                "keyword": keyword,
                "url": search_url,
                "cid": cid,
                "product_name": product_name,
                "seller": seller,
            }
        )
    return collected


def chunk_slice(items, chunk_id, total_chunks):
    if total_chunks <= 0:
        return items, 0, len(items)
    total_rows = len(items)
    rows_per_chunk = total_rows // total_chunks
    start_idx = (chunk_id - 1) * rows_per_chunk
    end_idx = chunk_id * rows_per_chunk if chunk_id < total_chunks else total_rows
    return items[start_idx:end_idx], start_idx, end_idx


def click_product_by_offset(driver, start_offset, target_name="", processed_names=None):
    processed_names = processed_names or set()
    target_key = normalize_name_key(target_name)
    current_offset = max(0, start_offset)

    last_count = -1
    stale_rounds = 0
    for _ in range(45):
        try:
            cards = driver.find_elements(By.CLASS_NAME, "MtXiu")
            count = len(cards)
            if count == last_count:
                stale_rounds += 1
            else:
                stale_rounds = 0
                last_count = count

            while current_offset < len(cards):
                card = cards[current_offset]
                card_name = get_text_safe(card, By.XPATH, ".//div[contains(@class,'gkQHve')]")
                card_name_key = normalize_name_key(card_name)

                if card_name_key and card_name_key in processed_names:
                    current_offset += 1
                    continue

                if target_key and card_name_key and card_name_key != target_key:
                    current_offset += 1
                    continue

                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", card)
                time.sleep(0.4)
                try:
                    WebDriverWait(driver, 5).until(EC.element_to_be_clickable(card))
                except Exception:
                    pass
                card.click()
                return {
                    "cid": card.get_attribute("id") or "",
                    "product_name": card_name,
                    "seller": get_text_safe(card, By.XPATH, ".//span[contains(@class,'WJMUdc')]"),
                }, current_offset + 1

            if stale_rounds >= 4:
                break
            driver.execute_script("window.scrollBy(0, Math.max(650, window.innerHeight * 0.8));")
            time.sleep(random.uniform(0.7, 1.3))
        except Exception:
            time.sleep(0.6)
    return None, current_offset


def extract_share_url(driver):
    share_url = ""
    try:
        share_button = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//div[contains(@class,'RSNrZe') and @role='button' and @aria-label='Share']")
            )
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", share_button)
        share_button.click()

        share_dialog = WebDriverWait(driver, 8).until(
            EC.visibility_of_element_located((By.XPATH, "//div[@role='dialog' and @aria-label='Share']"))
        )

        try:
            share_input = share_dialog.find_element(By.CSS_SELECTOR, "input[aria-label='Share link'][type='url']")
            share_url = (share_input.get_attribute("value") or "").strip()
        except Exception:
            share_url = ""

        if not share_url:
            try:
                share_url = share_dialog.find_element(By.CSS_SELECTOR, "div[jsname='tQ9n1c']").text.strip()
            except Exception:
                share_url = ""

        try:
            close_button = share_dialog.find_element(By.CSS_SELECTOR, "[jsname='tqp7ud']")
            close_button.click()
        except Exception:
            ActionChains(driver).send_keys(u"\ue00c").perform()  # ESC
    except Exception:
        share_url = ""
    return share_url


def scrape_product_for_meta(driver, meta, search_url, start_offset=0, processed_names=None):
    processed_names = processed_names or set()
    result = {
        "product_id": meta["product_id"],
        "keyword": meta["keyword"],
        "url": search_url,
        "last_response": "",
        "osb_url_match": "",
        "product_url": "",
        "seller": meta.get("seller", ""),
        "product_name": meta.get("product_name", ""),
        "cid": meta.get("cid", ""),
        "pid": "",
        "last_fetched_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "osb_position": 0,
        "osb_id": "",
        "seller_count": 0,
        "status": "",
        "competitors": [],
    }

    try:
        time.sleep(random.uniform(0.8, 1.5))
        clicked_meta, next_offset = click_product_by_offset(
            driver,
            start_offset=start_offset,
            target_name=meta.get("product_name", ""),
            processed_names=processed_names,
        )
        if not clicked_meta:
            result["status"] = "product_not_clickable"
            result["last_response"] = (
                f"Unable to click product from offset={start_offset}, expected_name={meta.get('product_name', '')}"
            )
            return result, next_offset

        if clicked_meta.get("product_name"):
            result["product_name"] = clicked_meta["product_name"]
        if clicked_meta.get("seller"):
            result["seller"] = clicked_meta["seller"]
        if clicked_meta.get("cid"):
            result["cid"] = clicked_meta["cid"]

        time.sleep(random.uniform(1.0, 2.0))
        result["product_url"] = extract_share_url(driver) or driver.current_url

        # Expand all available store rows before collecting seller data.
        max_more_store_clicks = 30
        for _ in range(max_more_store_clicks):
            try:
                more_stores = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'duf-h')]//div[@role='button']"))
                )
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", more_stores)
                more_stores.click()
                time.sleep(random.uniform(1.0, 1.8))
            except Exception:
                break

        offers_grid = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[@jsname='RSFNod' and @data-attrid='organic_offers_grid']"))
        )

        if driver.find_elements(By.XPATH, "//div[contains(@class,'iI1aN')]//div[@class='EDblX kjqWgb']"):
            result["options"] = get_product_options(driver)

        offer_elements = offers_grid.find_elements(By.CLASS_NAME, "R5K7Cb")
        competitors = []
        for seller_html in offer_elements:
            store_name = get_text_safe(seller_html, By.CSS_SELECTOR, "div.hP4iBf.gUf0b.uWvFpd") or "N/A"
            seller_product_name = get_text_safe(seller_html, By.CSS_SELECTOR, "div.Rp8BL") or "N/A"
            seller_url = get_attr_safe(seller_html, By.CSS_SELECTOR, "a.P9159d", "href") or "N/A"
            seller_price = (
                get_text_safe(seller_html, By.CSS_SELECTOR, "div.QcEgce span[aria-hidden='true']")
                or get_text_safe(seller_html, By.CSS_SELECTOR, "div.GBgquf span")
                or "N/A"
            )

            competitor_data = {
                "product_id": meta["product_id"],
                "seller": store_name,
                "seller_product_name": seller_product_name,
                "seller_url": seller_url,
                "seller_price": seller_price,
                "last_fetched_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            competitors.append(competitor_data)
            result["competitors"].append(competitor_data)

        search_seller = "1StopBedrooms"
        sellers = [c["seller"] for c in competitors]
        osb_position = sellers.index(search_seller) + 1 if search_seller in sellers else 0
        osb_id = ""
        if osb_position:
            for competitor in competitors:
                if competitor["seller"] == search_seller:
                    osb_id = normalize_url_path_slug(competitor.get("seller_url", ""))
                    break

        result.update(
            {
                "osb_position": osb_position,
                "seller_count": len(sellers),
                "osb_id": osb_id,
                "status": "completed",
                "last_response": f"Completed - OSB Position: {osb_position}, Total Sellers: {len(sellers)}",
            }
        )
        return result, next_offset
    except Exception as e:
        result["status"] = "error"
        result["last_response"] = f"Error: {str(e)}"
    return result, start_offset


def append_product_row(csv_path, result):
    osb_id = result.get('osb_id', '')
    osb_url = f"https://www.1stopbedrooms.com/{osb_id}" if osb_id else ""
    row = {
        "product_id": result.get("product_id", ""),
        # "web_id": "",
        # "name": "",
        # "mpn_sku": "",
        # "gtin": "",
        # "brand": "",
        # "category": "",
        "keyword": result.get("keyword", ""),
        "url": result.get("url", ""),
        "osb_url": osb_url,
        "last_response": result.get("last_response", ""),
        "osb_url_match": result.get("osb_url_match", ""),
        "product_url": result.get("product_url", ""),
        "seller": result.get("seller", ""),
        "product_name": result.get("product_name", ""),
        "cid": result.get("cid", ""),
        "pid": result.get("pid", ""),
        "last_fetched_date": result.get("last_fetched_date", ""),
        "osb_position": result.get("osb_position", 0),
        "osb_id": osb_id,
        "seller_count": result.get("seller_count", 0),
        "status": result.get("status", "error"),
    }

    file_exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def append_seller_rows(csv_path, competitors):
    if not competitors:
        return
    file_exists = os.path.exists(csv_path)
    fields = ["product_id", "seller", "seller_product_name", "seller_url", "seller_price", "last_fetched_date"]
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if not file_exists:
            writer.writeheader()
        for row in competitors:
            writer.writerow({k: row.get(k, "") for k in fields})


def process_keyword_chunk(keyword, chunk_id, total_chunks, max_products=0):
    search_url = build_search_url(keyword)
    print(f"Keyword: {keyword}")
    print(f"Search URL: {search_url}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    csv1_path = os.path.join(output_dir, f"product_info_chunk{chunk_id}_{timestamp}.csv")
    csv2_path = os.path.join(output_dir, f"seller_info_chunk{chunk_id}_{timestamp}.csv")

    driver = setup_driver()
    try:
        driver.get(search_url)
        captcha_result = handle_captcha(driver, search_url)
        if captcha_result == "failed":
            print("Captcha solving failed for initial search page.")
            return False

        products = collect_all_products(driver, keyword, search_url, max_products=max_products)
        print(f"Total discovered products: {len(products)}")
        if not products:
            print("No products discovered.")
            return False

        chunk_products, start_idx, end_idx = chunk_slice(products, chunk_id, total_chunks)
        if total_chunks <= 0:
            print(f"Chunk mode disabled (total_chunks=0): processing all {len(chunk_products)} products in one run")
        else:
            print(f"Chunk {chunk_id}: products {start_idx + 1} to {end_idx} ({len(chunk_products)} rows)")

        processed_names = set()
        next_offset = start_idx if total_chunks > 0 else 0

        for idx, meta in enumerate(chunk_products, start=1):
            name_key = normalize_name_key(meta.get("product_name", ""))
            if name_key and name_key in processed_names:
                print(f"\nSkipping duplicate {idx}/{len(chunk_products)} - {meta.get('product_name', '')}")
                continue

            print(f"\nProcessing {idx}/{len(chunk_products)} - Offset: {next_offset} - Name: {meta.get('product_name', '')}")
            result, next_offset = scrape_product_for_meta(
                driver,
                meta,
                search_url,
                start_offset=next_offset,
                processed_names=processed_names,
            )
            processed_key = normalize_name_key(result.get("product_name", "")) or name_key
            if processed_key:
                processed_names.add(processed_key)
            append_product_row(csv1_path, result)
            append_seller_rows(csv2_path, result.get("competitors", []))
            time.sleep(random.uniform(1.2, 2.4))

        print(f"✓ Saved product info: {os.path.basename(csv1_path)}")
        print(f"✓ Saved seller info: {os.path.basename(csv2_path)}")
        return True
    except Exception as e:
        print(f"Error processing keyword chunk: {str(e)}")
        traceback.print_exc()
        return False
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="Google Shopping Keyword Scraper")
    parser.add_argument("--chunk-id", type=int, default=1, help="Chunk ID (1-based)")
    parser.add_argument("--total-chunks", type=int, default=0, help="Total number of chunks (0 = all products)")
    parser.add_argument("--max-products", type=int, default=0, help="Maximum products to fetch (0 = no limit)")
    parser.add_argument("--keyword", type=str, required=True, help="Keyword to search on Google Shopping")
    args = parser.parse_args()

    print("=" * 60)
    print("Google Shopping Keyword Scraper")
    print(f"Chunk: {args.chunk_id} of {args.total_chunks}")
    print(f"Max products: {args.max_products if args.max_products > 0 else 'no limit'}")
    print(f"Keyword: {args.keyword}")
    print("=" * 60)

    ok = process_keyword_chunk(args.keyword, args.chunk_id, args.total_chunks, args.max_products)
    if ok:
        print("\n✓ Processing completed successfully")
        raise SystemExit(0)
    print("\n✗ Processing failed")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
