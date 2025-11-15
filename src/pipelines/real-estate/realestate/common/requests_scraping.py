# realestate/common/requests_scraping.py
"""
Alternative scraping using requests + BeautifulSoup instead of Selenium
Bypasses all Chrome DNS and compatibility issues
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import logging
from typing import List, Optional
import re

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Realistic headers to avoid blocking
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

def _normalize_url(url: str) -> str:
    """Chuyển URL tương đối thành URL tuyệt đối hợp lệ."""
    if not url:
        return url.strip()
    url = url.strip()

    # Nếu không có http, thêm prefix domain chính
    if not url.startswith("http"):
        # đảm bảo có dấu /
        if not url.startswith("/"):
            url = "/" + url
        url = "https://nhadat247.com.vn" + url

    # Nếu là dạng //ban-nha..., thêm https:
    elif url.startswith("//"):
        url = "https:" + url

    return url


def extract_coordinates(html_content: str):
    """Extract latitude and longitude from HTML content"""
    # Try multiple patterns for coordinates
    coord_patterns = [
        r'place\?q=([-+]?\d*\.\d+),([-+]?\d*\.\d+)',  # Google Maps place
        r'([-+]?\d{1,3}\.\d{4,}),\s*([-+]?\d{1,3}\.\d{4,})',  # Lat,Lng format
        r'"lat"\s*:\s*([-+]?\d*\.\d+).*?"lng"\s*:\s*([-+]?\d*\.\d+)',  # JSON lat/lng
        r'latitude["\']\s*:\s*["\']?([-+]?\d*\.\d+)["\']?.*?longitude["\']\s*:\s*["\']?([-+]?\d*\.\d+)["\']?',  # JSON format
        r'center=([-+]?\d*\.\d+),([-+]?\d*\.\d+)',  # Map center
        r'@([-+]?\d*\.\d+),([-+]?\d*\.\d+)',  # Google Maps @
    ]

    for pattern in coord_patterns:
        matches = re.findall(pattern, html_content, re.IGNORECASE)
        for match in matches:
            try:
                if isinstance(match, tuple) and len(match) == 2:
                    # Try both lng,lat and lat,lng orders
                    val1 = float(match[0])
                    val2 = float(match[1])

                    # Check which order makes sense for Vietnam
                    # Vietnam lat: 8-24, lng: 102-110
                    if 8 <= val1 <= 24 and 102 <= val2 <= 110:
                        # val1 is lat, val2 is lng
                        return [val1, val2]
                    elif 102 <= val1 <= 110 and 8 <= val2 <= 24:
                        # val1 is lng, val2 is lat - swap them
                        return [val2, val1]
            except (ValueError, TypeError, IndexError):
                continue

    return [None, None]

def extract_property_urls_single_page(html_content: str) -> List[str]:
    soup = BeautifulSoup(html_content, "html.parser")
    links = [a.get("href") for a in soup.select('a[href*="-pid"]') if a.get("href")]
    links = [_normalize_url(l) for l in links]
    return links

def process_single_property(url: str) -> dict:
    """Process single property using requests"""
    url = _normalize_url(url)
    logger.debug("Processing property: %s", url)

    try:
        # Add random delay to be respectful
        time.sleep(random.uniform(1.0, 3.0))

        # Ensure URL is valid
        if not url.startswith('http'):
            logger.warning("Invalid URL: %s", url)
            return {}

        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        titles = []
        values = []

        # Extract title - try multiple selectors based on debug
        title_elem = (soup.find("h1") or
                     soup.find("h1", class_=re.compile(r'title|heading', re.IGNORECASE)) or
                     soup.find("title"))
        if title_elem:
            titles.append("Tiêu đề")
            values.append(title_elem.get_text(strip=True))

        # Extract address - try multiple selectors based on actual HTML structure
        addr_elem = soup.find("span", class_=re.compile(r're__pr-short-description', re.IGNORECASE))
        if addr_elem and addr_elem.get_text(strip=True):
            addr_text = addr_elem.get_text(strip=True)
            # Clean up - keep only the address part, remove extra descriptions
            if ',' in addr_text:
                # Take the address part (usually before the comma with extra text)
                addr_parts = addr_text.split(',')
                addr_text = ','.join(addr_parts[:3])  # Keep first 3 parts: ward,district,city
            titles.append("Địa chỉ")
            values.append(addr_text.strip())
        else:
            # Fallback: look for spans containing location keywords
            location_spans = soup.find_all("span")
            for span in location_spans:
                text = span.get_text(strip=True)
                if any(loc in text for loc in ['Thanh Xuân', 'Hà Nội', 'TP.HCM', 'Đà Nẵng', 'Cần Thơ']):
                    # Clean up text - remove extra property details
                    clean_text = re.sub(r'Bán\s+.*?\d+(?:\.\d+)?\s*(?:tỷ|triệu).*?(?=\w)', '', text)
                    clean_text = re.sub(r'\d+(?:\.\d+)?\s*(?:tỷ|triệu).*', '', clean_text)
                    clean_text = re.sub(r'\d+\s*tầng.*', '', clean_text)
                    clean_text = re.sub(r'\d+m2.*', '', clean_text)
                    clean_text = clean_text.strip()
                    if clean_text and len(clean_text) > 3:
                        titles.append("Địa chỉ")
                        values.append(clean_text)
                        break

        # Extract price - use the input with class "js-gia-bds" which is most accurate
        price_text = None
        price_input = soup.find('input', class_='js-gia-bds')
        if price_input and price_input.get('value'):
            raw_value = price_input['value'].strip()
            # Clean to digits only
            cleaned = re.sub(r'\D', '', raw_value)
            if cleaned:
                price_text = cleaned
            else:
                # If no digits, check if page has "Thỏa thuận"
                if 'Thỏa thuận' in response.text or 'thỏa thuận' in response.text.lower():
                    price_text = 'Thỏa thuận'
                else:
                    # Fallback: use the raw value
                    price_text = raw_value
        
        if price_text:
            titles.append("Mức giá")
            values.append(price_text)

        # Extract area - find the value span in the item containing "Diện tích"
        area_text = None
        info_items = soup.find_all('div', class_=re.compile(r're__pr-short-info-item'))
        for item in info_items:
            if 'Diện tích' in item.get_text():
                area_value = item.find('span', class_=re.compile(r're__pr-specs-content-item-value|value'))
                if area_value:
                    area_text = area_value.get_text(strip=True)
                    break
        if not area_text:
            # Fallback: look for the area after "Diện tích" label
            area_elem = soup.find(text=re.compile(r'Diện tích', re.IGNORECASE))
            if area_elem:
                parent = area_elem.parent if area_elem.parent else area_elem
                area_match = re.search(r'(\d+(?:\.\d+)?\s*(?:m²|m2|mét|vuông))', parent.get_text(), re.IGNORECASE)
                if area_match:
                    area_text = area_match.group(1).strip()
        if not area_text:
            # Last fallback to the original regex on the whole page
            area_match = re.search(r'(\d+(?:\.\d+)?\s*(?:m²|m2|mét|vuông))', response.text, re.IGNORECASE)
            if area_match:
                area_text = area_match.group(1)
        
        if area_text:
            titles.append("Diện tích")
            values.append(area_text)

        # Extract coordinates
        lat, lon = extract_coordinates(response.text)
        logger.debug("Extracted coordinates: lat=%s, lon=%s", lat, lon)
        titles += ["latitude", "longitude"]
        values += [lat, lon]

        info = dict(zip(titles, values))
        info.setdefault("url", url)
        return info

    except requests.exceptions.RequestException as e:
        logger.warning("Request failed for %s: %s", url, e)
        return {}
    except Exception as e:
        logger.warning("Failed to process property %s: %s", url, e)
        return {}

def process_single_page(page_url: str, limit_each_page: int = 5) -> List[dict]:
    """Process single page using requests"""
    page_url = _normalize_url(page_url)
    logger.info("Scraping page: %s", page_url)

    try:
        # Add delay between pages
        time.sleep(random.uniform(2.0, 5.0))

        response = requests.get(page_url, headers=HEADERS, timeout=15)
        response.raise_for_status()

        # Check for blocking
        if "Cloudflare" in response.text or "Checking your browser" in response.text:
            logger.warning("Detected Cloudflare blocking on %s", page_url)
            return []

        urls = extract_property_urls_single_page(response.text)[:limit_each_page]
        logger.info("Found %d property URLs on page", len(urls))

        results = []
        for url in urls:
            property_data = process_single_property(url)
            if property_data:  # Only add if we got data
                results.append(property_data)

        logger.info("Successfully scraped %d properties from page %s", len(results), page_url)
        return results

    except Exception as e:
        logger.error("Failed to scrape page %s: %s", page_url, e)
        return []

def requests_scraping(urls: List[str], limit_each_page: int = 5) -> pd.DataFrame:
    """
    Main scraping function using requests instead of Selenium
    Much more reliable and no DNS/Chrome compatibility issues
    """
    if not urls:
        logger.info("No urls passed to requests_scraping")
        return _empty_df()

    urls = [_normalize_url(u) for u in urls]
    logger.info("🕷️ Starting requests-based scraping: %d URLs, limit_per_page=%d", len(urls), limit_each_page)

    results = []
    total_pages = len(urls)
    total_expected = total_pages * limit_each_page

    for i, url in enumerate(urls):
        logger.info("📄 Processing page %d/%d: %s", i+1, total_pages, url)
        try:
            page_results = process_single_page(url, limit_each_page=limit_each_page)
            valid_results = [r for r in page_results if r]  # Filter out empty dicts

            results.extend(valid_results)
            success_rate = len(valid_results) / limit_each_page * 100 if limit_each_page > 0 else 0

            logger.info("✅ Page %d/%d: %d/%d properties scraped (%.1f%% success)",
                       i+1, total_pages, len(valid_results), limit_each_page, success_rate)

        except Exception as e:
            logger.error("❌ Failed to process page %s: %s", url, e)
            continue

    if not results:
        logger.warning("⚠️ No data scraped from any page. Returning empty DataFrame.")
        return _empty_df()

    df = pd.DataFrame(results)
    overall_success_rate = len(df) / total_expected * 100 if total_expected > 0 else 0

    logger.info("🎉 Scraping completed: %d total properties from %d pages", len(df), total_pages)
    logger.info("📊 Overall success rate: %.1f%% (%d/%d)",
                overall_success_rate, len(df), total_expected)

    # Add missing columns to match existing table schema
    from datetime import datetime
    df["Ngày đăng"] = datetime.now().strftime("%Y-%m-%d")

    # Reorder columns to match existing table schema
    expected_columns = ["url", "Diện tích", "Mức giá", "Địa chỉ", "Ngày đăng", "Tiêu đề", "latitude", "longitude"]
    df = df.reindex(columns=expected_columns)

    return df

def _empty_df():
    return pd.DataFrame({
        "url": pd.Series([], dtype="string"),
        "Diện tích": pd.Series([], dtype="string"),
        "Mức giá": pd.Series([], dtype="string"),
        "Địa chỉ": pd.Series([], dtype="string"),
        "Ngày đăng": pd.Series([], dtype="string"),
        "Tiêu đề": pd.Series([], dtype="string"),
        "latitude": pd.Series([], dtype="float"),
        "longitude": pd.Series([], dtype="float"),
    })

# Test function
def test_requests_scraping():
    """Test the requests-based scraping"""
    print("Testing requests-based scraping...")

    test_urls = ["https://nhadat247.com.vn/mua-ban-nha-dat-ci38.html"]
    df = requests_scraping(test_urls, limit_each_page=2)

    print(f"✅ Scraping completed. Got {len(df)} properties")
    if len(df) > 0:
        print("Sample data:")
        print(df.head())
    return len(df) > 0

if __name__ == "__main__":
    test_requests_scraping()