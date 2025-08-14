import os
import re
import time
import json
import random
import logging
import subprocess
from contextlib import suppress
import pandas as pd
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
from bs4 import BeautifulSoup

# ===== Selenium (Edge) imports =====
from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
# ===================================

# ----------------------- Config -----------------------
REMOTE_DEBUG_PORT = 9222                  # must be free
NAV_MODE = "auto"                         # "auto" or "manual"
RESULTS_SAVE_EVERY = 10
CF_MANUAL_GATE_TIMEOUT = 240              # seconds to let a human clear a challenge
MAX_GATES_BEFORE_MANUAL = 3               # after this many gates, fall back to manual
DETAIL_MIN_SIGNALS = 2                    # how many content signals to accept as "detail page"

# ----------------------- Utils -----------------------
def to_city_state_slug(addr: str) -> str | None:
    """Extract and format city-state from address for URL."""
    if not isinstance(addr, str):
        return None
    m = re.search(r",\s*([^,]+),\s*([A-Z]{2})(?:,\s*\d{5})?$", addr.strip(), flags=re.I)
    if not m:
        return None
    city = re.sub(r"\s+", "-", m.group(1).strip())
    st = m.group(2).upper()
    return f"{city}-{st}"

def build_fps_url(first: str, last: str, address: str | None, zip_code: str | None) -> str:
    """Build FastPeopleSearch URL with fallback from ZIP to city-state."""
    fn = re.sub(r"[^A-Za-z\- ]", "", (first or "")).strip().replace(" ", "-")
    ln = re.sub(r"[^A-Za-z\- ]", "", (last or "")).strip().replace(" ", "-")
    where = None
    if isinstance(zip_code, str) and zip_code.strip():
        where = zip_code.strip()[:5]  # keep 5 digits
    if not where:
        where = to_city_state_slug(address or "")  # fallback
    return f"https://www.fastpeoplesearch.com/name/{quote_plus(fn)}-{quote_plus(ln)}_{quote_plus(where or '')}"

def save_atomic(df: pd.DataFrame, path: str):
    """Save DataFrame to CSV with atomic replacement."""
    tmp = f"{path}.tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)  # atomic on same filesystem

def setup_logging():
    """Set up logging with both file and console handlers."""
    log_dir = os.path.join(Path(__file__).parent.parent.parent, "logs")
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"fps_scraper_edge_{timestamp}.log")

    file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_formatter = logging.Formatter("%(levelname)s: %(message)s")

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger

logger = setup_logging()

# Paths
CURR_SCRIPT_PATH = os.path.realpath(os.path.dirname(__file__))
BASE_PATH = Path(CURR_SCRIPT_PATH).parent.parent
input_csv_path = os.path.join(BASE_PATH, "data", "processed", "test.csv")
output_path = os.path.join(BASE_PATH, "data", "scraped_data")
os.makedirs(output_path, exist_ok=True)

def human_sleep(min_sec=0.8, max_sec=2.0):
    """Add small random delay to pace requests."""
    delay = random.uniform(min_sec, max_sec)
    logger.debug(f"Sleeping for {delay:.2f} seconds")
    time.sleep(delay)

# ----------------------- Cloudflare / Gate helpers -----------------------
_CF_SIGNAL_STRINGS = [
    "checking your browser",
    "please wait while we check your browser",
    "verifying you are human",
    "just a moment",
    "cloudflare",
    "cf-challenge",
    "turnstile",
    "captcha",
]
def is_cloudflare_challenge(html: str) -> bool:
    h = (html or "").lower()
    return any(s in h for s in _CF_SIGNAL_STRINGS)

def looks_blocked(html: str) -> bool:
    """Detect common block/challenge pages."""
    html = (html or "").lower()
    triggers = [
        "access denied", "unusual traffic", "are you a human", "verify you are human",
        "security check", "just a moment", "please enable cookies",
    ]
    return any(t in html for t in triggers) or is_cloudflare_challenge(html)

def _click_cookie_banner_if_any(driver):
    """Try a few common consent patterns; ignore failures."""
    candidates = [
        "//button[contains(., 'Accept') and contains(., 'Cookies')]",
        "//button[contains(., 'I Agree')]",
        "//button[contains(., 'Accept All')]",
        "//*[@id='onetrust-accept-btn-handler']",
    ]
    for xp in candidates:
        try:
            btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.XPATH, xp)))
            btn.click()
            time.sleep(0.4)
            return
        except Exception:
            continue

def wait_for_normal_content(driver, timeout=45):
    """Wait for plausible signs that FPS content is visible (results or header)."""
    wait = WebDriverWait(driver, timeout)
    try:
        wait.until(EC.any_of(
            EC.presence_of_element_located((By.XPATH, "//*[@id='results' or contains(@class,'results')]")),
            EC.presence_of_element_located((By.XPATH, "//a[contains(@href,'/person') or contains(@href,'/people') or contains(@href,'/details')]")),
            EC.presence_of_element_located((By.XPATH, "//header//*[contains(text(),'FastPeopleSearch') or contains(@class,'navbar')]")),
        ))
        return True
    except TimeoutException:
        return False

def open_with_human_gate(driver, url: str, logger=None, first_timeout=20, gate_timeout=CF_MANUAL_GATE_TIMEOUT):
    """
    Navigate to URL. If a Cloudflare/human check shows, pause and let a human
    complete it in the Edge window, then continue. Returns True if a gate was observed.
    """
    driver.get(url)

    # Quick pass: did we already see normal content?
    if wait_for_normal_content(driver, timeout=first_timeout):
        return False

    gate_seen = False
    html = driver.page_source
    if is_cloudflare_challenge(html):
        gate_seen = True
        if logger: logger.info("Cloudflare challenge detected. Please complete it in the Edge window.")
        print("\n--- Action required ---")
        print("A Cloudflare / human check is visible in the Edge window.")
        print("Please complete it (e.g., click the button or solve the challenge).")
        print("This script will wait up to", gate_timeout, "seconds for the page to become available.\n")

        end = time.time() + gate_timeout
        while time.time() < end:
            time.sleep(3)
            if wait_for_normal_content(driver, timeout=5):
                if logger: logger.info("Challenge cleared. Continuing.")
                return gate_seen
        raise TimeoutException("Cloudflare check not cleared in time.")
    else:
        if not wait_for_normal_content(driver, timeout=first_timeout):
            raise TimeoutException("Page did not load expected content.")
    return gate_seen

# ----------------------- Edge remote-debug (attach/launch) -----------------------
def _find_msedge_exe() -> str:
    candidates = [
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\Microsoft\Edge SxS\Application\msedge.exe"),
    ]
    for p in candidates:
        if os.path.isfile(p):
            return p
    raise FileNotFoundError("msedge.exe not found in standard locations.")

def _start_edge_with_debug(user_data_dir: str, profile_dir: str, port: int = REMOTE_DEBUG_PORT):
    edge_path = _find_msedge_exe()
    cmd = [
        edge_path,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data_dir}",
        f"--profile-directory={profile_dir}",
        "--new-window",
    ]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def setup_edge_driver_remote_debug(
    profile_dir: str = "Default",
    user_data_dir: str | None = None,
    port: int = REMOTE_DEBUG_PORT,
    msedgedriver_path: str | None = None,
    page_load_timeout: int = 60,
):
    """
    Start (or attach to) Edge running with your real profile via remote debugging.
    Compliant: do not mask automation flags.
    """
    if not user_data_dir:
        user_data_dir = os.path.expandvars(r"%LOCALAPPDATA%\Microsoft\Edge\User Data")

    options = EdgeOptions()
    options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")
    options.page_load_strategy = "eager"

    service = EdgeService(executable_path=msedgedriver_path) if msedgedriver_path else EdgeService()

    def _connect():
        drv = webdriver.Edge(service=service, options=options)
        drv.set_page_load_timeout(page_load_timeout)
        return drv

    with suppress(WebDriverException):
        return _connect(), None

    proc = _start_edge_with_debug(user_data_dir, profile_dir, port)
    time.sleep(2.0)
    driver = _connect()
    return driver, proc

# ---------- Optional: verify VPN IP/country (manual sanity check) ----------
def get_ip_info_via_browser(driver) -> dict:
    """
    Fetch IP info in the controlled browser so you can confirm the VPN exit.
    """
    try:
        driver.get("https://ipinfo.io/json")
        pre = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "pre"))
        )
        return json.loads(pre.text)
    except Exception:
        return {}

# ----------------------- Detail-page detection -----------------------
def is_detail_page_html(html: str) -> bool:
    """Heuristic: detail page if we see several person-specific blocks."""
    if not html:
        return False
    soup = BeautifulSoup(html, "html.parser")
    signals = 0
    # Age header
    if soup.find(id="age-header"):
        signals += 1
    # Relatives / Associates blocks
    if soup.select("#relative-links a[title*='Details for'], #associate-links a[title*='Details for']"):
        signals += 1
    # Phone numbers (links having phone number hover titles)
    if soup.select("a[title*='phone number']"):
        signals += 1
    # Address link
    if soup.select("a[title*='Search people living at']"):
        signals += 1
    # Background report / FAQs sections
    if soup.find(id="background-report") or soup.find(id="faqs"):
        signals += 1
    return signals >= DETAIL_MIN_SIGNALS

# ----------------------- Parsing -----------------------
def _parse_detail_page_html_to_data(html: str, current_url: str):
    """Parse a *detail* page's HTML into our structured dict."""
    data = {
        "home_address": None,
        "phones": [],
        "age": None,
        "relatives": [],
        "emails": [],
        "marital_status": None,
        "associates": [],
        "previous_addresses": [],
        "current_address_details": None,
        "background_report": None,
        "faqs": None,
        "page_url": current_url,
    }
    if not html:
        return data

    soup = BeautifulSoup(html, "html.parser")

    # Address
    try:
        address_element = soup.find("a", title=lambda x: x and "Search people living at" in x)
        data["home_address"] = address_element.text.strip() if address_element else None
    except Exception:
        pass

    # Phones
    try:
        phone_elements = soup.find_all("a", title=lambda x: x and "Search people associated with the phone number" in x)
        data["phones"] = [phone.text.strip() for phone in phone_elements[:5]]
    except Exception:
        pass

    # Age
    try:
        age_element = soup.find("h2", id="age-header")
        if age_element:
            # e.g., "Age: 54"
            m = re.search(r"(\d{1,3})", age_element.get_text(" ", strip=True))
            data["age"] = m.group(1) if m else None
    except Exception:
        pass

    # Relatives
    try:
        relative_box = soup.find("div", id="relative-links")
        if relative_box:
            rel_links = relative_box.find_all("a", title=lambda x: x and "Details for" in x)
            data["relatives"] = [rl.text.strip() for rl in rel_links]
    except Exception:
        pass

    # Emails
    try:
        email_elements = soup.find_all("a", class_="__cf_email__")
        # If CF deobfuscation runs on the page, .text will be the plain email.
        data["emails"] = [e.get_text(strip=True) for e in email_elements if e.get_text(strip=True)]
    except Exception:
        pass

    # Marital status
    try:
        marital_section = soup.find("div", id="marital_status_section")
        if marital_section:
            mp = marital_section.find("p")
            data["marital_status"] = mp.get_text(" ", strip=True) if mp else None
    except Exception:
        pass

    # Associates
    try:
        assoc_box = soup.find("div", id="associate-links")
        if assoc_box:
            links = assoc_box.find_all("a", title=lambda x: x and "Details for" in x)
            data["associates"] = [a.text.strip() for a in links]
    except Exception:
        pass

    # Previous addresses
    try:
        prev_div = soup.find("div", id="previous-addresses")
        if prev_div:
            prev_links = prev_div.find_all("a", title=lambda x: x and "Search people who live at" in x)
            data["previous_addresses"] = [a.text.strip() for a in prev_links]
    except Exception:
        pass

    # Current address details
    try:
        cur_addr = soup.find("div", id="current-address-details")
        data["current_address_details"] = cur_addr.get_text(" ", strip=True) if cur_addr else None
    except Exception:
        pass

    # Background report
    try:
        bg = soup.find("div", id="background-report")
        data["background_report"] = bg.get_text(" ", strip=True) if bg else None
    except Exception:
        pass

    # FAQs
    try:
        faqs = soup.find("div", id="faqs")
        data["faqs"] = faqs.get_text("\n", strip=True) if faqs else None
    except Exception:
        pass

    return data

def scrape_current_page(driver):
    """
    Scrape whatever is currently loaded in the controlled tab.
    """
    html = driver.page_source
    if is_cloudflare_challenge(html):
        return None, "Cloudflare challenge still visible"
    if looks_blocked(html):
        return None, "Blocked / access denied page"
    if not is_detail_page_html(html):
        cur = driver.current_url or ""
        return None, f"Not on a recognizable detail page (URL: {cur})"
    # Try to dismiss cookie banners etc. (safe no-op if none)
    _click_cookie_banner_if_any(driver)
    html = driver.page_source
    data = _parse_detail_page_html_to_data(html, driver.current_url or "")
    return data, None

# ----------------------- Results-page helpers -----------------------
def _candidate_detail_links(driver):
    """
    Return candidate <a> elements that look like person detail links.
    """
    xpaths = [
        "//a[contains(@href,'/person') or contains(@href,'/people') or contains(@href,'/details')]",
        # Sometimes result cards have a CTA like "View Free Details"
        "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'view') and contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'detail')]",
    ]
    elems = []
    for xp in xpaths:
        try:
            found = driver.find_elements(By.XPATH, xp)
            elems.extend(found)
        except Exception:
            pass
    # de-duplicate by id or href
    seen = set()
    uniq = []
    for e in elems:
        href = (e.get_attribute("href") or "").strip()
        key = href or id(e)
        if key not in seen:
            uniq.append(e)
            seen.add(key)
    return uniq

def _normalize_name(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _score_anchor_for_name(a, first, last) -> int:
    """Simple score: +2 if both tokens in text, +1 if either in href/text."""
    txt = _normalize_name(a.text)
    href = _normalize_name(a.get_attribute("href") or "")
    score = 0
    if first in txt and last in txt:
        score += 2
    elif (first in txt) or (last in txt):
        score += 1
    if first in href and last in href:
        score += 1
    return score

def _get_best_result_href(driver, first: str, last: str) -> str | None:
    candidates = _candidate_detail_links(driver)
    if not candidates:
        return None
    nf, nl = _normalize_name(first), _normalize_name(last)
    # Score and pick best
    scored = sorted(
        [(a, _score_anchor_for_name(a, nf, nl)) for a in candidates],
        key=lambda x: x[1],
        reverse=True
    )
    best = scored[0][0]
    href = (best.get_attribute("href") or "").strip()
    if not href:
        return None
    if href.startswith("/"):
        href = "https://www.fastpeoplesearch.com" + href
    return href

# ----------------------- Automated per-record flow -----------------------
def do_record_auto(driver, first_name, last_name, address, zip_code, idx, total, gate_counter):
    """
    Automated navigation (preferred). Falls back to manual if gates are too frequent.
    """
    url = build_fps_url(first_name, last_name, address, zip_code)
    logger.info(f"[{idx+1}/{total}] Searching: {first_name} {last_name} @ {zip_code or address}")

    gate_seen = open_with_human_gate(driver, url, logger=logger, first_timeout=20, gate_timeout=CF_MANUAL_GATE_TIMEOUT)
    gate_counter += int(bool(gate_seen))
    if wait_for_normal_content(driver, timeout=20) is False:
        logger.warning("Results view did not appear; trying manual fallback.")
        return None, gate_counter, "need_manual"

    # Pick best result and open it
    try:
        WebDriverWait(driver, 15).until(EC.any_of(
            EC.presence_of_element_located((By.XPATH, "//*[@id='results' or contains(@class,'results')]")),
            EC.presence_of_element_located((By.XPATH, "//a[contains(@href,'/person') or contains(@href,'/people') or contains(@href,'/details')]")),
        ))
    except TimeoutException:
        logger.warning("No obvious results DOM; trying manual fallback.")
        return None, gate_counter, "need_manual"

    best_href = _get_best_result_href(driver, first_name, last_name)
    if not best_href:
        logger.info("No suitable detail link found on results; manual fallback suggested.")
        return None, gate_counter, "need_manual"

    # Open detail and scrape
    gate_seen2 = open_with_human_gate(driver, best_href, logger=logger, first_timeout=15, gate_timeout=CF_MANUAL_GATE_TIMEOUT)
    gate_counter += int(bool(gate_seen2))
    data, err = scrape_current_page(driver)
    if data:
        return data, gate_counter, None

    logger.info(f"Automated scrape couldn't confirm detail page ({err}); manual fallback suggested.")
    return None, gate_counter, "need_manual"

# ----------------------- Manual per-record flow (fallback) -----------------------
def do_record_manual(driver, first_name, last_name, address, zip_code, idx, total):
    """
    Manual fallback:
    - Prints the search URL
    - You navigate in the same Edge tab, clear any Cloudflare, click into the detail page
    - Press ENTER; we scrape the current tab
    """
    url = build_fps_url(first_name, last_name, address, zip_code)
    logger.info(f"[{idx+1}/{total}] Manual step for: {first_name} {last_name}")
    print("\n" + "="*72)
    print(f"RECORD {idx+1}/{total} → {first_name} {last_name}")
    print("1) In the Edge window this script opened, go to:")
    print(url)
    print("2) Solve any Cloudflare checks and click into the correct *detail page*.")
    print("3) When the detail page is fully visible, press ENTER here to scrape it.")
    print("(Type 'skip' and press ENTER to skip this record.)")
    print("="*72 + "\n")

    user_input = input().strip().lower()
    if user_input == "skip":
        return None

    start = time.time()
    while True:
        data, err = scrape_current_page(driver)
        if data:
            return data
        remaining = int(CF_MANUAL_GATE_TIMEOUT - (time.time() - start))
        if remaining <= 0:
            logger.warning("Timed out waiting for manual clearance; skipping record.")
            return None
        print(f"Not ready: {err}. Fix in Edge, then press ENTER (or type 'skip'). Time left ~{remaining}s")
        user_input = input().strip().lower()
        if user_input == "skip":
            return None

# ----------------------- Name filters -----------------------
def looks_like_composite_name(s: str) -> bool:
    """Filter out trust/org/composite names that won't map to a person card."""
    s = (s or "").lower()
    bad_chunks = [" & ", " and ", " revocable trust", " trust of ", " trustees", " survivors", " llc", " inc"]
    return any(b in s for b in bad_chunks)

# ----------------------- Main -----------------------
def main():
    logger.info(f"Starting FPS scraper (Edge, remote-debug, NAV_MODE={NAV_MODE})")
    os.makedirs(output_path, exist_ok=True)

    driver = None
    edge_proc = None
    try:
        # Read CSV
        logger.info(f"Reading input file: {input_csv_path}")
        output_df = pd.read_csv(input_csv_path)

        # Ensure text-friendly dtypes for columns we populate
        text_cols = [
            "Phone1", "Phone2", "Phone3", "Phone4", "Phone5",
            "Age", "Relatives", "Emails", "Marital Status", "Associates",
            "Previous Addresses", "Current Address Details",
            "Background Report Summary", "FAQs", "Page URL"
        ]
        for col in text_cols:
            if col not in output_df.columns:
                output_df[col] = pd.Series(dtype="string")
            else:
                try:
                    output_df[col] = output_df[col].astype("string")
                except Exception:
                    output_df[col] = output_df[col].astype("object")

        # Resolve msedgedriver path if provided locally/env
        msedge_driver = None
        env_driver = os.environ.get("MSEDGEDRIVER")
        script_dir_driver = os.path.join(CURR_SCRIPT_PATH, "msedgedriver.exe")
        for p in [env_driver, script_dir_driver]:
            if p and os.path.isfile(p):
                msedge_driver = p
                break

        # Init browser via remote debugging (attach or launch)
        try:
            driver, edge_proc = setup_edge_driver_remote_debug(
                profile_dir="Default",  # change if your profile is "Profile 1", etc.
                user_data_dir=os.path.expandvars(r"%LOCALAPPDATA%\Microsoft\Edge\User Data"),
                port=REMOTE_DEBUG_PORT,
                msedgedriver_path=msedge_driver,  # None -> Selenium Manager picks the right driver
                page_load_timeout=60,
            )
            # Optional: quick check that VPN exit is US
            ipinfo = get_ip_info_via_browser(driver)
            if ipinfo:
                logger.info(f"IP check -> {ipinfo.get('ip')} {ipinfo.get('country')} {ipinfo.get('region')}")
            print("\nOpened Edge with your profile. The script will drive searches automatically.\n")
        except Exception as e:
            logger.error(f"Failed to initialize browser session: {str(e)}")
            return

        total_records = len(output_df)
        logger.info(f"Processing {total_records} records")
        gate_counter = 0

        for idx, row in output_df.iterrows():
            try:
                first = re.sub(r"[^a-zA-Z\s-]", "", str(row.get("First Name", ""))).strip()
                last  = re.sub(r"[^a-zA-Z\s-]", "", str(row.get("Last Name", ""))).strip()
                if not first or not last:
                    logger.warning(f"Skipping record {idx + 1} - Invalid name format")
                    continue
                if looks_like_composite_name(first) or looks_like_composite_name(last):
                    logger.warning(f"Skipping record {idx + 1} - Composite/organization-like name")
                    continue

                zip_code = None
                if pd.notna(row.get("ZIP")):
                    try:
                        zip_code = str(int(row["ZIP"]))
                    except Exception:
                        zip_code = str(row.get("ZIP")).strip() if pd.notna(row.get("ZIP")) else None

                address = row.get("address")

                data = None
                fallback_needed = False

                if NAV_MODE == "auto":
                    data, gate_counter, flag = do_record_auto(
                        driver=driver,
                        first_name=first,
                        last_name=last,
                        address=address,
                        zip_code=zip_code,
                        idx=idx,
                        total=total_records,
                        gate_counter=gate_counter
                    )
                    if flag == "need_manual" or gate_counter >= MAX_GATES_BEFORE_MANUAL:
                        fallback_needed = True

                if NAV_MODE == "manual" or fallback_needed:
                    data = do_record_manual(
                        driver=driver,
                        first_name=first,
                        last_name=last,
                        address=address,
                        zip_code=zip_code,
                        idx=idx,
                        total=total_records
                    )

                if data:
                    # Fill row
                    if data["phones"]:
                        for i, phone in enumerate(data["phones"][:5]):
                            output_df.at[idx, f"Phone{i+1}"] = phone
                    output_df.at[idx, "Age"] = data["age"]
                    output_df.at[idx, "Relatives"] = ", ".join(data["relatives"]) if data["relatives"] else None
                    output_df.at[idx, "Emails"] = ", ".join(data["emails"]) if data["emails"] else None
                    output_df.at[idx, "Marital Status"] = data["marital_status"]
                    output_df.at[idx, "Associates"] = ", ".join(data["associates"]) if data["associates"] else None
                    output_df.at[idx, "Previous Addresses"] = ", ".join(data["previous_addresses"]) if data["previous_addresses"] else None
                    output_df.at[idx, "Current Address Details"] = data["current_address_details"]
                    output_df.at[idx, "Background Report Summary"] = data["background_report"]
                    output_df.at[idx, "FAQs"] = data["faqs"]
                    output_df.at[idx, "Page URL"] = data["page_url"]

                # Pace a bit between records
                human_sleep(0.5, 1.6)

                # Save progress periodically
                if (idx + 1) % RESULTS_SAVE_EVERY == 0:
                    logger.info("Saving progress to input file...")
                    save_atomic(output_df, input_csv_path)

            except Exception as e:
                logger.error(f"Error processing record {idx + 1}: {str(e)}")
                continue

        # Final save
        save_atomic(output_df, input_csv_path)

        # Backups
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_csv = os.path.join(output_path, f"scraped_results_{timestamp}.csv")
        output_xlsx = os.path.join(output_path, f"scraped_results_{timestamp}.xlsx")
        logger.info(f"Saving backup copy to: {output_csv}")
        output_df.to_csv(output_csv, index=False)
        output_df.to_excel(output_xlsx, index=False)

    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}")
        try:
            if "output_df" in locals():
                error_csv = os.path.join(output_path, f"error_backup_{int(time.time())}.csv")
                output_df.to_csv(error_csv, index=False)
                logger.info(f"Saved error backup to: {error_csv}")
        except Exception:
            pass
        raise
    finally:
        try:
            if driver:
                logger.info("Cleaning up browser session...")
                driver.quit()
        except Exception as e:
            logger.error(f"Error during browser cleanup: {str(e)}")

        if edge_proc and edge_proc.poll() is None:
            with suppress(Exception):
                edge_proc.terminate()

        logger.info("Scraping process completed")

if __name__ == "__main__":
    main()
