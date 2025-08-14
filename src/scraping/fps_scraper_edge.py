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
from selenium.common.exceptions import TimeoutException, WebDriverException, ElementClickInterceptedException
# ===================================

# ----------------------- Config -----------------------
REMOTE_DEBUG_PORT = 9222
NAV_MODE = "auto"                         # "auto" or "manual"
RESULTS_SAVE_EVERY = 10
CF_MANUAL_GATE_TIMEOUT = 240
MAX_GATES_BEFORE_MANUAL = 3
DEBUG_DUMPS = bool(int(os.environ.get("DEBUG_DUMPS", "1")))  # dump html/screenshots by default during debugging
SCROLL_PAUSE = 0.8

# ----------------------- Paths & Logging -----------------------
CURR_SCRIPT_PATH = os.path.realpath(os.path.dirname(__file__))
BASE_PATH = Path(CURR_SCRIPT_PATH).parent.parent
input_csv_path = os.path.join(BASE_PATH, "data", "processed", "test.csv")
output_path = os.path.join(BASE_PATH, "data", "scraped_data")
debug_path = os.path.join(output_path, "debug")
os.makedirs(output_path, exist_ok=True)
os.makedirs(debug_path, exist_ok=True)

def setup_logging():
    log_dir = os.path.join(Path(__file__).parent.parent.parent, "logs")
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"fps_scraper_edge_{timestamp}.log")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(fh)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logger.addHandler(ch)
    return logger

logger = setup_logging()

# ----------------------- Utils -----------------------
def human_sleep(a=0.8, b=2.0):
    d = random.uniform(a, b)
    logger.debug(f"Sleeping {d:.2f}s")
    time.sleep(d)

def to_city_state_slug(addr: str) -> str | None:
    if not isinstance(addr, str):
        return None
    m = re.search(r",\s*([^,]+),\s*([A-Z]{2})(?:,\s*\d{5})?$", addr.strip(), flags=re.I)
    if not m:
        return None
    city = re.sub(r"\s+", "-", m.group(1).strip())
    st = m.group(2).upper()
    return f"{city}-{st}"

def build_fps_url(first: str, last: str, address: str | None, zip_code: str | None) -> str:
    fn = re.sub(r"[^A-Za-z\- ]", "", (first or "")).strip().replace(" ", "-")
    ln = re.sub(r"[^A-Za-z\- ]", "", (last or "")).strip().replace(" ", "-")
    where = None
    if isinstance(zip_code, str) and zip_code.strip():
        where = zip_code.strip()[:5]
    if not where:
        where = to_city_state_slug(address or "")
    return f"https://www.fastpeoplesearch.com/name/{quote_plus(fn)}-{quote_plus(ln)}_{quote_plus(where or '')}"

def save_atomic(df: pd.DataFrame, path: str):
    tmp = f"{path}.tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)

def safe_name_stub(first, last, idx):
    return f"{idx:05d}_{re.sub(r'[^a-z0-9]+','-',(first or '').lower()).strip('-')}_{re.sub(r'[^a-z0-9]+','-',(last or '').lower()).strip('-')}"

# ----------------------- Cloudflare / Gate helpers -----------------------
_CF_SIGNAL_STRINGS = [
    "checking your browser", "please wait while we check your browser",
    "verifying you are human", "just a moment",
    "cloudflare", "cf-challenge", "turnstile", "captcha",
]
def is_cloudflare_challenge(html: str) -> bool:
    h = (html or "").lower()
    return any(s in h for s in _CF_SIGNAL_STRINGS)

def looks_blocked(html: str) -> bool:
    html = (html or "").lower()
    triggers = [
        "access denied", "unusual traffic", "are you a human",
        "verify you are human", "security check", "please enable cookies",
    ]
    return any(t in html for t in triggers) or is_cloudflare_challenge(html)

def _click_cookie_banner_if_any(driver):
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
    driver.get(url)
    if wait_for_normal_content(driver, timeout=first_timeout):
        return False
    gate_seen = False
    html = driver.page_source
    if is_cloudflare_challenge(html):
        gate_seen = True
        if logger: logger.info("Cloudflare challenge detected. Please clear it in the Edge window.")
        end = time.time() + gate_timeout
        while time.time() < end:
            time.sleep(3)
            if wait_for_normal_content(driver, timeout=5):
                if logger: logger.info("Challenge cleared.")
                return gate_seen
        raise TimeoutException("Cloudflare check not cleared in time.")
    else:
        if not wait_for_normal_content(driver, timeout=first_timeout):
            raise TimeoutException("Page did not load expected content.")
    return gate_seen

# ----------------------- Edge remote-debug -----------------------
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

def get_ip_info_via_browser(driver) -> dict:
    try:
        driver.get("https://ipinfo.io/json")
        pre = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
        return json.loads(pre.text)
    except Exception:
        return {}

# ----------------------- Robust extraction helpers -----------------------
PHONE_RE = re.compile(r'(?:\+1[\s\-\.\u2011\u2012\u2013\u2014]?)?(?:\(?\d{3}\)?[\s\-\.\u2011\u2012\u2013\u2014]?)\d{3}[\s\-\.\u2011\u2012\u2013\u2014]?\d{4}')
EMAIL_RE = re.compile(r'[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}', re.I)

def _normalize_phone(raw: str) -> str:
    digits = re.sub(r'\D', '', raw or '')
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f'({digits[0:3]}) {digits[3:6]}-{digits[6:10]}'
    return (raw or '').strip()

def decode_cf_email(cf_hex: str) -> str | None:
    try:
        key = int(cf_hex[:2], 16)
        return ''.join(chr(int(cf_hex[i:i+2], 16) ^ key) for i in range(2, len(cf_hex), 2))
    except Exception:
        return None

def _dedupe_preserve_order(items):
    seen = set(); out = []
    for it in items:
        it = (it or "").strip()
        if it and it not in seen:
            out.append(it); seen.add(it)
    return out

# ----------------------- Detail-page detection -----------------------
def is_detail_page_html(html: str) -> bool:
    if not html: return False
    if is_cloudflare_challenge(html) or looks_blocked(html): return False
    soup = BeautifulSoup(html, "html.parser")
    body_text = soup.get_text(" ", strip=True).lower()
    signals = 0
    if soup.select("a[href^='tel:'], a[href*='/phone/']") or PHONE_RE.search(body_text): signals += 1
    if soup.select("a[href*='/address/']") or 'current address' in body_text or 'address history' in body_text: signals += 1
    if re.search(r'age\s*:?\s*\d{1,3}', body_text): signals += 1
    if any(k in body_text for k in ('possible relatives','relatives','associated people','associates')): signals += 1
    return signals >= 1

# ----------------------- Parsing (robust) -----------------------
def _parse_detail_page_html_to_data(html: str, current_url: str):
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
    body_text = soup.get_text(" ", strip=True)

    # Address
    try:
        addr_el = soup.select_one("a[href*='/address/']")
        if addr_el:
            data["home_address"] = addr_el.get_text(" ", strip=True) or None
        if not data["home_address"]:
            m = re.search(r'current address\s*:?\s*(.+?)(?:\s{2,}|$)', body_text, flags=re.I)
            if m: data["home_address"] = m.group(1).strip()
    except Exception:
        pass

    # Phones
    phones = []
    try:
        for a in soup.select("a[href^='tel:'], a[href*='/phone/']"):
            t = (a.get_text(" ", strip=True) or a.get("href") or "")
            phones.extend(PHONE_RE.findall(t))
        if not phones:
            phones.extend(PHONE_RE.findall(body_text))
        phones = [_normalize_phone(p) for p in phones]
        data["phones"] = _dedupe_preserve_order(phones)[:5]
    except Exception:
        pass

    # Age
    try:
        m = re.search(r'\bage\s*:?\s*(\d{1,3})\b', body_text, flags=re.I)
        if m: data["age"] = m.group(1)
    except Exception:
        pass

    # Emails
    emails = []
    try:
        for cf in soup.select("a.__cf_email__"):
            cf_hex = cf.get("data-cfemail")
            dec = decode_cf_email(cf_hex) if cf_hex else None
            if dec: emails.append(dec)
        for a in soup.select("a[href^='mailto:']"):
            href = a.get("href", "")
            if href.lower().startswith("mailto:"): emails.append(href.split(":", 1)[1])
        if not emails:
            emails.extend(EMAIL_RE.findall(body_text))
        data["emails"] = _dedupe_preserve_order(emails)
    except Exception:
        pass

    # Relatives / Associates
    def _extract_names_by_heading(keywords):
        out = []
        for h in soup.find_all(re.compile(r'^h[1-6]$')):
            title = h.get_text(" ", strip=True).lower()
            if any(k in title for k in keywords):
                container = h.find_next(lambda t: t and t.name in ("div","section","ul","ol"))
                if container:
                    for a in container.select("a[href*='/name/'], a[href*='/person/'], a[href*='/people/']"):
                        nm = a.get_text(" ", strip=True)
                        if nm and "detail" not in nm.lower():
                            out.append(nm)
        return _dedupe_preserve_order(out)

    try:
        data["relatives"] = _extract_names_by_heading({"possible relatives","relatives"})
        data["associates"] = _extract_names_by_heading({"associated people","associates"})
    except Exception:
        pass

    # Previous addresses
    try:
        addr_links = [a.get_text(" ", strip=True) for a in soup.select("a[href*='/address/']")]
        addr_links = _dedupe_preserve_order(addr_links)
        if addr_links:
            data["previous_addresses"] = addr_links[1:]
    except Exception:
        pass

    # Background / FAQs (best effort)
    try:
        for label in ("background report","background check"):
            m = re.search(rf'({label}.*?)\s{{5,}}', body_text, flags=re.I | re.S)
            if m:
                data["background_report"] = m.group(1).strip(); break
    except Exception:
        pass
    try:
        faq_match = re.search(r'(faq[s]?:?.+)$', body_text, flags=re.I | re.S)
        if faq_match: data["faqs"] = faq_match.group(1).strip()
    except Exception:
        pass

    return data

# ----------------------- Results-page helpers -----------------------
def _candidate_detail_links(driver):
    xpaths = [
        "//a[contains(@href,'/person') or contains(@href,'/people') or contains(@href,'/details')]",
        "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'view') and contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'detail')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'view') and contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'detail')]",
    ]
    elems = []
    for xp in xpaths:
        try:
            found = driver.find_elements(By.XPATH, xp)
            elems.extend(found)
        except Exception:
            pass
    # de-duplicate by href/text
    seen = set(); uniq = []
    for e in elems:
        href = (e.get_attribute("href") or e.get_attribute("data-href") or "").strip()
        key = href or (e.text.strip() + "_" + str(id(e)))
        if key not in seen:
            uniq.append(e); seen.add(key)
    return uniq

def _normalize_name(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _score_anchor_for_name(a, first, last) -> int:
    txt = _normalize_name(a.text)
    href = _normalize_name(a.get_attribute("href") or a.get_attribute("data-href") or "")
    score = 0
    if first in txt and last in txt: score += 2
    elif (first in txt) or (last in txt): score += 1
    if first in href and last in href: score += 1
    return score

def _scroll_to_bottom(driver, steps=4):
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(steps):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height: break
        last_height = new_height

def _safe_click(driver, elem):
    try:
        elem.click()
        return True
    except (ElementClickInterceptedException, WebDriverException):
        try:
            driver.execute_script("arguments[0].click();", elem)
            return True
        except Exception:
            return False

def _open_best_result(driver, first: str, last: str, idx_stub: str):
    # Let results render and lazy-load
    _scroll_to_bottom(driver, steps=3)
    human_sleep(0.6, 1.2)

    cands = _candidate_detail_links(driver)
    if not cands:
        return None

    nf, nl = _normalize_name(first), _normalize_name(last)
    scored = sorted(
        [(a, _score_anchor_for_name(a, nf, nl)) for a in cands],
        key=lambda x: x[1],
        reverse=True
    )
    best = scored[0][0]
    href = (best.get_attribute("href") or best.get_attribute("data-href") or "").strip()
    if href:
        if href.startswith("/"):
            href = "https://www.fastpeoplesearch.com" + href
        driver.get(href)
        return href

    # If no href (button that navigates), try clicking
    if _safe_click(driver, best):
        return driver.current_url
    return None

def _expand_detail_sections(driver):
    # Click common expanders so hidden sections appear
    xpaths = [
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view') and contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'detail')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'show more')]",
        "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'show more')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'expand')]",
        "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view full')]",
    ]
    clicked = 0
    for xp in xpaths:
        for elem in driver.find_elements(By.XPATH, xp):
            try:
                if _safe_click(driver, elem):
                    clicked += 1
                    time.sleep(0.4)
            except Exception:
                continue
    if clicked:
        human_sleep(0.4, 0.9)

# ----------------------- Scrape current page -----------------------
def scrape_current_page(driver):
    try:
        WebDriverWait(driver, 30).until(lambda d: is_detail_page_html(d.page_source))
    except TimeoutException:
        cur = driver.current_url or ""
        return None, f"Detail content not detected after waiting (URL: {cur})"

    _click_cookie_banner_if_any(driver)
    _expand_detail_sections(driver)
    _scroll_to_bottom(driver, steps=2)
    time.sleep(0.5)

    html = driver.page_source
    if is_cloudflare_challenge(html):
        return None, "Cloudflare challenge still visible"
    if looks_blocked(html):
        return None, "Blocked / access denied page"

    data = _parse_detail_page_html_to_data(html, driver.current_url or "")
    logger.debug("Parsed detail: phones=%s emails=%s addr=%s age=%s",
                 data.get("phones", [])[:2], data.get("emails", [])[:2],
                 data.get("home_address"), data.get("age"))
    return data, None

# ----------------------- Automated per-record flow -----------------------
def do_record_auto(driver, first_name, last_name, address, zip_code, idx, total, gate_counter, idx_stub):
    url = build_fps_url(first_name, last_name, address, zip_code)
    logger.info(f"[{idx+1}/{total}] Searching: {first_name} {last_name} @ {zip_code or address}")
    gate_seen = open_with_human_gate(driver, url, logger=logger, first_timeout=20, gate_timeout=CF_MANUAL_GATE_TIMEOUT)
    gate_counter += int(bool(gate_seen))

    # Debug dump: results
    if DEBUG_DUMPS:
        try:
            with open(os.path.join(debug_path, f"{idx_stub}_results.html"), "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            driver.save_screenshot(os.path.join(debug_path, f"{idx_stub}_results.png"))
        except Exception:
            pass

    try:
        WebDriverWait(driver, 15).until(EC.any_of(
            EC.presence_of_element_located((By.XPATH, "//*[@id='results' or contains(@class,'results')]")),
            EC.presence_of_element_located((By.XPATH, "//a[contains(@href,'/person') or contains(@href,'/people') or contains(@href,'/details')]")),
            EC.presence_of_element_located((By.XPATH, "//button[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view') and contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'detail')]")),
        ))
    except TimeoutException:
        logger.warning("No obvious results DOM; trying manual fallback.")
        return None, gate_counter, "need_manual"

    # Open best result
    opened = _open_best_result(driver, first_name, last_name, idx_stub)
    if not opened:
        logger.info("No suitable detail link found on results; manual fallback suggested.")
        return None, gate_counter, "need_manual"

    human_sleep(0.7, 1.4)

    # Debug dump: detail
    if DEBUG_DUMPS:
        try:
            with open(os.path.join(debug_path, f"{idx_stub}_detail.html"), "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            driver.save_screenshot(os.path.join(debug_path, f"{idx_stub}_detail.png"))
        except Exception:
            pass

    # Scrape
    data, err = scrape_current_page(driver)
    if data:
        return data, gate_counter, None
    logger.info(f"Automated scrape couldn't confirm detail page ({err}); manual fallback suggested.")
    return None, gate_counter, "need_manual"

# ----------------------- Manual per-record flow -----------------------
def do_record_manual(driver, first_name, last_name, address, zip_code, idx, total, idx_stub):
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
        # Debug dump before scrape attempt
        if DEBUG_DUMPS:
            try:
                with open(os.path.join(debug_path, f"{idx_stub}_manual_detail.html"), "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                driver.save_screenshot(os.path.join(debug_path, f"{idx_stub}_manual_detail.png"))
            except Exception:
                pass

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
    s = (s or "").lower()
    bad = [" & ", " and ", " revocable trust", " trust of ", " trustees", " survivors", " llc", " inc"]
    return any(b in s for b in bad)

# ----------------------- Main -----------------------
def main():
    logger.info(f"Starting FPS scraper (Edge, remote-debug, NAV_MODE={NAV_MODE})")
    os.makedirs(output_path, exist_ok=True)

    driver = None
    edge_proc = None
    try:
        # Read CSV (your sample headers are honored)
        logger.info(f"Reading input file: {input_csv_path}")
        output_df = pd.read_csv(input_csv_path)

        # Ensure text-friendly dtypes for columns we populate
        text_cols = [
            "Full Address",
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

        # Resolve driver path if provided
        msedge_driver = None
        env_driver = os.environ.get("MSEDGEDRIVER")
        script_dir_driver = os.path.join(CURR_SCRIPT_PATH, "msedgedriver.exe")
        for p in [env_driver, script_dir_driver]:
            if p and os.path.isfile(p):
                msedge_driver = p; break

        # Init browser via remote debugging (attach or launch)
        try:
            driver, edge_proc = setup_edge_driver_remote_debug(
                profile_dir="Default",
                user_data_dir=os.path.expandvars(r"%LOCALAPPDATA%\Microsoft\Edge\User Data"),
                port=REMOTE_DEBUG_PORT,
                msedgedriver_path=msedge_driver,
                page_load_timeout=60,
            )
            ipinfo = get_ip_info_via_browser(driver)
            if ipinfo:
                logger.info(f"IP check -> {ipinfo.get('ip')} {ipinfo.get('country')} {ipinfo.get('region')}")
            print("\nOpened Edge with your profile. The script will drive searches automatically.\n")
        except Exception as e:
            logger.error(f"Failed to initialize browser session: {str(e)}")
            return

        total_records = len(output_df)

        # Optional: limit rows for debugging
        limit = os.environ.get("LIMIT_ROWS")
        iter_df = output_df.head(int(limit)) if (limit and limit.isdigit()) else output_df
        total_iter = len(iter_df)

        logger.info(f"Processing {total_iter} records (of total {total_records})")
        gate_counter = 0

        for idx, row in iter_df.iterrows():
            try:
                first = re.sub(r"[^a-zA-Z\s-]", "", str(row.get("First Name", ""))).strip()
                last  = re.sub(r"[^a-zA-Z\s-]", "", str(row.get("Last Name", ""))).strip()
                if not first or not last:
                    logger.warning(f"Skipping record {idx + 1} - Invalid name")
                    continue
                if looks_like_composite_name(first) or looks_like_composite_name(last):
                    logger.warning(f"Skipping record {idx + 1} - Composite/org-like name")
                    continue

                zip_code = None
                if pd.notna(row.get("ZIP")):
                    try:
                        zip_code = str(int(row["ZIP"]))
                    except Exception:
                        zip_code = str(row.get("ZIP")).strip() if pd.notna(row.get("ZIP")) else None

                # Be robust to address column variants
                address = None
                for col in ("address", "Address", "Home Address", "Mailing Address", "Property Address", "Full Address"):
                    if col in output_df.columns and pd.notna(row.get(col)):
                        address = str(row.get(col)); break

                data = None
                fallback_needed = False
                idx_stub = safe_name_stub(first, last, idx)

                if NAV_MODE == "auto":
                    data, gate_counter, flag = do_record_auto(
                        driver=driver,
                        first_name=first,
                        last_name=last,
                        address=address,
                        zip_code=zip_code,
                        idx=idx,
                        total=total_iter,
                        gate_counter=gate_counter,
                        idx_stub=idx_stub
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
                        total=total_iter,
                        idx_stub=idx_stub
                    )

                if data:
                    # Phones
                    if data["phones"]:
                        for i, phone in enumerate(data["phones"][:5]):
                            output_df.at[idx, f"Phone{i+1}"] = phone

                    # Age
                    output_df.at[idx, "Age"] = data["age"]

                    # Relatives / Emails / Associates / Previous Addresses
                    output_df.at[idx, "Relatives"] = ", ".join(data["relatives"]) if data["relatives"] else None
                    output_df.at[idx, "Emails"] = ", ".join(data["emails"]) if data["emails"] else None
                    output_df.at[idx, "Associates"] = ", ".join(data["associates"]) if data["associates"] else None
                    output_df.at[idx, "Previous Addresses"] = ", ".join(data["previous_addresses"]) if data["previous_addresses"] else None

                    # Address → write to BOTH "Full Address" (if present) and "Current Address Details"
                    if "Full Address" in output_df.columns:
                        output_df.at[idx, "Full Address"] = data.get("home_address") or data.get("current_address_details")
                    output_df.at[idx, "Current Address Details"] = data.get("home_address") or data.get("current_address_details")

                    # Background / FAQs / URL
                    output_df.at[idx, "Background Report Summary"] = data["background_report"]
                    output_df.at[idx, "FAQs"] = data["faqs"]
                    output_df.at[idx, "Page URL"] = data["page_url"]

                # Pace
                human_sleep(0.5, 1.6)

                # Periodic save
                if (idx + 1) % RESULTS_SAVE_EVERY == 0:
                    logger.info("Saving progress to input file...")
                    save_atomic(output_df, input_csv_path)

            except Exception as e:
                logger.error(f"Error processing record {idx + 1}: {str(e)}")

        # Final save
        save_atomic(output_df, input_csv_path)

        # Backups
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_csv = os.path.join(output_path, f"scraped_results_{ts}.csv")
        out_xlsx = os.path.join(output_path, f"scraped_results_{ts}.xlsx")
        logger.info(f"Saving backups: {out_csv}")
        output_df.to_csv(out_csv, index=False)
        output_df.to_excel(out_xlsx, index=False)

    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        try:
            if "output_df" in locals():
                error_csv = os.path.join(output_path, f"error_backup_{int(time.time())}.csv")
                output_df.to_csv(error_csv, index=False)
                logger.info(f"Saved error backup: {error_csv}")
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
