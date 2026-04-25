"""
Harris County Motivated Seller Lead Scraper v9
Fixes:
  - Clean Grantor/Grantee name splitting (no more blobs)
  - Robust HCAD address lookup with auto-detected DBF field names
  - Fixed dead-code flag bugs (NOTICE, MED)
  - Fixed doc_type flag checks (L/P not LP)
  - Recalibrated scoring so foreclosure/LP leads score Hot
  - LLC/CORP/TRUST name handling in GHL export
  - Pagination errors logged instead of silently swallowed
"""
import asyncio, csv, json, logging, os, re, io, zipfile
from datetime import datetime, timedelta
from pathlib import Path
import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

try:
    from dbfread import DBF
    DBFREAD_AVAILABLE = True
except ImportError:
    DBFREAD_AVAILABLE = False

BASE_URL = "https://www.cclerk.hctx.net"
RP_URL   = f"{BASE_URL}/applications/websearch/RP.aspx"
LOOKBACK = int(os.environ.get("LOOKBACK_DAYS", 7))
HEADLESS = os.environ.get("HEADLESS", "true").lower() != "false"

# Confirmed field names from JS inspection
F_FROM   = "ctl00$ContentPlaceHolder1$txtFrom"
F_TO     = "ctl00$ContentPlaceHolder1$txtTo"
F_INST   = "ctl00$ContentPlaceHolder1$txtInstrument"
F_BTN_ID = "ctl00_ContentPlaceHolder1_btnSearch"

OUTPUT_PATHS = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV      = Path("data/ghl_export.csv")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DOC_TYPES = {
    "L/P":    ("foreclosure", "Lis Pendens"),
    "NOTICE": ("foreclosure", "Notice of Foreclosure"),
    "TRSALE": ("tax",         "Tax/Sheriff Deed"),
    "JUDGE":  ("judgment",    "Abstract of Judgment"),
    "A/J":    ("judgment",    "Abstract of Judgment"),
    "DEED":   ("tax",         "Tax/Sheriff Deed"),
    "T/L":    ("tax_lien",    "Federal Tax Lien"),
    "LIEN":   ("lien",        "Lien"),
    "M/L":    ("lien",        "Mechanic Lien"),
    "HOA":    ("lien",        "HOA Lien"),
    "MED":    ("lien",        "Medicaid Lien"),
    "REL":    ("release",     "Release"),
    "PROB":   ("probate",     "Probate Document"),
}

ENTITY_KEYWORDS = ("LLC", "INC", "CORP", "LTD", "LP ", "L.P.", "L.L.C", "TRUST",
                   "FUND", "VENTURE", "CAPITAL", "PROPERTIES", "GROUP", "MGMT")


# ── Name parsing ──────────────────────────────────────────────────────────────

def parse_names(names_raw, fallback_grantee=""):
    """
    Harris County Names column format:
      'Grantor : OWNER NAME Grantee : OTHER NAME'
    or multiple grantees:
      'Grantor : NAME Grantee : NAME1 Grantee : NAME2'

    Returns (grantor, grantee) as clean strings.
    """
    if not names_raw:
        return "", fallback_grantee

    # Split on Grantor/Grantee markers (case-insensitive)
    grantor_match = re.search(r"[Gg]rantor\s*:\s*(.+?)(?=\s*[Gg]rantee\s*:|$)", names_raw, re.DOTALL)
    grantee_matches = re.findall(r"[Gg]rantee\s*:\s*(.+?)(?=\s*[Gg]rantee\s*:|$)", names_raw, re.DOTALL)

    grantor = grantor_match.group(1).strip() if grantor_match else names_raw.strip()
    grantee = " / ".join(g.strip() for g in grantee_matches if g.strip())

    # If no markers found at all, treat whole string as grantor
    if not grantor_match and not grantee_matches:
        grantor = names_raw.strip()
        grantee = fallback_grantee

    # Clean up extra whitespace
    grantor = re.sub(r"\s+", " ", grantor).strip()
    grantee = re.sub(r"\s+", " ", grantee).strip()

    return grantor, grantee


def split_name_for_ghl(full_name):
    """
    Split owner name into First / Last for GHL export.
    Handles: individuals, LLCs, trusts, corps.
    Returns (first, last).
    """
    if not full_name:
        return "", ""

    nm = full_name.strip()

    # Entity — put everything in Last, blank First
    if any(kw in nm.upper() for kw in ENTITY_KEYWORDS):
        return "", nm

    # "Last, First" format
    if "," in nm:
        parts = nm.split(",", 1)
        return parts[1].strip(), parts[0].strip()

    # "First Last" format
    parts = nm.split()
    if len(parts) == 1:
        return "", parts[0]
    return " ".join(parts[:-1]), parts[-1]


# ── Scoring & flags ───────────────────────────────────────────────────────────

def compute_flags(r, now):
    flags = []
    cat    = r.get("cat", "")
    dt     = r.get("doc_type", "")
    owner  = (r.get("owner") or "").upper()

    if dt == "L/P" or cat == "foreclosure":
        flags.append("Lis pendens")
    if dt == "NOTICE":
        flags.append("Notice of foreclosure")   # FIX: was dead tuple expression
    if cat == "judgment":
        flags.append("Judgment lien")
    if cat in ("tax", "tax_lien"):
        flags.append("Tax lien")
    if dt == "MED":
        flags.append("Medicaid lien")           # FIX: was dead tuple expression
    if cat == "probate":
        flags.append("Probate / estate")
    if any(k in owner for k in ENTITY_KEYWORDS):
        flags.append("LLC / corp owner")

    try:
        filed = r.get("filed")
        if filed and (now - datetime.strptime(filed, "%Y-%m-%d")).days <= 7:
            flags.append("New this week")
    except Exception:
        pass

    return flags


def compute_score(r, flags):
    cat = r.get("cat", "")
    dt  = r.get("doc_type", "")

    # Base score by category — foreclosure/LP starts high
    base = {
        "foreclosure": 65,
        "tax":         55,
        "tax_lien":    55,
        "judgment":    50,
        "lien":        45,
        "probate":     50,
        "release":     20,
    }.get(cat, 30)

    s = base

    # Flag bonuses
    s += len([f for f in flags if f not in ("LLC / corp owner", "New this week")]) * 8

    # Specific type bonus
    if dt in ("L/P", "NOTICE") and cat == "foreclosure":
        s += 15
    if dt == "PROB":
        s += 10

    # Amount bonus
    try:
        a = float(str(r.get("amount") or 0).replace(",", "").replace("$", ""))
        if a > 100000:
            s += 15
        elif a > 50000:
            s += 10
        elif a > 10000:
            s += 5
    except Exception:
        pass

    if "New this week" in flags:
        s += 5
    if r.get("prop_address"):
        s += 5

    return min(s, 100)


# ── Scraper ───────────────────────────────────────────────────────────────────

class ClerkScraper:
    def __init__(self, df, dt):
        self.df = df
        self.dt = dt
        self.records = []

    async def run(self):
        if not PLAYWRIGHT_AVAILABLE:
            log.error("Playwright not installed")
            return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-blink-features=AutomationControlled"]
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="America/Chicago",
            )
            await ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )
            page = await ctx.new_page()

            # Warm up
            await page.goto(f"{BASE_URL}/applications/websearch/Home.aspx",
                            timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)

            for code, (cat, label) in DOC_TYPES.items():
                for attempt in range(3):
                    try:
                        await self._search(page, code, cat, label)
                        break
                    except Exception as e:
                        log.warning(f"[{code}] attempt {attempt+1}: {e}")
                        if attempt < 2:
                            await asyncio.sleep(3)

            await browser.close()

        log.info(f"Total scraped: {len(self.records)}")
        return self.records

    async def _search(self, page, code, cat, label):
        log.info(f"Searching: {code}")
        await page.goto(RP_URL, timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)

        df_str = self.df.strftime("%m/%d/%Y")
        dt_str = self.dt.strftime("%m/%d/%Y")

        await page.evaluate(f"""() => {{
            document.querySelector("input[name='{F_FROM}']").value = '{df_str}';
            document.querySelector("input[name='{F_TO}']").value   = '{dt_str}';
            document.querySelector("input[name='{F_INST}']").value = '{code}';
        }}""")

        vals = await page.evaluate(f"""() => ({{
            from: document.querySelector("input[name='{F_FROM}']")?.value,
            to:   document.querySelector("input[name='{F_TO}']")?.value,
            inst: document.querySelector("input[name='{F_INST}']")?.value,
        }})""")
        log.info(f"  Fields: {vals}")

        clicked = await page.evaluate(f"""() => {{
            const btn = document.getElementById('{F_BTN_ID}');
            if (btn) {{ btn.click(); return true; }}
            return false;
        }}""")
        log.info(f"  Button clicked: {clicked}")

        await page.wait_for_load_state("networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        html  = await page.content()
        soup  = BeautifulSoup(html, "lxml")
        recs  = self._parse(soup, code, cat, label)
        self.records.extend(recs)
        log.info(f"  [{code}] page 1: {len(recs)} records")

        # Pagination
        pg = 1
        while pg < 50:
            try:
                nxt = page.locator("a:has-text('Next'), input[value='Next >']").first
                if await nxt.count() == 0:
                    break
                await nxt.click()
                await page.wait_for_load_state("networkidle", timeout=20000)
                html  = await page.content()
                soup  = BeautifulSoup(html, "lxml")
                recs  = self._parse(soup, code, cat, label)
                self.records.extend(recs)
                pg   += 1
                log.info(f"  [{code}] page {pg}: {len(recs)} records")
                if not recs:
                    break
            except Exception as e:
                log.warning(f"  [{code}] pagination error on page {pg}: {e}")
                break

    def _parse(self, soup, code, cat, label):
        recs = []
        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            if len(rows) < 2:
                continue

            hdrs = [th.get_text(" ", strip=True).lower()
                    for th in rows[0].find_all(["th", "td"])]
            joined = " ".join(hdrs)

            if not any(k in joined for k in
                       ("file number", "file date", "names", "grantor",
                        "instrument", "grantee")):
                continue
            if len(hdrs) < 3:
                continue

            col = {}
            for i, h in enumerate(hdrs):
                hl = h.lower()
                if "file number" in hl or "file no" in hl:
                    col.setdefault("doc_num", i)
                elif "file date" in hl or "date" in hl:
                    col.setdefault("filed", i)
                elif "names" in hl or "grantor" in hl or "or name" in hl:
                    col.setdefault("names", i)   # unified "Names" column
                elif "grantee" in hl or "ee name" in hl:
                    col.setdefault("grantee", i)
                elif "legal" in hl or "description" in hl:
                    col.setdefault("legal", i)
                elif "amount" in hl or "consid" in hl:
                    col.setdefault("amount", i)
                elif "type" in hl and "vol" in hl:
                    col.setdefault("type_vol", i)

            if "doc_num" not in col:
                continue

            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells:
                    continue
                try:
                    def t(k):
                        i = col.get(k)
                        return (cells[i].get_text(" ", strip=True)
                                if i is not None and i < len(cells) else "")

                    doc_num = t("doc_num")
                    if not doc_num or len(doc_num) < 2:
                        continue

                    # Clerk URL
                    url = ""
                    for cell in cells:
                        a = cell.find("a", href=True)
                        if a:
                            href = a["href"]
                            url  = (href if href.startswith("http")
                                    else BASE_URL + "/" + href.lstrip("/"))
                            break
                    if not url:
                        url = f"{RP_URL}?FileNum={doc_num}"

                    # Amount
                    amt = None
                    try:
                        raw = re.sub(r"[^\d.]", "", t("amount"))
                        if raw:
                            amt = float(raw)
                    except Exception:
                        pass

                    # Filed date
                    fd = ""
                    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                        try:
                            fd = datetime.strptime(t("filed").strip(), fmt).strftime("%Y-%m-%d")
                            break
                        except Exception:
                            pass

                    # ── Name parsing (KEY FIX) ──────────────────────────
                    # Harris County uses ONE "Names" column containing both
                    # "Grantor : NAME" and "Grantee : NAME" in the same cell.
                    names_raw       = t("names")
                    extra_grantee   = t("grantee")   # separate col if exists
                    grantor, grantee = parse_names(names_raw, extra_grantee)

                    recs.append({
                        "doc_num":      doc_num,
                        "doc_type":     code,
                        "filed":        fd,
                        "cat":          cat,
                        "cat_label":    label,
                        "owner":        grantor,   # clean grantor name only
                        "grantee":      grantee,   # clean grantee name(s)
                        "amount":       amt,
                        "legal":        t("legal"),
                        "prop_address": "",
                        "prop_city":    "",
                        "prop_state":   "",
                        "prop_zip":     "",
                        "mail_address": "",
                        "mail_city":    "",
                        "mail_state":   "",
                        "mail_zip":     "",
                        "clerk_url":    url,
                        "flags":        [],
                        "score":        0,
                    })
                except Exception as e:
                    log.debug(f"Row parse error: {e}")

        return recs


# ── HCAD Parcel Lookup ────────────────────────────────────────────────────────

class ParcelLookup:
    """
    Downloads HCAD Real_acct_owner.zip and builds an owner-name index.
    Auto-detects actual DBF field names instead of assuming them.
    """
    URLS = [
        "https://pdata.hcad.org/Pdata/download/Real_acct_owner.zip",
        "https://pdata.hcad.org/data/download/Real_acct_owner.zip",
    ]

    def __init__(self):
        self._idx        = {}
        self._addr_fields = {}   # detected field name mapping

    def _clean(self, n):
        return re.sub(r"\s+", " ", (n or "").upper().strip())

    def _variants(self, full):
        p = full.split()
        v = [full]
        if len(p) >= 2:
            v += [f"{p[-1]} {' '.join(p[:-1])}",
                  f"{p[-1]}, {' '.join(p[:-1])}"]
        return [self._clean(x) for x in v]

    def _detect_fields(self, sample_row):
        """
        Auto-detect field names by pattern-matching against known HCAD field patterns.
        Returns dict mapping logical name → actual DBF field name.
        """
        fields = {k: str(k) for k in sample_row.keys()}
        mapping = {}

        patterns = {
            "owner":        ["OWNER", "OWN1", "OWNER_NAME", "OWNERNAME"],
            "prop_address": ["SITE_ADDR", "SITEADDR", "SITE_AD", "STRADR",
                             "STR_ADDR", "STRT_ADDR", "PROP_ADDR"],
            "prop_city":    ["SITE_CITY", "SITECITY", "SITE_CTY", "PROP_CITY"],
            "prop_zip":     ["SITE_ZIP",  "SITEZIP",  "SITE_ZP",  "PROP_ZIP"],
            "mail_address": ["ADDR_1", "MAILADR1", "MAIL_ADDR", "MAIL_AD",
                             "MAILING_ADDR", "MAIL1"],
            "mail_city":    ["CITY", "MAILCITY", "MAIL_CITY", "MAIL_CTY"],
            "mail_state":   ["STATE", "MAILSTATE", "MAIL_STATE", "MAIL_ST"],
            "mail_zip":     ["ZIP", "MAILZIP", "MAIL_ZIP", "MAIL_ZP"],
        }

        upper_fields = {k.upper(): k for k in fields}

        for logical, candidates in patterns.items():
            for c in candidates:
                if c in upper_fields:
                    mapping[logical] = upper_fields[c]
                    break

        log.info(f"HCAD field mapping: {mapping}")
        return mapping

    def load(self):
        if not DBFREAD_AVAILABLE:
            log.warning("dbfread not installed — address lookup disabled")
            return False

        dbf_path = Path("data/parcel.dbf")
        dbf_path.parent.mkdir(parents=True, exist_ok=True)

        if not dbf_path.exists():
            for url in self.URLS:
                try:
                    log.info(f"Downloading HCAD parcel data from {url}...")
                    r = requests.get(url, timeout=180)
                    r.raise_for_status()
                    raw = r.content
                    if raw[:2] == b"PK":
                        with zipfile.ZipFile(io.BytesIO(raw)) as z:
                            dbf_files = [n for n in z.namelist()
                                         if n.lower().endswith(".dbf")]
                            if dbf_files:
                                dbf_path.write_bytes(z.read(dbf_files[0]))
                                log.info(f"Extracted: {dbf_files[0]}")
                    else:
                        dbf_path.write_bytes(raw)
                    log.info("Parcel DBF saved")
                    break
                except Exception as e:
                    log.warning(f"HCAD download failed ({url}): {e}")

        if not dbf_path.exists():
            log.warning("Parcel DBF not available — addresses will be empty")
            return False

        try:
            count   = 0
            detected = False
            table   = DBF(str(dbf_path), encoding="latin-1",
                          ignore_missing_memofile=True, raw=True)

            for row in table:
                # Decode bytes → str
                decoded = {}
                for k, v in row.items():
                    if isinstance(v, bytes):
                        decoded[k] = v.decode("latin-1", "ignore").strip()
                    else:
                        decoded[k] = str(v).strip() if v is not None else ""

                # Auto-detect field mapping on first row
                if not detected:
                    self._addr_fields = self._detect_fields(decoded)
                    detected = True
                    if "owner" not in self._addr_fields:
                        log.error("Could not detect owner field in HCAD DBF")
                        log.error(f"Available fields: {list(decoded.keys())[:20]}")
                        return False

                owner_field = self._addr_fields["owner"]
                owner = decoded.get(owner_field, "")
                if not owner:
                    continue

                for v in self._variants(owner):
                    self._idx.setdefault(v, decoded)
                count += 1

            log.info(f"HCAD index built: {count:,} records")
            return True

        except Exception as e:
            log.error(f"HCAD DBF load error: {e}")
            return False

    def lookup(self, name):
        if not name or not self._idx or not self._addr_fields:
            return {}

        for variant in self._variants(name):
            row = self._idx.get(variant)
            if row:
                def g(logical):
                    field = self._addr_fields.get(logical, "")
                    return row.get(field, "") if field else ""

                return {
                    "prop_address": g("prop_address"),
                    "prop_city":    g("prop_city"),
                    "prop_state":   "TX",
                    "prop_zip":     g("prop_zip"),
                    "mail_address": g("mail_address"),
                    "mail_city":    g("mail_city"),
                    "mail_state":   g("mail_state") or "TX",
                    "mail_zip":     g("mail_zip"),
                }

        return {}


# ── GHL CSV Export ────────────────────────────────────────────────────────────

GHL_COLS = [
    "First Name", "Last Name",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL",
]


def write_ghl(records, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=GHL_COLS)
        w.writeheader()
        for r in records:
            try:
                first, last = split_name_for_ghl(r.get("owner", ""))
                w.writerow({
                    "First Name":            first,
                    "Last Name":             last,
                    "Mailing Address":       r.get("mail_address", ""),
                    "Mailing City":          r.get("mail_city", ""),
                    "Mailing State":         r.get("mail_state", ""),
                    "Mailing Zip":           r.get("mail_zip", ""),
                    "Property Address":      r.get("prop_address", ""),
                    "Property City":         r.get("prop_city", ""),
                    "Property State":        r.get("prop_state", ""),
                    "Property Zip":          r.get("prop_zip", ""),
                    "Lead Type":             r.get("cat_label", ""),
                    "Document Type":         r.get("doc_type", ""),
                    "Date Filed":            r.get("filed", ""),
                    "Document Number":       r.get("doc_num", ""),
                    "Amount/Debt Owed":      r.get("amount", ""),
                    "Seller Score":          r.get("score", 0),
                    "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                    "Source":                "Harris County Clerk",
                    "Public Records URL":    r.get("clerk_url", ""),
                })
            except Exception as e:
                log.debug(f"GHL row error: {e}")

    log.info(f"GHL CSV: {path} ({len(records)} rows)")


# ── Output ────────────────────────────────────────────────────────────────────

def write_output(records, df, dt):
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Harris County Clerk",
        "date_range":   {"from": df.strftime("%Y-%m-%d"), "to": dt.strftime("%Y-%m-%d")},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":      records,
    }
    for p in OUTPUT_PATHS:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info(f"→ {p}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    now = datetime.utcnow()
    dt  = now
    df  = now - timedelta(days=LOOKBACK)
    log.info(f"Harris County Scraper v9 | {df.date()} → {dt.date()}")

    # Scrape
    records = await ClerkScraper(df, dt).run()

    # HCAD address lookup
    parcel = ParcelLookup()
    loaded = parcel.load()
    if not loaded:
        log.warning("HCAD lookup unavailable — addresses will be empty")

    # Enrich records
    for r in records:
        try:
            if loaded:
                addr_data = parcel.lookup(r.get("owner", ""))
                if addr_data:
                    r.update(addr_data)
            flags     = compute_flags(r, now)
            r["flags"]  = flags
            r["score"]  = compute_score(r, flags)
        except Exception as e:
            log.debug(f"Enrich error: {e}")

    # Sort + deduplicate
    records.sort(key=lambda r: r.get("score", 0), reverse=True)
    seen, unique = set(), []
    for r in records:
        k = r.get("doc_num") or (r.get("owner", "") + r.get("filed", ""))
        if k and k not in seen:
            seen.add(k)
            unique.append(r)
    records = unique

    # Write outputs
    write_output(records, df, dt)
    write_ghl(records, GHL_CSV)

    with_addr = sum(1 for r in records if r.get("prop_address"))
    log.info(f"Done: {len(records)} records | {with_addr} with address")
    log.info(f"Hot (≥70): {sum(1 for r in records if r.get('score',0)>=70)}")
    log.info(f"Warm (50-69): {sum(1 for r in records if 50<=r.get('score',0)<70)}")


if __name__ == "__main__":
    asyncio.run(main())
