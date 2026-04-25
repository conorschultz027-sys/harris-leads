"""
Harris County Motivated Seller Lead Scraper v11
Fixes vs v10:
  - prop_address no longer has city appended (p[17] only)
  - owners.txt aka column (col 3) also indexed for lookup
  - lookup() tries name-order flip: HCAD=LAST FIRST, clerk=FIRST LAST
  - fuzzy threshold lowered 0.7 → 0.60, prefix length 20 → 25
  - _parse() column detection hardened so LIEN/HOA/MED tables don't
    stuff a person's name into doc_num with owner=""
  - CURRENT OWNER rows skipped in owner index
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

BASE_URL = "https://www.cclerk.hctx.net"
RP_URL   = f"{BASE_URL}/applications/websearch/RP.aspx"
LOOKBACK = int(os.environ.get("LOOKBACK_DAYS", 7))
HEADLESS = os.environ.get("HEADLESS", "true").lower() != "false"

F_FROM   = "ctl00$ContentPlaceHolder1$txtFrom"
F_TO     = "ctl00$ContentPlaceHolder1$txtTo"
F_INST   = "ctl00$ContentPlaceHolder1$txtInstrument"
F_BTN_ID = "ctl00_ContentPlaceHolder1_btnSearch"

OUTPUT_PATHS = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV      = Path("data/ghl_export.csv")
HCAD_ZIP     = Path("data/Real_acct_owner.zip")

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
                   "FUND", "VENTURE", "CAPITAL", "PROPERTIES", "GROUP", "MGMT",
                   "PARTNERSHIP", "PTNSH", "ASSOC", "HOLDING")

# real_acct.txt confirmed column indices (from header inspection):
# 0=acct  2=mailto  3=mail_addr_1  4=mail_addr_2  5=mail_city
# 6=mail_state  7=mail_zip  17=site_addr_1  18=site_addr_2(city)  19=site_addr_3(zip)
RA_ACCT      = 0
RA_MAIL_ADDR = 3
RA_MAIL_CITY = 5
RA_MAIL_ST   = 6
RA_MAIL_ZIP  = 7
RA_SITE1     = 17   # e.g. "907 COMMERCE ST"
RA_SITE_CITY = 18   # e.g. "HOUSTON"
RA_SITE_ZIP  = 19   # e.g. "77002"
RA_MIN_COLS  = 20

# owners.txt confirmed column indices:
# 0=acct  1=ln_num  2=name  3=aka  4=pct_own
OW_ACCT = 0
OW_NAME = 2
OW_AKA  = 3


# ── Name utilities ────────────────────────────────────────────────────────────

def clean(s):
    return re.sub(r"\s+", " ", (s or "").upper().strip())

def name_tokens(name):
    """Sorted frozenset of significant tokens — used for fuzzy matching."""
    stop = {"THE", "OF", "AND", "A", "AN", "AT", "IN", "FOR"}
    tokens = re.findall(r"[A-Z0-9]+", name.upper())
    return frozenset(t for t in tokens if t not in stop and len(t) > 1)

def flip_name(name):
    """
    Try reversing assumed name order.
    HCAD stores LASTNAME FIRSTNAME; clerk sometimes gives FIRSTNAME LASTNAME.
    'EVANS BEAU' → try 'BEAU EVANS' and vice-versa.
    Only flips simple 2-3 token names to avoid mangling entities.
    """
    parts = name.split()
    if len(parts) == 2:
        return f"{parts[1]} {parts[0]}"
    if len(parts) == 3:
        # LAST FIRST MID  →  FIRST MID LAST  and  FIRST LAST MID
        return f"{parts[1]} {parts[2]} {parts[0]}"
    return None

def parse_names(names_raw, fallback_grantee=""):
    """Split 'Grantor : NAME Grantee : NAME' into (grantor, grantee)."""
    if not names_raw:
        return "", fallback_grantee
    grantor_m  = re.search(r"[Gg]rantor\s*:\s*(.+?)(?=\s*[Gg]rantee\s*:|$)", names_raw, re.DOTALL)
    grantee_ms = re.findall(r"[Gg]rantee\s*:\s*(.+?)(?=\s*[Gg]rantee\s*:|$)", names_raw, re.DOTALL)
    grantor = clean(grantor_m.group(1)) if grantor_m else clean(names_raw)
    grantee = " / ".join(clean(g) for g in grantee_ms if g.strip())
    if not grantor_m and not grantee_ms:
        grantee = fallback_grantee
    return grantor, grantee

def split_name_for_ghl(full_name):
    nm = (full_name or "").strip()
    if not nm:
        return "", ""
    if any(kw in nm.upper() for kw in ENTITY_KEYWORDS):
        return "", nm
    if "," in nm:
        parts = nm.split(",", 1)
        return parts[1].strip(), parts[0].strip()
    parts = nm.split()
    return (" ".join(parts[:-1]), parts[-1]) if len(parts) > 1 else ("", parts[0])


# ── Scoring ───────────────────────────────────────────────────────────────────

def compute_flags(r, now):
    flags = []
    cat, dt = r.get("cat", ""), r.get("doc_type", "")
    owner = (r.get("owner") or "").upper()
    if dt == "L/P" or cat == "foreclosure":
        flags.append("Lis pendens")
    if dt == "NOTICE":
        flags.append("Notice of foreclosure")
    if cat == "judgment":
        flags.append("Judgment lien")
    if cat in ("tax", "tax_lien"):
        flags.append("Tax lien")
    if dt == "MED":
        flags.append("Medicaid lien")
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
    base = {"foreclosure": 65, "tax": 55, "tax_lien": 55,
            "judgment": 50, "lien": 45, "probate": 50, "release": 20}.get(r.get("cat", ""), 30)
    s = base
    s += len([f for f in flags if f not in ("LLC / corp owner", "New this week")]) * 8
    if r.get("doc_type") in ("L/P", "NOTICE"):
        s += 15
    try:
        a = float(str(r.get("amount") or 0).replace(",", "").replace("$", ""))
        s += 15 if a > 100000 else 10 if a > 50000 else 5 if a > 10000 else 0
    except Exception:
        pass
    if "New this week" in flags: s += 5
    if r.get("prop_address"):    s += 5
    return min(s, 100)


# ── HCAD Address Lookup ───────────────────────────────────────────────────────

class HCADLookup:
    """
    Reads owners.txt and real_acct.txt from Real_acct_owner.zip.
    Builds: name -> [accts]  (exact, aka, token fuzzy)
            acct -> address dict
    v11 changes:
      - aka column indexed
      - name_to_acct maps to LIST of accts (multiple owners per name)
      - lookup() tries name-flip before fuzzy
      - fuzzy threshold 0.60 (was 0.70), prefix length 25 (was 20)
    """

    def __init__(self):
        self._name_to_accts  = {}   # exact upper name -> [acct, ...]
        self._token_index    = []   # list of (frozenset_tokens, acct) tuples
        self._acct_to_addr   = {}   # acct -> address dict
        self._loaded         = False

    # ── Internal loaders ──────────────────────────────────────────────────────

    def _load_addresses(self, zf):
        """
        Parse real_acct.txt.
        Confirmed columns (from header row inspection):
          17=site_addr_1 ("907 COMMERCE ST")
          18=site_addr_2 / city ("HOUSTON")
          19=site_addr_3 / zip  ("77002")
          3=mail_addr_1  5=mail_city  6=mail_state  7=mail_zip
        prop_address = site_addr_1 only (NOT appending city).
        """
        log.info("Loading real_acct.txt addresses...")
        count = 0
        with zf.open("real_acct.txt") as fh:
            fh.readline()  # skip header
            for raw in fh:
                try:
                    p = raw.decode("latin-1", "ignore").rstrip("\r\n").split("\t")
                    if len(p) < RA_MIN_COLS:
                        continue
                    acct = p[RA_ACCT].strip()
                    if not acct:
                        continue
                    site_addr = p[RA_SITE1].strip()   # e.g. "907 COMMERCE ST"
                    site_city = p[RA_SITE_CITY].strip()
                    site_zip  = p[RA_SITE_ZIP].strip()
                    self._acct_to_addr[acct] = {
                        "prop_address": site_addr,
                        "prop_city":    site_city,
                        "prop_state":   "TX",
                        "prop_zip":     site_zip,
                        "mail_address": p[RA_MAIL_ADDR].strip(),
                        "mail_city":    p[RA_MAIL_CITY].strip(),
                        "mail_state":   p[RA_MAIL_ST].strip() or "TX",
                        "mail_zip":     p[RA_MAIL_ZIP].strip(),
                    }
                    count += 1
                except Exception:
                    continue
        log.info(f"  {count:,} address records loaded")

    def _index_name(self, name, acct):
        """Add a name→acct mapping to both the exact and token indexes."""
        if not name or not acct:
            return
        # Exact index: name → list of accts
        self._name_to_accts.setdefault(name, [])
        if acct not in self._name_to_accts[name]:
            self._name_to_accts[name].append(acct)
        # Token index as list of tuples (allows duplicates per token set)
        toks = name_tokens(name)
        if len(toks) >= 2:
            self._token_index.append((toks, acct))

    def _load_owners(self, zf):
        """
        Parse owners.txt: acct(0) ln_num(1) name(2) aka(3) pct_own(4).
        Indexes both name and aka.
        Only indexes accts that have address data.
        """
        log.info("Loading owners.txt name index...")
        count = 0
        with zf.open("owners.txt") as fh:
            fh.readline()  # skip header
            for raw in fh:
                try:
                    p    = raw.decode("latin-1", "ignore").rstrip("\r\n").split("\t")
                    if len(p) < 3:
                        continue
                    acct = p[OW_ACCT].strip()
                    name = clean(p[OW_NAME])
                    aka  = clean(p[OW_AKA]) if len(p) > OW_AKA else ""
                    if not acct or not name:
                        continue
                    if name in ("CURRENT OWNER", ""):
                        continue
                    if acct not in self._acct_to_addr:
                        continue
                    self._index_name(name, acct)
                    if aka and aka not in ("", " "):
                        self._index_name(aka, acct)
                    count += 1
                except Exception:
                    continue
        log.info(f"  {count:,} owner name records indexed "
                 f"({len(self._name_to_accts):,} unique names, "
                 f"{len(self._token_index):,} token entries)")

    # ── Public API ────────────────────────────────────────────────────────────

    def _ensure_zip(self):
        HCAD_ZIP.parent.mkdir(parents=True, exist_ok=True)
        if HCAD_ZIP.exists() and HCAD_ZIP.stat().st_size > 50_000_000:
            log.info(f"HCAD zip already present ({HCAD_ZIP.stat().st_size // 1_000_000} MB)")
            return True
        log.info("Downloading HCAD data from Google Drive...")
        try:
            import gdown
            FILE_ID = "1edpPMYI5rzx6nCGH5x8tGdo3JuyluNR8"
            gdown.download(id=FILE_ID, output=str(HCAD_ZIP), quiet=False)
            size = HCAD_ZIP.stat().st_size if HCAD_ZIP.exists() else 0
            if size < 50_000_000:
                log.error(f"Downloaded file too small ({size} bytes)")
                return False
            log.info(f"Saved {HCAD_ZIP} ({size // 1_000_000} MB)")
            return True
        except Exception as e:
            log.error(f"HCAD download failed: {e}")
            return False

    def load(self):
        if not self._ensure_zip():
            return False
        try:
            with zipfile.ZipFile(HCAD_ZIP) as zf:
                self._load_addresses(zf)
                self._load_owners(zf)
            self._loaded = True
            log.info(f"HCAD ready: {len(self._name_to_accts):,} unique names, "
                     f"{len(self._acct_to_addr):,} addresses")
            return True
        except Exception as e:
            log.error(f"HCAD load error: {e}")
            return False

    def _acct_to_result(self, acct):
        """Return address dict for acct, or {} if no usable address."""
        addr = self._acct_to_addr.get(acct, {})
        return addr if addr.get("prop_address") or addr.get("mail_address") else {}

    def _lookup_exact(self, key):
        """Return first good address from exact name match."""
        accts = self._name_to_accts.get(key, [])
        for acct in accts:
            result = self._acct_to_result(acct)
            if result:
                return result
        return None

    def _lookup_prefix(self, key, prefix_len=25):
        """Match on first prefix_len characters — handles HCAD truncation."""
        pfx = key[:prefix_len]
        for n, accts in self._name_to_accts.items():
            if n.startswith(pfx) or key.startswith(n[:prefix_len]):
                for acct in accts:
                    result = self._acct_to_result(acct)
                    if result:
                        return result
        return None

    def _lookup_fuzzy(self, key, threshold=0.60):
        """Token Jaccard similarity across all indexed names."""
        toks = name_tokens(key)
        if len(toks) < 2:
            return None
        best_score, best_acct = 0.0, None
        for idx_toks, acct in self._token_index:
            shared = len(toks & idx_toks)
            if shared >= 2:
                score = shared / max(len(toks), len(idx_toks))
                if score > best_score:
                    best_score, best_acct = score, acct
        if best_score >= threshold and best_acct:
            return self._acct_to_result(best_acct)
        return None

    def lookup(self, name):
        if not self._loaded or not name:
            return {}
        key = clean(name)

        # 1. Exact match
        result = self._lookup_exact(key)
        if result:
            return result

        # 2. Name-order flip (HCAD=LAST FIRST, clerk sometimes=FIRST LAST)
        flipped = flip_name(key)
        if flipped:
            result = self._lookup_exact(flipped)
            if result:
                return result

        # 3. Prefix match (handles HCAD ~25-char truncation)
        if len(key) > 15:
            result = self._lookup_prefix(key, prefix_len=25)
            if result:
                return result

        # 4. Prefix match on flipped name
        if flipped and len(flipped) > 15:
            result = self._lookup_prefix(flipped, prefix_len=25)
            if result:
                return result

        # 5. Token fuzzy match (threshold 0.60)
        result = self._lookup_fuzzy(key, threshold=0.60)
        if result:
            return result

        # 6. Fuzzy on flipped name
        if flipped:
            result = self._lookup_fuzzy(flipped, threshold=0.60)
            if result:
                return result

        return {}


# ── Scraper ───────────────────────────────────────────────────────────────────

class ClerkScraper:
    def __init__(self, df, dt):
        self.df = df; self.dt = dt; self.records = []

    async def run(self):
        if not PLAYWRIGHT_AVAILABLE:
            log.error("Playwright not installed"); return []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-blink-features=AutomationControlled"])
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US", timezone_id="America/Chicago")
            await ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            page = await ctx.new_page()
            await page.goto(f"{BASE_URL}/applications/websearch/Home.aspx",
                            timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)
            for code, (cat, label) in DOC_TYPES.items():
                for attempt in range(3):
                    try:
                        await self._search(page, code, cat, label); break
                    except Exception as e:
                        log.warning(f"[{code}] attempt {attempt+1}: {e}")
                        if attempt < 2: await asyncio.sleep(3)
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
        clicked = await page.evaluate(f"""() => {{
            const btn = document.getElementById('{F_BTN_ID}');
            if (btn) {{ btn.click(); return true; }}
            return false;
        }}""")
        log.info(f"  [{code}] button clicked: {clicked}")
        await page.wait_for_load_state("networkidle", timeout=30000)
        await page.wait_for_timeout(2000)
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        recs = self._parse(soup, code, cat, label)
        self.records.extend(recs)
        log.info(f"  [{code}] page 1: {len(recs)} records")
        pg = 1
        while pg < 50:
            try:
                nxt = page.locator("a:has-text('Next'), input[value='Next >']").first
                if await nxt.count() == 0: break
                await nxt.click()
                await page.wait_for_load_state("networkidle", timeout=20000)
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")
                recs = self._parse(soup, code, cat, label)
                self.records.extend(recs)
                pg += 1
                log.info(f"  [{code}] page {pg}: {len(recs)} records")
                if not recs: break
            except Exception as e:
                log.warning(f"  [{code}] pagination stopped at page {pg}: {e}"); break

    def _parse(self, soup, code, cat, label):
        recs = []
        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            if len(rows) < 2:
                continue
            hdrs = [th.get_text(" ", strip=True).lower()
                    for th in rows[0].find_all(["th", "td"])]
            joined = " ".join(hdrs)
            # Use v10 detection — broad match to avoid missing tables
            if not any(k in joined for k in
                       ("file number", "file date", "names", "grantor",
                        "instrument", "grantee")):
                continue
            if len(hdrs) < 3:
                continue

            col = {}
            for i, h in enumerate(hdrs):
                if "file number" in h or "file no" in h:
                    col.setdefault("doc_num", i)
                elif "file date" in h or "date" in h:
                    col.setdefault("filed", i)
                elif "names" in h or "grantor" in h:
                    col.setdefault("names", i)
                elif "grantee" in h:
                    col.setdefault("grantee", i)
                elif "legal" in h or "description" in h:
                    col.setdefault("legal", i)
                elif "amount" in h or "consid" in h:
                    col.setdefault("amount", i)

            if "doc_num" not in col:
                continue

            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells:
                    continue
                try:
                    def t(k):
                        i = col.get(k)
                        return cells[i].get_text(" ", strip=True) if (
                            i is not None and i < len(cells)) else ""

                    doc_num = t("doc_num")
                    if not doc_num or len(doc_num) < 2:
                        continue

                    url = ""
                    for cell in cells:
                        a = cell.find("a", href=True)
                        if a:
                            href = a["href"]
                            url = href if href.startswith("http") else BASE_URL + "/" + href.lstrip("/")
                            break
                    if not url:
                        url = f"{RP_URL}?FileNum={doc_num}"

                    amt = None
                    try:
                        raw = re.sub(r"[^\d.]", "", t("amount"))
                        if raw:
                            amt = float(raw)
                    except Exception:
                        pass

                    fd = ""
                    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                        try:
                            fd = datetime.strptime(t("filed").strip(), fmt).strftime("%Y-%m-%d")
                            break
                        except Exception:
                            pass

                    grantor, grantee = parse_names(t("names"), t("grantee"))

                    recs.append({
                        "doc_num":   doc_num,
                        "doc_type":  code,
                        "filed":     fd,
                        "cat":       cat,
                        "cat_label": label,
                        "owner":     grantor,
                        "grantee":   grantee,
                        "amount":    amt,
                        "legal":     t("legal"),
                        "prop_address": "", "prop_city": "", "prop_state": "", "prop_zip": "",
                        "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
                        "clerk_url": url,
                        "flags": [], "score": 0,
                    })
                except Exception as e:
                    log.debug(f"Row parse error: {e}")
        return recs


# ── GHL Export ────────────────────────────────────────────────────────────────

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
                    "First Name":    first,
                    "Last Name":     last,
                    "Mailing Address": r.get("mail_address", ""),
                    "Mailing City":    r.get("mail_city", ""),
                    "Mailing State":   r.get("mail_state", ""),
                    "Mailing Zip":     r.get("mail_zip", ""),
                    "Property Address": r.get("prop_address", ""),
                    "Property City":    r.get("prop_city", ""),
                    "Property State":   r.get("prop_state", ""),
                    "Property Zip":     r.get("prop_zip", ""),
                    "Lead Type":        r.get("cat_label", ""),
                    "Document Type":    r.get("doc_type", ""),
                    "Date Filed":       r.get("filed", ""),
                    "Document Number":  r.get("doc_num", ""),
                    "Amount/Debt Owed": r.get("amount", ""),
                    "Seller Score":     r.get("score", 0),
                    "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                    "Source":           "Harris County Clerk",
                    "Public Records URL": r.get("clerk_url", ""),
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
    log.info(f"Harris County Scraper v11 | {df.date()} → {dt.date()}")

    records = await ClerkScraper(df, dt).run()

    hcad   = HCADLookup()
    loaded = hcad.load()
    if not loaded:
        log.warning("HCAD lookup unavailable — addresses will be empty")

    for r in records:
        try:
            if loaded:
                addr = hcad.lookup(r.get("owner", ""))
                if addr:
                    r.update(addr)
            flags      = compute_flags(r, now)
            r["flags"] = flags
            r["score"] = compute_score(r, flags)
        except Exception as e:
            log.debug(f"Enrich error: {e}")

    records.sort(key=lambda r: r.get("score", 0), reverse=True)
    seen, unique = set(), []
    for r in records:
        k = r.get("doc_num") or (r.get("owner", "") + r.get("filed", ""))
        if k and k not in seen:
            seen.add(k)
            unique.append(r)
    records = unique

    write_output(records, df, dt)
    write_ghl(records, GHL_CSV)

    with_addr = sum(1 for r in records if r.get("prop_address"))
    log.info(f"Done: {len(records)} records | {with_addr} with address")
    log.info(f"Hot  (≥70): {sum(1 for r in records if r.get('score', 0) >= 70)}")
    log.info(f"Warm (50-69): {sum(1 for r in records if 50 <= r.get('score', 0) < 70)}")

if __name__ == "__main__":
    asyncio.run(main())
