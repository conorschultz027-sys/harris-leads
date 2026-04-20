"""
Harris County Motivated Seller Lead Scraper
Scrapes Harris County Clerk portal for distressed property records,
enriches with parcel/owner data, scores leads, and outputs JSON + CSV.
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import time
import traceback
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("Playwright not available – clerk scraping disabled")

try:
    from dbfread import DBF
    DBFREAD_AVAILABLE = True
except ImportError:
    DBFREAD_AVAILABLE = False
    logging.warning("dbfread not available – parcel enrichment disabled")

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
CLERK_URL       = "https://www.cclerk.hctx.net/PublicRecords.aspx"
LOOKBACK_DAYS   = int(os.environ.get("LOOKBACK_DAYS", 7))
HEADLESS        = os.environ.get("HEADLESS", "true").lower() != "false"
MAX_RETRIES     = 3
RETRY_DELAY     = 5  # seconds

OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]

GHL_CSV_PATH = Path("data/ghl_export.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Doc-type catalogue
# ─────────────────────────────────────────────
DOC_TYPES = {
    # Codes → (category, label)
    "LP":       ("foreclosure",  "Lis Pendens"),
    "NOFC":     ("foreclosure",  "Notice of Foreclosure"),
    "TAXDEED":  ("tax",          "Tax Deed"),
    "JUD":      ("judgment",     "Judgment"),
    "CCJ":      ("judgment",     "Certified Judgment"),
    "DRJUD":    ("judgment",     "Domestic Judgment"),
    "LNCORPTX": ("tax_lien",     "Corp Tax Lien"),
    "LNIRS":    ("tax_lien",     "IRS Lien"),
    "LNFED":    ("tax_lien",     "Federal Lien"),
    "LN":       ("lien",         "Lien"),
    "LNMECH":   ("lien",         "Mechanic Lien"),
    "LNHOA":    ("lien",         "HOA Lien"),
    "MEDLN":    ("lien",         "Medicaid Lien"),
    "PRO":      ("probate",      "Probate Document"),
    "NOC":      ("construction", "Notice of Commencement"),
    "RELLP":    ("release",      "Release Lis Pendens"),
}

# Short codes the clerk portal uses internally (used in form POST)
CLERK_DOC_CODES = list(DOC_TYPES.keys())

# ─────────────────────────────────────────────
# Seller-score logic
# ─────────────────────────────────────────────
def compute_flags(record: dict, now: datetime) -> list[str]:
    flags = []
    cat   = record.get("cat", "")
    dtype = record.get("doc_type", "")
    owner = (record.get("owner") or "").upper()

    if dtype in ("LP", "RELLP") or cat == "foreclosure":
        flags.append("Lis pendens")
    if dtype == "NOFC":
        flags.append("Pre-foreclosure")
    if cat == "judgment":
        flags.append("Judgment lien")
    if cat in ("tax", "tax_lien"):
        flags.append("Tax lien")
    if dtype == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "probate":
        flags.append("Probate / estate")
    if any(kw in owner for kw in ("LLC", "INC", "CORP", "LTD", "LP ", "L.P.", "L.L.C")):
        flags.append("LLC / corp owner")

    filed = record.get("filed")
    if filed:
        try:
            filed_dt = datetime.strptime(filed, "%Y-%m-%d")
            if (now - filed_dt).days <= 7:
                flags.append("New this week")
        except ValueError:
            pass

    return flags


def compute_score(record: dict, flags: list[str], now: datetime) -> int:
    score = 30  # base

    score += len(flags) * 10

    # LP + foreclosure combo
    cat   = record.get("cat", "")
    dtype = record.get("doc_type", "")
    if dtype in ("LP", "NOFC") and cat == "foreclosure":
        score += 20

    # Amount bonuses
    amount = record.get("amount") or 0
    try:
        amount = float(str(amount).replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        amount = 0
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10

    # New this week
    if "New this week" in flags:
        score += 5

    # Has address
    if record.get("prop_address"):
        score += 5

    return min(score, 100)


# ─────────────────────────────────────────────
# Parcel / owner enrichment
# ─────────────────────────────────────────────
class ParcelLookup:
    """Loads Harris County bulk parcel DBF and builds owner-name index."""

    PARCEL_URLS = [
        # HCAD bulk data – try several known endpoints
        "https://pdata.hcad.org/Pdata/download/Real_acct_owner.zip",
        "https://pdata.hcad.org/Pdata/download/real_acct_owner.zip",
        "https://hcad.org/assets/uploads/data/Real_acct_owner.zip",
    ]

    # Column name variants across different DBF schema versions
    OWNER_COLS   = ["OWNER", "OWN1", "OWNER_NAME", "OWNERNAME"]
    SITE_ADDR    = ["SITE_ADDR", "SITEADDR", "SITE_ADDRESS", "PROP_ADDR"]
    SITE_CITY    = ["SITE_CITY", "SITECITY", "PROP_CITY"]
    SITE_ZIP     = ["SITE_ZIP", "SITEZIP", "PROP_ZIP"]
    MAIL_ADDR    = ["ADDR_1", "MAILADR1", "MAIL_ADDR", "MAIL_ADDRESS"]
    MAIL_CITY    = ["CITY", "MAILCITY", "MAIL_CITY"]
    MAIL_STATE   = ["STATE", "MAILSTATE", "MAIL_STATE"]
    MAIL_ZIP     = ["ZIP", "MAILZIP", "MAIL_ZIP"]

    def __init__(self):
        self._index: dict[str, dict] = {}  # normalized_name → parcel row

    def _col(self, row: dict, candidates: list[str]) -> str:
        for c in candidates:
            if c in row and row[c]:
                return str(row[c]).strip()
        return ""

    def _index_name(self, name: str) -> str:
        return re.sub(r"\s+", " ", name.upper().strip())

    def _name_variants(self, full: str) -> list[str]:
        """Generate lookup variants: FIRST LAST, LAST FIRST, LAST, FIRST"""
        parts = full.split()
        variants = [full]
        if len(parts) >= 2:
            variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
            variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
        return [self._index_name(v) for v in variants]

    def load(self) -> bool:
        if not DBFREAD_AVAILABLE:
            log.warning("dbfread not installed; skipping parcel load")
            return False

        dbf_path = Path("data/parcel.dbf")
        dbf_path.parent.mkdir(parents=True, exist_ok=True)

        if not dbf_path.exists():
            downloaded = self._download_parcel_dbf(dbf_path)
            if not downloaded:
                log.warning("Could not download parcel DBF; enrichment disabled")
                return False

        try:
            log.info("Loading parcel DBF …")
            table = DBF(str(dbf_path), encoding="latin-1", ignore_missing_memofile=True)
            for row in table:
                row = dict(row)
                owner = self._col(row, self.OWNER_COLS)
                if not owner:
                    continue
                for variant in self._name_variants(owner):
                    if variant not in self._index:
                        self._index[variant] = row
            log.info(f"Parcel index built: {len(self._index):,} name keys")
            return True
        except Exception as exc:
            log.error(f"DBF load error: {exc}")
            return False

    def _download_parcel_dbf(self, dest: Path) -> bool:
        for url in self.PARCEL_URLS:
            for attempt in range(MAX_RETRIES):
                try:
                    log.info(f"Downloading parcel data from {url} (attempt {attempt+1})")
                    resp = requests.get(url, timeout=120, stream=True)
                    resp.raise_for_status()
                    raw = resp.content

                    # Might be a ZIP
                    if raw[:2] == b"PK":
                        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                            dbf_names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                            if not dbf_names:
                                log.warning("ZIP has no DBF files")
                                break
                            dbf_name = dbf_names[0]
                            with zf.open(dbf_name) as f:
                                dest.write_bytes(f.read())
                    else:
                        dest.write_bytes(raw)

                    log.info(f"Parcel DBF saved to {dest}")
                    return True
                except Exception as exc:
                    log.warning(f"Download attempt {attempt+1} failed: {exc}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
        return False

    def lookup(self, name: str) -> dict:
        """Return enrichment dict for an owner name, or empty dict."""
        if not name or not self._index:
            return {}
        for variant in self._name_variants(name):
            row = self._index.get(variant)
            if row:
                return {
                    "prop_address": self._col(row, self.SITE_ADDR),
                    "prop_city":    self._col(row, self.SITE_CITY),
                    "prop_state":   "TX",
                    "prop_zip":     self._col(row, self.SITE_ZIP),
                    "mail_address": self._col(row, self.MAIL_ADDR),
                    "mail_city":    self._col(row, self.MAIL_CITY),
                    "mail_state":   self._col(row, self.MAIL_STATE) or "TX",
                    "mail_zip":     self._col(row, self.MAIL_ZIP),
                }
        return {}


# ─────────────────────────────────────────────
# Harris County Clerk scraper (Playwright)
# ─────────────────────────────────────────────
class ClerkScraper:
    """
    Scrapes https://www.cclerk.hctx.net/PublicRecords.aspx
    using Playwright to handle ASP.NET __doPostBack forms.
    """

    BASE = "https://www.cclerk.hctx.net"

    def __init__(self, date_from: datetime, date_to: datetime):
        self.date_from = date_from
        self.date_to   = date_to
        self.records: list[dict] = []

    async def run(self) -> list[dict]:
        if not PLAYWRIGHT_AVAILABLE:
            log.error("Playwright unavailable; returning empty records")
            return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=HEADLESS)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()

            for doc_code in CLERK_DOC_CODES:
                for attempt in range(MAX_RETRIES):
                    try:
                        await self._scrape_doc_type(page, doc_code)
                        break
                    except Exception as exc:
                        log.warning(
                            f"[{doc_code}] attempt {attempt+1} failed: {exc}"
                        )
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(RETRY_DELAY)
                        else:
                            log.error(f"[{doc_code}] giving up after {MAX_RETRIES} attempts")

            await browser.close()

        log.info(f"Clerk scraper finished: {len(self.records)} records")
        return self.records

    async def _scrape_doc_type(self, page, doc_code: str):
        log.info(f"Scraping doc type: {doc_code}")

        # ── Load the search form ──────────────────────────────────────
        await page.goto(CLERK_URL, timeout=60_000)
        await page.wait_for_load_state("networkidle", timeout=30_000)

        # Try to find document-type dropdown / selector
        # The portal uses an ASP.NET UpdatePanel; selectors may vary.
        try:
            # Select document type
            await page.select_option(
                "select[id*='DocType'], select[name*='DocType'], #ddlDocType",
                value=doc_code,
                timeout=10_000,
            )
        except Exception:
            try:
                # Some versions use a text input for doc type code
                await page.fill(
                    "input[id*='DocType'], input[name*='DocType']",
                    doc_code,
                    timeout=5_000,
                )
            except Exception:
                log.warning(f"[{doc_code}] Could not locate doc-type selector – skipping")
                return

        # Fill date range
        date_from_str = self.date_from.strftime("%m/%d/%Y")
        date_to_str   = self.date_to.strftime("%m/%d/%Y")

        for sel in ["input[id*='DateFrom'], input[name*='DateFrom'], #txtDateFrom"]:
            try:
                await page.fill(sel, date_from_str, timeout=5_000)
                break
            except Exception:
                pass

        for sel in ["input[id*='DateTo'], input[name*='DateTo'], #txtDateTo"]:
            try:
                await page.fill(sel, date_to_str, timeout=5_000)
                break
            except Exception:
                pass

        # Submit search
        try:
            await page.click(
                "input[type='submit'][id*='Search'], button[id*='Search'], #btnSearch",
                timeout=10_000,
            )
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            log.warning(f"[{doc_code}] Submit click failed – trying Enter")
            await page.keyboard.press("Enter")
            await asyncio.sleep(3)

        # ── Parse results pages ───────────────────────────────────────
        page_num = 1
        while True:
            html    = await page.content()
            new_recs = self._parse_results_page(html, doc_code)
            self.records.extend(new_recs)
            log.info(f"  [{doc_code}] page {page_num}: {len(new_recs)} records")

            # Try to click "Next" page
            try:
                next_btn = await page.query_selector(
                    "a[id*='Next'], input[value='Next >'], "
                    "a:has-text('Next'), a:has-text('>')"
                )
                if not next_btn:
                    break
                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=20_000)
                page_num += 1
            except Exception:
                break

    def _parse_results_page(self, html: str, doc_code: str) -> list[dict]:
        """Parse search-result HTML table rows into record dicts."""
        soup = BeautifulSoup(html, "lxml")
        records = []

        # The portal typically renders an HTML table with class GridView or similar
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Detect header row
            headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
            if not any(k in " ".join(headers) for k in ("doc", "filed", "grantor", "grantee")):
                continue

            col_map = self._map_columns(headers)

            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells:
                    continue
                try:
                    rec = self._extract_row(cells, col_map, doc_code)
                    if rec:
                        records.append(rec)
                except Exception as exc:
                    log.debug(f"Row parse error: {exc}")

        return records

    def _map_columns(self, headers: list[str]) -> dict:
        """Build index → field mapping from header names."""
        mapping = {}
        for i, h in enumerate(headers):
            if "doc" in h and "num" in h:
                mapping["doc_num"] = i
            elif "doc" in h and "type" in h:
                mapping["doc_type_col"] = i
            elif "filed" in h or "record" in h or "date" in h:
                mapping.setdefault("filed", i)
            elif "grantor" in h or "owner" in h:
                mapping["grantor"] = i
            elif "grantee" in h:
                mapping["grantee"] = i
            elif "legal" in h or "description" in h:
                mapping["legal"] = i
            elif "amount" in h or "consid" in h:
                mapping["amount"] = i
        return mapping

    def _extract_row(self, cells: list, col_map: dict, doc_code: str) -> dict | None:
        def cell_text(idx):
            if idx is None or idx >= len(cells):
                return ""
            return cells[idx].get_text(strip=True)

        doc_num   = cell_text(col_map.get("doc_num"))
        filed     = cell_text(col_map.get("filed"))
        grantor   = cell_text(col_map.get("grantor"))
        grantee   = cell_text(col_map.get("grantee"))
        legal     = cell_text(col_map.get("legal"))
        amount_s  = cell_text(col_map.get("amount"))

        if not doc_num:
            return None

        # Normalise date → YYYY-MM-DD
        filed_norm = self._normalise_date(filed)

        # Normalise amount → float
        amount = self._parse_amount(amount_s)

        # Build direct URL to document
        # Harris County clerk doc link pattern (adjust if portal differs)
        clerk_url = (
            f"{self.BASE}/PublicAccess/ViewDoc.aspx?DocNum={doc_num}"
            if doc_num else ""
        )

        cat, cat_label = DOC_TYPES.get(doc_code, ("other", doc_code))

        return {
            "doc_num":   doc_num,
            "doc_type":  doc_code,
            "filed":     filed_norm,
            "cat":       cat,
            "cat_label": cat_label,
            "owner":     grantor,
            "grantee":   grantee,
            "amount":    amount,
            "legal":     legal,
            # Address fields filled later by parcel lookup
            "prop_address": "",
            "prop_city":    "",
            "prop_state":   "",
            "prop_zip":     "",
            "mail_address": "",
            "mail_city":    "",
            "mail_state":   "",
            "mail_zip":     "",
            "clerk_url":    clerk_url,
            "flags":        [],
            "score":        0,
        }

    @staticmethod
    def _normalise_date(raw: str) -> str:
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
        return raw.strip()

    @staticmethod
    def _parse_amount(raw: str) -> float | None:
        if not raw:
            return None
        cleaned = re.sub(r"[^\d.]", "", raw)
        try:
            return float(cleaned) if cleaned else None
        except ValueError:
            return None


# ─────────────────────────────────────────────
# GHL CSV export
# ─────────────────────────────────────────────
GHL_COLUMNS = [
    "First Name", "Last Name",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL",
]


def _split_name(full: str) -> tuple[str, str]:
    """Best-effort split of owner name into first / last."""
    if not full:
        return "", ""
    parts = full.strip().split()
    if len(parts) == 1:
        return "", parts[0]
    # Check for LAST, FIRST format
    if "," in full:
        last, *rest = [p.strip() for p in full.split(",", 1)]
        return " ".join(rest), last
    return " ".join(parts[:-1]), parts[-1]


def build_ghl_row(rec: dict) -> dict:
    first, last = _split_name(rec.get("owner", ""))
    return {
        "First Name":             first,
        "Last Name":              last,
        "Mailing Address":        rec.get("mail_address", ""),
        "Mailing City":           rec.get("mail_city", ""),
        "Mailing State":          rec.get("mail_state", ""),
        "Mailing Zip":            rec.get("mail_zip", ""),
        "Property Address":       rec.get("prop_address", ""),
        "Property City":          rec.get("prop_city", ""),
        "Property State":         rec.get("prop_state", ""),
        "Property Zip":           rec.get("prop_zip", ""),
        "Lead Type":              rec.get("cat_label", ""),
        "Document Type":          rec.get("doc_type", ""),
        "Date Filed":             rec.get("filed", ""),
        "Document Number":        rec.get("doc_num", ""),
        "Amount/Debt Owed":       rec.get("amount", ""),
        "Seller Score":           rec.get("score", 0),
        "Motivated Seller Flags": "; ".join(rec.get("flags", [])),
        "Source":                 "Harris County Clerk",
        "Public Records URL":     rec.get("clerk_url", ""),
    }


def write_ghl_csv(records: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=GHL_COLUMNS)
        writer.writeheader()
        for rec in records:
            try:
                writer.writerow(build_ghl_row(rec))
            except Exception as exc:
                log.debug(f"CSV row error: {exc}")
    log.info(f"GHL CSV written: {path} ({len(records)} rows)")


# ─────────────────────────────────────────────
# Output helpers
# ─────────────────────────────────────────────
def write_output(records: list[dict], date_from: datetime, date_to: datetime):
    now_str   = datetime.utcnow().isoformat() + "Z"
    with_addr = sum(1 for r in records if r.get("prop_address"))

    payload = {
        "fetched_at":   now_str,
        "source":       "Harris County Clerk – https://www.cclerk.hctx.net/",
        "date_range":   {
            "from": date_from.strftime("%Y-%m-%d"),
            "to":   date_to.strftime("%Y-%m-%d"),
        },
        "total":        len(records),
        "with_address": with_addr,
        "records":      records,
    }

    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info(f"Records written → {path}")

    return payload


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
async def main():
    now       = datetime.utcnow()
    date_to   = now
    date_from = now - timedelta(days=LOOKBACK_DAYS)

    log.info(f"Harris County Lead Scraper starting")
    log.info(f"Date range: {date_from.date()} → {date_to.date()}")

    # 1. Scrape clerk portal
    scraper = ClerkScraper(date_from, date_to)
    records = await scraper.run()

    # 2. Load parcel data for enrichment
    parcel = ParcelLookup()
    parcel_loaded = parcel.load()

    # 3. Enrich, flag, score
    for rec in records:
        try:
            # Parcel enrichment
            if parcel_loaded:
                enrichment = parcel.lookup(rec.get("owner", ""))
                rec.update(enrichment)

            # Flags & score
            flags       = compute_flags(rec, now)
            rec["flags"] = flags
            rec["score"] = compute_score(rec, flags, now)
        except Exception as exc:
            log.debug(f"Enrichment error for {rec.get('doc_num')}: {exc}")

    # Sort by score descending
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    # 4. Write outputs
    write_output(records, date_from, date_to)
    write_ghl_csv(records, GHL_CSV_PATH)

    log.info(
        f"Done. {len(records)} records, "
        f"{sum(1 for r in records if r.get('prop_address'))} with address, "
        f"avg score {sum(r.get('score',0) for r in records) / max(len(records),1):.1f}"
    )


if __name__ == "__main__":
    asyncio.run(main())
