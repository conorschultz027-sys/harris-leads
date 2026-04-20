"""
Harris County Motivated Seller Lead Scraper v4
Form fields confirmed from screenshot:
- Date (From) / Date (To) — placeholder MM/DD/YYYY
- Instrument Type — text input
- SEARCH button
"""

import asyncio
import csv
import json
import logging
import os
import re
import io
import zipfile
import time
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

BASE_URL  = "https://www.cclerk.hctx.net"
RP_URL    = f"{BASE_URL}/applications/websearch/RP.aspx"
FRCL_URL  = f"{BASE_URL}/applications/websearch/FRCL_R.aspx"
LOOKBACK  = int(os.environ.get("LOOKBACK_DAYS", 7))
HEADLESS  = os.environ.get("HEADLESS", "true").lower() != "false"

OUTPUT_PATHS = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV      = Path("data/ghl_export.csv")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DOC_TYPES = {
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

def compute_flags(r, now):
    flags = []
    cat, dt = r.get("cat",""), r.get("doc_type","")
    owner = (r.get("owner") or "").upper()
    if dt in ("LP","RELLP") or cat=="foreclosure": flags.append("Lis pendens")
    if dt=="NOFC": flags.append("Pre-foreclosure")
    if cat=="judgment": flags.append("Judgment lien")
    if cat in ("tax","tax_lien"): flags.append("Tax lien")
    if dt=="LNMECH": flags.append("Mechanic lien")
    if cat=="probate": flags.append("Probate / estate")
    if any(k in owner for k in ("LLC","INC","CORP","LTD","LP ","L.P.","L.L.C")): flags.append("LLC / corp owner")
    try:
        if r.get("filed") and (now-datetime.strptime(r["filed"],"%Y-%m-%d")).days<=7:
            flags.append("New this week")
    except: pass
    return flags

def compute_score(r, flags):
    s = 30 + len(flags)*10
    if r.get("doc_type") in ("LP","NOFC") and r.get("cat")=="foreclosure": s+=20
    try:
        a=float(str(r.get("amount") or 0).replace(",","").replace("$",""))
        if a>100000: s+=15
        elif a>50000: s+=10
    except: pass
    if "New this week" in flags: s+=5
    if r.get("prop_address"): s+=5
    return min(s,100)


class ClerkScraper:
    def __init__(self, date_from, date_to):
        self.df = date_from
        self.dt = date_to
        self.records = []

    async def run(self):
        if not PLAYWRIGHT_AVAILABLE:
            log.error("Playwright not available"); return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage"]
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
                viewport={"width":1280,"height":900}
            )
            page = await ctx.new_page()

            # First: get the real input field names by inspecting the page
            await page.goto(RP_URL, timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)

            # Log ALL input field names so we know exactly what to target
            inputs = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll('input')).map(i => ({
                    name: i.name,
                    id: i.id,
                    type: i.type,
                    placeholder: i.placeholder,
                    value: i.value
                }));
            }""")
            log.info(f"Form inputs found: {json.dumps(inputs, indent=2)}")

            # Now search for each doc type
            for code, (cat, label) in DOC_TYPES.items():
                for attempt in range(3):
                    try:
                        await self._search(page, code, cat, label, inputs)
                        break
                    except Exception as e:
                        log.warning(f"[{code}] attempt {attempt+1}: {e}")
                        if attempt < 2: await asyncio.sleep(3)

            # Foreclosure dedicated page
            try:
                await self._search_frcl(page)
            except Exception as e:
                log.warning(f"FRCL: {e}")

            await browser.close()

        log.info(f"Total: {len(self.records)} records")
        return self.records

    async def _fill_field(self, page, inputs, keywords, value):
        """Fill a field by matching input name/id/placeholder to keywords."""
        for inp in inputs:
            name = (inp.get("name") or "").lower()
            iid  = (inp.get("id") or "").lower()
            ph   = (inp.get("placeholder") or "").lower()
            combined = f"{name} {iid} {ph}"
            if any(kw in combined for kw in keywords):
                selector = f"input[name='{inp['name']}']" if inp['name'] else f"input[id='{inp['id']}']"
                try:
                    await page.fill(selector, value, timeout=5000)
                    log.info(f"  Filled '{inp['name'] or inp['id']}' with '{value}'")
                    return True
                except Exception as e:
                    log.debug(f"  Fill failed for {selector}: {e}")
        return False

    async def _search(self, page, code, cat, label, inputs):
        log.info(f"Searching: {code}")
        await page.goto(RP_URL, timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)

        df_str = self.df.strftime("%m/%d/%Y")
        dt_str = self.dt.strftime("%m/%d/%Y")

        # Fill dates using confirmed placeholder text "MM/DD/YYYY"
        await self._fill_field(page, inputs, ["from","begin","datefrom","startdate","mm/dd/yyyy"], df_str)
        await self._fill_field(page, inputs, ["to","end","dateto","enddate"], dt_str)

        # Fill Instrument Type
        await self._fill_field(page, inputs, ["instrument","type","instrtype","doctype"], code)

        # Click SEARCH button
        for sel in ["input[value='SEARCH']","input[value='Search']","button:has-text('SEARCH')","input[type='submit']"]:
            try:
                await page.click(sel, timeout=5000)
                break
            except: pass

        await page.wait_for_load_state("networkidle", timeout=30000)
        await self._paginate(page, code, cat, label)

    async def _search_frcl(self, page):
        log.info("Foreclosure notices page")
        await page.goto(FRCL_URL, timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)

        # Get inputs for this page too
        inputs = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('input')).map(i => ({
                name: i.name, id: i.id, type: i.type, placeholder: i.placeholder
            }));
        }""")
        log.info(f"FRCL inputs: {inputs}")

        df_str = self.df.strftime("%m/%d/%Y")
        dt_str = self.dt.strftime("%m/%d/%Y")

        await self._fill_field(page, inputs, ["from","begin","start"], df_str)
        await self._fill_field(page, inputs, ["to","end"], dt_str)

        for sel in ["input[value='SEARCH']","input[value='Search']","input[type='submit']","button[type='submit']"]:
            try: await page.click(sel, timeout=5000); break
            except: pass

        await page.wait_for_load_state("networkidle", timeout=30000)
        await self._paginate(page, "NOFC", "foreclosure", "Notice of Foreclosure")

    async def _paginate(self, page, code, cat, label):
        pg = 1
        while pg <= 50:
            html = await page.content()
            recs = self._parse(html, code, cat, label)
            self.records.extend(recs)
            log.info(f"  [{code}] page {pg}: {len(recs)} records")
            try:
                nxt = page.locator("a:has-text('Next'), input[value='Next >']").first
                if await nxt.count() == 0: break
                await nxt.click()
                await page.wait_for_load_state("networkidle", timeout=20000)
                pg += 1
            except: break

    def _parse(self, html, code, cat, label):
        soup = BeautifulSoup(html, "lxml")
        recs = []
        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            if len(rows) < 2: continue
            hdrs = [th.get_text(" ", strip=True).lower() for th in rows[0].find_all(["th","td"])]
            if not any(k in " ".join(hdrs) for k in ("doc","filed","grantor","instrument","grantee")): continue
            col = {}
            for i,h in enumerate(hdrs):
                if any(x in h for x in ("doc num","instrument","file num")): col.setdefault("doc_num",i)
                elif "filed" in h or h=="date": col.setdefault("filed",i)
                elif any(x in h for x in ("grantor","debtor","seller")): col.setdefault("grantor",i)
                elif any(x in h for x in ("grantee","creditor","buyer")): col.setdefault("grantee",i)
                elif "legal" in h or "description" in h: col.setdefault("legal",i)
                elif "amount" in h or "consid" in h: col.setdefault("amount",i)
            if not col: continue
            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells: continue
                try:
                    def t(k):
                        i=col.get(k)
                        return cells[i].get_text(" ",strip=True) if i is not None and i<len(cells) else ""
                    doc_num = t("doc_num")
                    if not doc_num: continue
                    url = ""
                    for cell in cells:
                        a = cell.find("a", href=True)
                        if a:
                            href = a["href"]
                            url = href if href.startswith("http") else BASE_URL+"/"+href.lstrip("/")
                            break
                    if not url: url = f"{RP_URL}?FileNum={doc_num}"
                    amt = None
                    try:
                        raw = re.sub(r"[^\d.]","",t("amount"))
                        if raw: amt = float(raw)
                    except: pass
                    fd = ""
                    for fmt in ("%m/%d/%Y","%Y-%m-%d","%m-%d-%Y"):
                        try: fd=datetime.strptime(t("filed").strip(),fmt).strftime("%Y-%m-%d"); break
                        except: pass
                    recs.append({"doc_num":doc_num,"doc_type":code,"filed":fd,"cat":cat,"cat_label":label,
                        "owner":t("grantor"),"grantee":t("grantee"),"amount":amt,"legal":t("legal"),
                        "prop_address":"","prop_city":"","prop_state":"","prop_zip":"",
                        "mail_address":"","mail_city":"","mail_state":"","mail_zip":"",
                        "clerk_url":url,"flags":[],"score":0})
                except Exception as e: log.debug(f"row: {e}")
        return recs


class ParcelLookup:
    URLS = ["https://pdata.hcad.org/Pdata/download/Real_acct_owner.zip"]
    def __init__(self): self._idx = {}
    def _col(self, row, cols):
        for c in cols:
            if c in row and row[c]: return str(row[c]).strip()
        return ""
    def _key(self, n): return re.sub(r"\s+"," ",n.upper().strip())
    def _variants(self, full):
        p = full.split(); v = [full]
        if len(p)>=2: v += [f"{p[-1]} {' '.join(p[:-1])}", f"{p[-1]}, {' '.join(p[:-1])}"]
        return [self._key(x) for x in v]
    def load(self):
        if not DBFREAD_AVAILABLE: return False
        dbf = Path("data/parcel.dbf")
        dbf.parent.mkdir(parents=True, exist_ok=True)
        if not dbf.exists():
            for url in self.URLS:
                try:
                    log.info("Downloading parcel data...")
                    r = requests.get(url, timeout=180); r.raise_for_status()
                    raw = r.content
                    if raw[:2]==b"PK":
                        with zipfile.ZipFile(io.BytesIO(raw)) as z:
                            names=[n for n in z.namelist() if n.lower().endswith(".dbf")]
                            if names: dbf.write_bytes(z.read(names[0]))
                    else: dbf.write_bytes(raw)
                    log.info("Parcel DBF saved"); break
                except Exception as e: log.warning(f"Parcel download: {e}")
        if not dbf.exists(): return False
        try:
            count = 0
            for row in DBF(str(dbf), encoding="latin-1", ignore_missing_memofile=True):
                try:
                    row=dict(row)
                    owner=self._col(row,["OWNER","OWN1","OWNER_NAME"])
                    if not owner: continue
                    for v in self._variants(owner): self._idx.setdefault(v,row)
                    count+=1
                except: continue
            log.info(f"Parcel: {count:,} records, {len(self._idx):,} keys")
            return True
        except Exception as e: log.error(f"DBF: {e}"); return False
    def lookup(self, name):
        if not name or not self._idx: return {}
        for v in self._variants(name):
            row = self._idx.get(v)
            if row:
                def c(cols): return self._col(row,cols)
                return {"prop_address":c(["SITE_ADDR","SITEADDR"]),"prop_city":c(["SITE_CITY","SITECITY"]),
                        "prop_state":"TX","prop_zip":c(["SITE_ZIP","SITEZIP"]),
                        "mail_address":c(["ADDR_1","MAILADR1"]),"mail_city":c(["CITY","MAILCITY"]),
                        "mail_state":c(["STATE","MAILSTATE"]) or "TX","mail_zip":c(["ZIP","MAILZIP"])}
        return {}


GHL_COLS=["First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
          "Property Address","Property City","Property State","Property Zip","Lead Type","Document Type",
          "Date Filed","Document Number","Amount/Debt Owed","Seller Score","Motivated Seller Flags","Source","Public Records URL"]

def write_ghl(records, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=GHL_COLS); w.writeheader()
        for r in records:
            try:
                nm=(r.get("owner") or "").strip()
                if "," in nm: last,*rest=nm.split(",",1); first=" ".join(rest).strip()
                else: p=nm.split(); first=" ".join(p[:-1]) if len(p)>1 else ""; last=p[-1] if p else ""
                w.writerow({"First Name":first,"Last Name":last,
                    "Mailing Address":r.get("mail_address",""),"Mailing City":r.get("mail_city",""),
                    "Mailing State":r.get("mail_state",""),"Mailing Zip":r.get("mail_zip",""),
                    "Property Address":r.get("prop_address",""),"Property City":r.get("prop_city",""),
                    "Property State":r.get("prop_state",""),"Property Zip":r.get("prop_zip",""),
                    "Lead Type":r.get("cat_label",""),"Document Type":r.get("doc_type",""),
                    "Date Filed":r.get("filed",""),"Document Number":r.get("doc_num",""),
                    "Amount/Debt Owed":r.get("amount",""),"Seller Score":r.get("score",0),
                    "Motivated Seller Flags":"; ".join(r.get("flags",[])),"Source":"Harris County Clerk",
                    "Public Records URL":r.get("clerk_url","")})
            except: pass
    log.info(f"GHL CSV: {path} ({len(records)} rows)")

def write_output(records, df, dt):
    payload={"fetched_at":datetime.utcnow().isoformat()+"Z","source":"Harris County Clerk",
        "date_range":{"from":df.strftime("%Y-%m-%d"),"to":dt.strftime("%Y-%m-%d")},
        "total":len(records),"with_address":sum(1 for r in records if r.get("prop_address")),"records":records}
    for p in OUTPUT_PATHS:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(payload,indent=2,default=str),encoding="utf-8")
        log.info(f"→ {p}")

async def main():
    now=datetime.utcnow(); dt=now; df=now-timedelta(days=LOOKBACK)
    log.info(f"Harris County Scraper | {df.date()} → {dt.date()}")
    records=await ClerkScraper(df,dt).run()
    parcel=ParcelLookup(); loaded=parcel.load()
    for r in records:
        try:
            if loaded: r.update(parcel.lookup(r.get("owner","")))
            flags=compute_flags(r,now); r["flags"]=flags; r["score"]=compute_score(r,flags)
        except: pass
    records.sort(key=lambda r:r.get("score",0),reverse=True)
    seen=set(); unique=[]
    for r in records:
        k=r.get("doc_num") or (r.get("owner","")+ r.get("filed",""))
        if k and k not in seen: seen.add(k); unique.append(r)
    records=unique
    write_output(records,df,dt)
    write_ghl(records,GHL_CSV)
    log.info(f"Done: {len(records)} records | {sum(1 for r in records if r.get('prop_address'))} with address")

if __name__=="__main__":
    asyncio.run(main())
