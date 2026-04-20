"""
Harris County Motivated Seller Lead Scraper v6
Uses requests + BeautifulSoup to handle ASP.NET ViewState properly.
Gets ViewState from page, then POSTs form with all required fields.
"""

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
    from dbfread import DBF
    DBFREAD_AVAILABLE = True
except ImportError:
    DBFREAD_AVAILABLE = False

BASE_URL = "https://www.cclerk.hctx.net"
RP_URL   = f"{BASE_URL}/applications/websearch/RP.aspx"
LOOKBACK = int(os.environ.get("LOOKBACK_DAYS", 7))

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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": RP_URL,
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
        self.df     = date_from
        self.dt     = date_to
        self.records = []
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def run(self):
        # Get initial page to extract ViewState
        log.info("Loading RP search page...")
        for attempt in range(3):
            try:
                resp = self.session.get(RP_URL, timeout=30)
                resp.raise_for_status()
                break
            except Exception as e:
                log.warning(f"GET attempt {attempt+1}: {e}")
                time.sleep(5)
        else:
            log.error("Could not load RP page"); return []

        soup = BeautifulSoup(resp.text, "lxml")

        # Extract ALL hidden fields (ViewState, EventValidation, etc.)
        base_data = {}
        for inp in soup.find_all("input", type="hidden"):
            name = inp.get("name","")
            val  = inp.get("value","")
            if name:
                base_data[name] = val
                if "VIEWSTATE" in name.upper():
                    log.info(f"  Found hidden field: {name[:40]}... ({len(val)} chars)")

        log.info(f"  Total hidden fields: {len(base_data)}")

        df_str = self.df.strftime("%m/%d/%Y")
        dt_str = self.dt.strftime("%m/%d/%Y")

        # Search each doc type
        for code, (cat, label) in DOC_TYPES.items():
            for attempt in range(3):
                try:
                    self._search(base_data.copy(), code, cat, label, df_str, dt_str)
                    break
                except Exception as e:
                    log.warning(f"[{code}] attempt {attempt+1}: {e}")
                    if attempt < 2: time.sleep(5)

        log.info(f"Total: {len(self.records)} records")
        return self.records

    def _search(self, data, code, cat, label, df_str, dt_str):
        log.info(f"Searching: {code}")

        # Add form fields using EXACT confirmed names
        data.update({
            "ctl00$ContentPlaceHolder1$txtFrom":       df_str,
            "ctl00$ContentPlaceHolder1$txtTo":         dt_str,
            "ctl00$ContentPlaceHolder1$txtInstrument": code,
            "ctl00$ContentPlaceHolder1$txtFileNo":     "",
            "ctl00$ContentPlaceHolder1$txtFilmCd":     "",
            "ctl00$ContentPlaceHolder1$txtOR":         "",
            "ctl00$ContentPlaceHolder1$txtEE":         "",
            "ctl00$ContentPlaceHolder1$txtNameTee":    "",
            "ctl00$ContentPlaceHolder1$txtDesc":       "",
            "ctl00$ContentPlaceHolder1$txtVolNo":      "",
            "ctl00$ContentPlaceHolder1$txtPageNo":     "",
            "ctl00$ContentPlaceHolder1$btnSearch":     "Search",
        })

        log.info(f"  POSTing: from={df_str}, to={dt_str}, instrument={code}")

        resp = self.session.post(RP_URL, data=data, timeout=30)
        resp.raise_for_status()

        log.info(f"  Response: {resp.status_code}, {len(resp.text)} chars")

        # Check if we got results or the search form back
        if "No Records Found" in resp.text or "no records" in resp.text.lower():
            log.info(f"  [{code}] No records found")
            return

        soup = BeautifulSoup(resp.text, "lxml")

        # Look for results table
        recs = self._parse(soup, code, cat, label)
        self.records.extend(recs)
        log.info(f"  [{code}] page 1: {len(recs)} records")

        # Handle pagination via __doPostBack
        page_num = 1
        while page_num < 50:
            # Look for Next page link
            next_link = None
            for a in soup.find_all("a", href=True):
                if "Next" in a.get_text() or ">" in a.get_text():
                    next_link = a
                    break

            if not next_link:
                break

            # Extract __doPostBack arguments
            href = next_link.get("href","")
            match = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
            if not match:
                break

            data["__EVENTTARGET"]   = match.group(1)
            data["__EVENTARGUMENT"] = match.group(2)
            data.pop("ctl00$ContentPlaceHolder1$btnSearch", None)

            resp = self.session.post(RP_URL, data=data, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            recs = self._parse(soup, code, cat, label)
            self.records.extend(recs)
            page_num += 1
            log.info(f"  [{code}] page {page_num}: {len(recs)} records")

            if not recs:
                break

    def _parse(self, soup, code, cat, label):
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
            log.info(f"Parcel: {count:,} records")
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

def main():
    now=datetime.utcnow(); dt=now; df=now-timedelta(days=LOOKBACK)
    log.info(f"Harris County Scraper v6 | {df.date()} → {dt.date()}")
    records=ClerkScraper(df,dt).run()
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
    main()
