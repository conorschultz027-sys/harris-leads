"""
Harris County Motivated Seller Lead Scraper v8
Back to Playwright - but using EXACT confirmed field names
and submitting via JavaScript click on the confirmed button ID.
"""
import asyncio, csv, json, logging, os, re, io, zipfile, time
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
# CONFIRMED field names from JS inspection
F_FROM   = "ctl00$ContentPlaceHolder1$txtFrom"
F_TO     = "ctl00$ContentPlaceHolder1$txtTo"
F_INST   = "ctl00$ContentPlaceHolder1$txtInstrument"
F_BTN_ID = "ctl00_ContentPlaceHolder1_btnSearch"
OUTPUT_PATHS = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV      = Path("data/ghl_export.csv")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
DOC_TYPES = {
    "L/P":      ("foreclosure",  "Lis Pendens"),
    "NOTICE":   ("foreclosure",  "Notice of Foreclosure"),
    "TRSALE":   ("tax",          "Trustee Sale"),
    "JUDGE":    ("judgment",     "Judgment"),
    "A/J":      ("judgment",     "Abstract of Judgment"),
    "DEED":     ("tax",          "Tax/Sheriff Deed"),
    "T/L":      ("tax_lien",     "Federal Tax Lien"),
    "LIEN":     ("lien",         "Lien"),
    "M/L":      ("lien",         "Mechanic Lien"),
    "HOA":      ("lien",         "HOA Lien"),
    "MED":      ("lien",         "Medicaid Lien"),
    
    "REL":      ("release",      "Release"),
    "PROB":     ("probate",      "Probate Document"),
    
    
}
def compute_flags(r, now):
    flags=[]
    cat,dt=r.get("cat",""),r.get("doc_type","")
    owner=(r.get("owner") or "").upper()
    if dt in ("LP","RELLP") or cat=="foreclosure": flags.append("Lis pendens")
    if dt=="NOTICE":   ("foreclosure",  "Notice of Foreclosure"),
    if cat=="judgment": flags.append("Judgment lien")
    if cat in ("tax","tax_lien"): flags.append("Tax lien")
    if dt=="MED":      ("lien",         "Medicaid Lien"),
    if cat=="probate": flags.append("Probate / estate")
    if any(k in owner for k in ("LLC","INC","CORP","LTD","LP ","L.P.","L.L.C")): flags.append("LLC / corp owner")
    try:
        if r.get("filed") and (now-datetime.strptime(r["filed"],"%Y-%m-%d")).days<=7:
            flags.append("New this week")
    except: pass
    return flags
def compute_score(r, flags):
    s=30+len(flags)*10
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
    def __init__(self, df, dt):
        self.df=df; self.dt=dt; self.records=[]
    async def run(self):
        if not PLAYWRIGHT_AVAILABLE: return []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox","--disable-setuid-sandbox",
                      "--disable-blink-features=AutomationControlled"]
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width":1366,"height":768},
                locale="en-US",
                timezone_id="America/Chicago",
            )
            await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            page = await ctx.new_page()
            # Warm up
            await page.goto(f"{BASE_URL}/applications/websearch/Home.aspx",
                           timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)
            for code,(cat,label) in DOC_TYPES.items():
                for attempt in range(3):
                    try:
                        await self._search(page, code, cat, label)
                        break
                    except Exception as e:
                        log.warning(f"[{code}] attempt {attempt+1}: {e}")
                        if attempt<2: await asyncio.sleep(3)
            await browser.close()
        log.info(f"Total: {len(self.records)}")
        return self.records
    async def _search(self, page, code, cat, label):
        log.info(f"Searching: {code}")
        await page.goto(RP_URL, timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)
        df_str = self.df.strftime("%m/%d/%Y")
        dt_str = self.dt.strftime("%m/%d/%Y")
        # Fill using exact name attributes
        await page.evaluate(f"""() => {{
            document.querySelector("input[name='{F_FROM}']").value = '{df_str}';
            document.querySelector("input[name='{F_TO}']").value = '{dt_str}';
            document.querySelector("input[name='{F_INST}']").value = '{code}';
        }}""")
        # Verify fields filled
        vals = await page.evaluate(f"""() => ({{
            from: document.querySelector("input[name='{F_FROM}']")?.value,
            to:   document.querySelector("input[name='{F_TO}']")?.value,
            inst: document.querySelector("input[name='{F_INST}']")?.value,
        }})""")
        log.info(f"  Fields: {vals}")
        # Click button by ID using JavaScript
        clicked = await page.evaluate(f"""() => {{
            const btn = document.getElementById('{F_BTN_ID}');
            if (btn) {{ btn.click(); return true; }}
            return false;
        }}""")
        log.info(f"  Button clicked via JS: {clicked}")
        await page.wait_for_load_state("networkidle", timeout=30000)
        await page.wait_for_timeout(2000)
        html = await page.content()
        log.info(f"  Response: {len(html)} chars")
        # Log tables for debugging
        soup = BeautifulSoup(html, "lxml")
        tables = soup.find_all("table")
        log.info(f"  Tables found: {len(tables)}")
        for i,tbl in enumerate(tables[:5]):
            rows=tbl.find_all("tr")
            if rows:
                hdrs=[td.get_text(" ",strip=True) for td in rows[0].find_all(["th","td"])]
                if any(hdrs): log.info(f"    Table {i} ({len(rows)} rows): {hdrs[:8]}")
        recs = self._parse(soup, code, cat, label)
        self.records.extend(recs)
        log.info(f"  [{code}] page 1: {len(recs)} records")
        # Pagination
        pg=1
        while pg<50:
            try:
                nxt=page.locator("a:has-text('Next'), input[value='Next >']").first
                if await nxt.count()==0: break
                await nxt.click()
                await page.wait_for_load_state("networkidle",timeout=20000)
                html=await page.content()
                soup=BeautifulSoup(html,"lxml")
                recs=self._parse(soup,code,cat,label)
                self.records.extend(recs)
                pg+=1
                log.info(f"  [{code}] page {pg}: {len(recs)} records")
                if not recs: break
            except: break
    def _parse(self, soup, code, cat, label):
        recs=[]
        for tbl in soup.find_all("table"):
            rows=tbl.find_all("tr")
            if len(rows)<2: continue
            hdrs=[th.get_text(" ",strip=True).lower() for th in rows[0].find_all(["th","td"])]
            joined=" ".join(hdrs)
            if not any(k in joined for k in ("doc","filed","grantor","instrument","grantee","file no","or name","ee name")): continue
            if len(hdrs)<3: continue
            col={}
            for i,h in enumerate(hdrs):
                hl=h.lower()
                if any(x in hl for x in ("file no","doc no","doc num","instrument","film","number")): col.setdefault("doc_num",i)
                elif any(x in hl for x in ("filed","record date","date")): col.setdefault("filed",i)
                elif any(x in hl for x in ("grantor","or name","debtor","seller")): col.setdefault("grantor",i)
                elif any(x in hl for x in ("grantee","ee name","creditor","buyer")): col.setdefault("grantee",i)
                elif any(x in hl for x in ("legal","description","subdiv")): col.setdefault("legal",i)
                elif any(x in hl for x in ("amount","consid")): col.setdefault("amount",i)
            if "doc_num" not in col and "filed" not in col: continue
            for row in rows[1:]:
                cells=row.find_all("td")
                if not cells: continue
                try:
                    def t(k):
                        i=col.get(k)
                        return cells[i].get_text(" ",strip=True) if i is not None and i<len(cells) else ""
                    doc_num=t("doc_num")
                    if not doc_num or len(doc_num)<2: continue
                    url=""
                    for cell in cells:
                        a=cell.find("a",href=True)
                        if a:
                            href=a["href"]
                            url=href if href.startswith("http") else BASE_URL+"/"+href.lstrip("/")
                            break
                    if not url: url=f"{RP_URL}?FileNum={doc_num}"
                    amt=None
                    try:
                        raw=re.sub(r"[^\d.]","",t("amount"))
                        if raw: amt=float(raw)
                    except: pass
                    fd=""
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
    URLS=["https://pdata.hcad.org/Pdata/download/Real_acct_owner.zip"]
    def __init__(self): self._idx={}
    def _col(self,row,cols):
        for c in cols:
            if c in row and row[c]: return str(row[c]).strip()
        return ""
    def _key(self,n): return re.sub(r"\s+"," ",n.upper().strip())
    def _variants(self,full):
        p=full.split(); v=[full]
        if len(p)>=2: v+=[f"{p[-1]} {' '.join(p[:-1])}",f"{p[-1]}, {' '.join(p[:-1])}"]
        return [self._key(x) for x in v]
    def load(self):
        if not DBFREAD_AVAILABLE: return False
        dbf=Path("data/parcel.dbf"); dbf.parent.mkdir(parents=True,exist_ok=True)
        if not dbf.exists():
            for url in self.URLS:
                try:
                    log.info("Downloading parcel data...")
                    r=requests.get(url,timeout=180); r.raise_for_status()
                    raw=r.content
                    if raw[:2]==b"PK":
                        with zipfile.ZipFile(io.BytesIO(raw)) as z:
                            names=[n for n in z.namelist() if n.lower().endswith(".dbf")]
                            if names: dbf.write_bytes(z.read(names[0]))
                    else: dbf.write_bytes(raw)
                    log.info("Parcel DBF saved"); break
                except Exception as e: log.warning(f"Parcel: {e}")
        if not dbf.exists(): return False
        try:
            count=0
            for row in DBF(str(dbf),encoding="latin-1",ignore_missing_memofile=True):
                try:
                    row=dict(row); owner=self._col(row,["OWNER","OWN1","OWNER_NAME"])
                    if not owner: continue
                    for v in self._variants(owner): self._idx.setdefault(v,row)
                    count+=1
                except: continue
            log.info(f"Parcel: {count:,}"); return True
        except Exception as e: log.error(f"DBF: {e}"); return False
    def lookup(self,name):
        if not name or not self._idx: return {}
        for v in self._variants(name):
            row=self._idx.get(v)
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
def write_ghl(records,path):
    path.parent.mkdir(parents=True,exist_ok=True)
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
def write_output(records,df,dt):
    payload={"fetched_at":datetime.utcnow().isoformat()+"Z","source":"Harris County Clerk",
        "date_range":{"from":df.strftime("%Y-%m-%d"),"to":dt.strftime("%Y-%m-%d")},
        "total":len(records),"with_address":sum(1 for r in records if r.get("prop_address")),"records":records}
    for p in OUTPUT_PATHS:
        p.parent.mkdir(parents=True,exist_ok=True)
        p.write_text(json.dumps(payload,indent=2,default=str),encoding="utf-8")
        log.info(f"→ {p}")
async def main():
    now=datetime.utcnow(); dt=now; df=now-timedelta(days=LOOKBACK)
    log.info(f"Harris County Scraper v8 | {df.date()} → {dt.date()}")
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
