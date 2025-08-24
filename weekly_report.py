# -*- coding: utf-8 -*-
"""Weekly marketplace ranking report

Reads last 7 days CSVs from Google Drive (rank/{oykorea,oyglobal,amazon,qoo10,daiso}),
aggregates weekly stats (OUT-penalty), and posts Slack blocks.
Product names are printed in FULL (raw).

Env:
  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN
  GDRIVE_FOLDER_ID, SLACK_WEBHOOK_URL
"""
import io, os, re, json, math, logging
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import pandas as pd
import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
L = logging.getLogger("weekly")

SUBFOLDERS = ["oykorea","oyglobal","amazon","qoo10","daiso"]
TOP_CAP = {"oykorea":100,"oyglobal":100,"amazon":100,"qoo10":200,"daiso":200}
PENALTY = {k:v+1 for k,v in TOP_CAP.items()}

RE_PROMO = re.compile(r"(올영픽|특가|1\+1|더블|기획|에디션)")
RE_PICK  = re.compile(r"(?i)(?<!올영)\bpick\b")

def key_from_url(market, url):
    if not isinstance(url, str): return None
    p = urlparse(url); qs = parse_qs(p.query or "")
    if market=="oykorea":  return qs.get("goodsNo",[None])[0]
    if market=="oyglobal": return qs.get("productId",[None])[0]
    if market=="amazon":
        m = re.search(r"/dp/([A-Z0-9]{10})", p.path or "", re.I)
        return (m.group(1) if m else qs.get("asin",[None])[0])
    if market=="qoo10":    return qs.get("product_code",[None])[0]
    if market=="daiso":    return qs.get("pdNo",[None])[0]
    return None

def drive_service():
    creds = Credentials(
        token=None,
        refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    creds.refresh(Request())
    return build("drive","v3",credentials=creds, cache_discovery=False)

def list_children_folders(svc, parent_id):
    q = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    out, tok = [], None
    while True:
        resp = svc.files().list(q=q, fields="nextPageToken, files(id,name)", pageToken=tok).execute()
        out += resp.get("files",[]); tok = resp.get("nextPageToken")
        if not tok: break
    return out

def list_files_in_folder(svc, folder_id):
    q = f"'{folder_id}' in parents and mimeType!='application/vnd.google-apps.folder' and trashed=false"
    out, tok = [], None
    while True:
        resp = svc.files().list(q=q, fields="nextPageToken, files(id,name,modifiedTime)", pageToken=tok).execute()
        out += resp.get("files",[]); tok = resp.get("nextPageToken")
        if not tok: break
    return out

DATE_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})")
def parse_date(name): m=DATE_RE.search(name or ""); return m.group(1) if m else None

def download_csv(svc, fid, name):
    req = svc.files().get_media(fileId=fid)
    buf = io.BytesIO(); downloader = MediaIoBaseDownload(buf, req)
    done=False
    while not done: status, done = downloader.next_chunk()
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:
            buf.seek(0); return pd.read_csv(buf, encoding=enc)
        except Exception: pass
    raise RuntimeError(f"CSV read fail: {name}")

def normalize_columns(df):
    colmap = {}
    for c in df.columns:
        lc = str(c).strip().lower()
        if lc in ("rank","순위"): colmap[c]="rank"
        elif lc in ("name","상품명","제품명","title"): colmap[c]="name"
        elif lc in ("brand","브랜드"): colmap[c]="brand"
        elif lc in ("url","링크"): colmap[c]="url"
        elif lc in ("discount","할인율","discount_rate"): colmap[c]="discount_rate"
        elif lc in ("price","가격"): colmap[c]="price"
        elif lc in ("category","카테고리"): colmap[c]="category"
    df = df.rename(columns=colmap)
    for col in ["rank","name","url"]:
        if col not in df.columns: df[col]=None
    keep = [c for c in ["rank","name","brand","price","discount_rate","category","url"] if c in df.columns]
    return df[keep]

def load_market_last7(svc, parent_id, market):
    sub = {f["name"]: f["id"] for f in list_children_folders(svc, parent_id)}
    if market not in sub:
        L.warning("subfolder missing: %s", market); return pd.DataFrame()
    files = list_files_in_folder(svc, sub[market])
    pairs = [(parse_date(f["name"]), f) for f in files if parse_date(f["name"])]
    pairs.sort(key=lambda x:x[0], reverse=True)
    dates = sorted({d for d,_ in pairs})[-7:]
    dates = sorted(dates)
    selected = [f for d,f in pairs if d in dates]
    frames=[]
    for f in selected:
        d=parse_date(f["name"])
        try:
            df=download_csv(svc, f["id"], f["name"])
            df=normalize_columns(df)
            df["rank"]=pd.to_numeric(df["rank"], errors="coerce")
            df["date"]=d; df["market"]=market
            df["key"]=df["url"].apply(lambda u: key_from_url(market,u))
            frames.append(df)
        except Exception as e:
            L.exception("read fail %s: %s", f["name"], e)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def weekly_aggregate(df, market):
    if df.empty: return {}
    cap, pen = TOP_CAP[market], PENALTY[market]
    df = df[df["rank"].le(cap)]
    latest_name = (df.sort_values("date").groupby("key")["name"]
                     .agg(lambda s: s.dropna().iloc[-1] if s.dropna().size else None))
    latest_brand = (df.sort_values("date").groupby("key")["brand"]
                     .agg(lambda s: s.dropna().iloc[-1] if s.dropna().size else None))
    dates = sorted(df["date"].unique())
    piv = df.pivot_table(index="key", columns="date", values="rank", aggfunc="min").reindex(columns=dates)
    piv_filled = piv.fillna(pen)
    summary = pd.DataFrame({
        "key": piv_filled.index,
        "name": latest_name.reindex(piv_filled.index).values,
        "brand": latest_brand.reindex(piv_filled.index).values,
        "avg_rank": piv_filled.mean(axis=1).values,
        "best_rank": piv.min(axis=1).values,
        "days_in": piv.notna().sum(axis=1).values,
    }).sort_values(["avg_rank","best_rank"]).reset_index(drop=True)
    top10 = summary.head(10).copy()

    # movement (last vs prev)
    mv = []
    if len(dates)>=2:
        t = df[df["date"]==dates[-1]].set_index("key")["rank"]
        p = df[df["date"]==dates[-2]].set_index("key")["rank"]
        for _,r in top10.iterrows():
            k=r["key"]; tr=t.get(k, math.nan); pr=p.get(k, math.nan)
            if pd.isna(pr) and not pd.isna(tr): mv.append("NEW")
            elif not pd.isna(pr) and pd.isna(tr): mv.append("OUT")
            elif pd.isna(pr) and pd.isna(tr): mv.append("")
            else:
                d=int(pr-tr)
                mv.append(f"▲{d}" if d>0 else ("▼{}".format(-d) if d<0 else "—"))
    else:
        mv = [""]*len(top10)
    top10["move"]=mv

    # tags (olive young only)
    if market in ("oykorea","oyglobal"):
        def tag(s):
            s = s or ""
            promo = bool(RE_PROMO.search(s)); pick=bool(RE_PICK.search(s))
            if promo and pick: return "프로모션 + Pick"
            if promo: return "프로모션"
            if pick: return "Pick"
            return ""
        top10["tag"]=top10["name"].apply(tag)
    else:
        top10["tag"]=""

    # brand share
    bs = (pd.concat([
            df.groupby("brand")["key"].nunique().rename("sku"),
            df.groupby("brand").size().rename("hits")
        ], axis=1).fillna(0).sort_values(["sku","hits"], ascending=False).head(10).reset_index())
    return {"dates":dates, "top10":top10, "brand_share":bs}

def to_slack_blocks(market, agg):
    title_map = {
        "oykorea":"올리브영 국내 Top100",
        "oyglobal":"올리브영 글로벌 Top100",
        "amazon":"아마존 US Top100",
        "qoo10":"큐텐 재팬 뷰티 Top200",
        "daiso":"다이소몰 뷰티/위생 Top200",
    }
    title = title_map.get(market, market)
    d0,d1 = (agg["dates"][0], agg["dates"][-1]) if agg.get("dates") else ("","")
    lines=[]
    for i,r in agg["top10"].reset_index(drop=True).iterrows():
        tag = f" · {r['tag']}" if r.get("tag") else ""
        move = f" {r['move']}" if r.get("move") else ""
        lines.append(f"{i+1}. {str(r['name'])}{tag} (등장 {int(r['days_in'])}일){move}")
    top10_txt = "\n".join(lines) if lines else "데이터 없음"

    bs_lines = [f"{str(r['brand'])} {int(r['sku'])}개 ({int(r['hits'])}회)" for _,r in agg["brand_share"].iterrows()]                if len(agg.get("brand_share",[])) else []
    brand_txt = " · ".join(bs_lines) if bs_lines else "데이터 없음"

    return [
        {"type":"header","text":{"type":"plain_text","text":f"📊 주간 리포트 · {title} ({d0}~{d1})"}},
        {"type":"section","text":{"type":"mrkdwn","text":f"*🏆 Top10 (패널티 평균, raw 제품명)*\n{top10_txt}"}},
        {"type":"section","text":{"type":"mrkdwn","text":f"*🏷️ 브랜드 점유율*\n{brand_txt}"}},
        {"type":"context","elements":[{"type":"mrkdwn","text":"※ 기준: url 키로 동일상품 식별, 미등장일 패널티 적용(Top100→101 / Top200→201)"}]}
    ]

def post_slack(blocks):
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url: print(json.dumps({"blocks":blocks}, ensure_ascii=False, indent=2)); return
    r = requests.post(url, json={"blocks":blocks}, timeout=30)
    if r.status_code>=300: L.error("Slack error %s %s", r.status_code, r.text)

def main():
    parent = os.environ["GDRIVE_FOLDER_ID"]
    svc = drive_service()
    for market in SUBFOLDERS:
        try:
            df = load_market_last7(svc, parent, market)
            if df.empty: 
                L.warning("no data: %s", market); 
                continue
            agg = weekly_aggregate(df, market)
            blocks = to_slack_blocks(market, agg)
            post_slack(blocks)
        except Exception as e:
            L.exception("market %s failed: %s", market, e)

if __name__ == "__main__":
    main()
