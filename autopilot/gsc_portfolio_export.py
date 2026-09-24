"""Central Search Console export for the user's editorial portfolio."""
import csv, json
from datetime import date, timedelta, timezone, datetime
from pathlib import Path
from autopilot.gsc_fetch import build_service

SITES={
 "defensapenalcordoba.com.ar":"sc-domain:defensapenalcordoba.com.ar",
 "pediatriacordoba.com.ar":"sc-domain:pediatriacordoba.com.ar",
 "sucesionescordoba.com.ar":"sc-domain:sucesionescordoba.com.ar",
 "sailhalong.com":"sc-domain:sailhalong.com",
}
PAGE_SIZE=25000
FIELDS=["page","query","clicks","impressions","ctr","position","startDate","endDate"]

def query_rows(api,site,start,end,dimensions):
    out=[]; offset=0
    while True:
        body={"startDate":start.isoformat(),"endDate":end.isoformat(),"dimensions":dimensions,"type":"web","rowLimit":PAGE_SIZE,"startRow":offset}
        batch=api.searchanalytics().query(siteUrl=site,body=body).execute().get("rows",[])
        for item in batch:
            keys=dict(zip(dimensions,item.get("keys",[])))
            out.append({"page":keys.get("page",""),"query":keys.get("query",""),"clicks":item.get("clicks",0),"impressions":item.get("impressions",0),"ctr":item.get("ctr",0),"position":item.get("position",0),"startDate":start.isoformat(),"endDate":end.isoformat()})
        if len(batch)<PAGE_SIZE:return out
        offset+=PAGE_SIZE
        if offset>=50000:raise RuntimeError("Search Console row cap reached")

def write(path,rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=FIELDS);w.writeheader();w.writerows(rows)

def main():
    api=build_service();end=date.today()-timedelta(days=3);failures=[]
    portfolio={"generated_at_utc":datetime.now(timezone.utc).isoformat(),"data_end_date":end.isoformat(),"sites":{}}
    for slug,site in SITES.items():
        manifest={"property":site,"windows":{}}
        try:
            for days in (28,90):
                start=end-timedelta(days=days-1);counts={}
                for label,dims in (("queries",["query"]),("pages",["page"]),("page_queries",["page","query"])):
                    rows=query_rows(api,site,start,end,dims)
                    write(Path("gsc-portfolio-export")/slug/f"{days}d_{label}.csv",rows)
                    if days==28 and label=="page_queries":write(Path("data/search-console")/slug/"gsc_latest.csv",rows)
                    counts[label]=len(rows)
                manifest["windows"][str(days)]={"start":start.isoformat(),"end":end.isoformat(),"rows":counts}
            manifest["status"]="success"
        except Exception as exc:
            message=f"{type(exc).__name__}: {exc}"
            manifest["status"]="pending_permission" if "403" in message or "sufficient permission" in message else "failed"
            manifest["error"]=message;failures.append(slug)
        portfolio["sites"][slug]=manifest
    root=Path("gsc-portfolio-export");root.mkdir(exist_ok=True)
    (root/"manifest.json").write_text(json.dumps(portfolio,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(portfolio))
    if failures:
        print("Portfolio export incomplete; access or query errors: "+", ".join(failures))
if __name__=="__main__":main()
