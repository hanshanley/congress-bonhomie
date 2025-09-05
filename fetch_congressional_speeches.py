import os
import re
import json
import time
import argparse
from datetime import date
from typing import Optional, Dict, Any, Iterator, List, Tuple

try:
    import requests
except ModuleNotFoundError:
    raise SystemExit("Missing dependency: requests. Install with 'pip install requests'")


BASE = "https://api.govinfo.gov"


def compact_whitespace(s: str) -> str:
    import re as _re
    return _re.sub(r"\s+", " ", (s or "").strip())


def _get(path: str, api_key: str, params: Optional[Dict[str, Any]] = None, stream: bool = False):
    params = dict(params or {})
    params["api_key"] = api_key
    for attempt in range(6):
        r = requests.get(BASE + path, params=params, timeout=60, stream=stream)
        if r.status_code in (429, 502, 503, 504):
            time.sleep(min(2 ** attempt, 10))
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()


def iter_crec_packages(api_key: str, start_date: str, end_date: str, page_size: int = 100, rate_delay: float = 0.2) -> Iterator[Dict[str, Any]]:
    offset = 0
    while True:
        resp = _get("/collections/CREC", api_key, params={
            "startDate": start_date,
            "endDate": end_date,
            "pageSize": page_size,
            "offset": offset,
        }).json()
        items = resp.get("packages", []) or []
        if not items:
            break
        for p in items:
            yield p
        if len(items) < page_size:
            break
        offset += page_size
        time.sleep(rate_delay)


def iter_granules(api_key: str, package_id: str, page_size: int = 100, rate_delay: float = 0.2) -> Iterator[Dict[str, Any]]:
    offset = 0
    while True:
        resp = _get(f"/packages/{package_id}/granules", api_key, params={
            "pageSize": page_size,
            "offset": offset,
        }).json()
        items = resp.get("granules", []) or []
        if not items:
            break
        for g in items:
            yield g
        if len(items) < page_size:
            break
        offset += page_size
        time.sleep(rate_delay)


def get_granule_summary(api_key: str, package_id: str, granule_id: str) -> Dict[str, Any]:
    return _get(f"/packages/{package_id}/granules/{granule_id}/summary", api_key).json()


def fetch_granule_text(api_key: str, package_id: str, granule_id: str) -> Tuple[Optional[str], Dict[str, Any]]:
    summary = get_granule_summary(api_key, package_id, granule_id)
    dl = summary.get("download") or {}
    url = dl.get("xmlLink") or dl.get("txtLink") or dl.get("htmLink") or dl.get("htmlLink")
    if not url:
        return None, summary
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    return r.text, summary


def extract_speeches_from_xml(xml_text: str) -> List[Dict[str, Any]]:
    import xml.etree.ElementTree as ET
    speeches: List[Dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return speeches

    def tagname(el):
        return el.tag.split('}')[-1]

    for node in root.iter():
        if tagname(node) == 'speaking':
            speaker = node.attrib.get('speaker') or node.attrib.get('speaker_name') or node.attrib.get('who') or ''
            bioguide = (node.attrib.get('bioGuideId') or node.attrib.get('bioguide_id') or
                        node.attrib.get('bioGuideID') or node.attrib.get('bioguideId') or '')
            text = compact_whitespace(''.join(node.itertext()))
            if text:
                speeches.append({
                    'speaker': speaker,
                    'bioguide_id': bioguide,
                    'text': text,
                })

    if not speeches:
        paras: List[str] = []
        for node in root.iter():
            if tagname(node) == 'p':
                t = compact_whitespace(''.join(node.itertext()))
                if t:
                    paras.append(t)
        if paras:
            speeches.append({'speaker': '', 'bioguide_id': '', 'text': '\n\n'.join(paras)})

    return speeches


def parse_page_from_granule_id(granule_id: str) -> Optional[str]:
    m = re.search(r'(Pg[SH]\d+(?:-\d+)?)', granule_id or '')
    return m.group(1) if m else None


def jsonl_to_csv(jsonl_path: str, csv_path: str, field_order: Optional[List[str]] = None) -> int:
    import csv
    rows = []
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            rows.append(json.loads(line))
    if not rows:
        return 0
    if not field_order:
        field_order = [
            'date', 'chamber', 'speaker', 'bioguide_id', 'title', 'page', 'package_id', 'granule_id', 'text'
        ]
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=field_order, extrasaction='ignore')
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Fetch Congressional Record speeches from GovInfo")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--out", default="data", help="Output directory (default: data)")
    parser.add_argument("--csv", action="store_true", help="Also write a CSV next to JSONL")
    parser.add_argument("--max-packages", type=int, default=None, help="Limit number of packages (testing)")
    parser.add_argument("--max-granules", type=int, default=None, help="Limit granules per package (testing)")
    parser.add_argument("--rate-delay", type=float, default=0.2, help="Delay between API calls (seconds)")
    args = parser.parse_args()

    api_key = os.getenv("GOVINFO_API_KEY")
    if not api_key:
        raise SystemExit("Set GOVINFO_API_KEY environment variable with your GovInfo API key")

    os.makedirs(args.out, exist_ok=True)
    jsonl_path = os.path.join(args.out, f"speeches_{args.start}_to_{args.end}.jsonl")
    csv_path = os.path.join(args.out, f"speeches_{args.start}_to_{args.end}.csv")

    count_packages = 0
    count_granules = 0
    count_speeches = 0

    print(f"Fetching CREC packages {args.start} to {args.end}...")
    with open(jsonl_path, 'w', encoding='utf-8') as out:
        for p in iter_crec_packages(api_key, args.start, args.end, rate_delay=args.rate_delay):
            package_id = p.get('packageId')
            pkg_date = p.get('dateIssued')
            if not package_id:
                continue
            count_packages += 1
            print(f"Package {count_packages}: {package_id} ({pkg_date})")
            granules_seen = 0
            for g in iter_granules(api_key, package_id, rate_delay=args.rate_delay):
                granule_id = g.get('granuleId')
                chamber = (g.get('granuleClass') or '').upper()
                if not granule_id:
                    continue
                granules_seen += 1
                if args.max_granules and granules_seen > args.max_granules:
                    break

                try:
                    text, summary = fetch_granule_text(api_key, package_id, granule_id)
                except Exception as e:
                    print(f"  - Failed to fetch {granule_id}: {e}")
                    continue
                if not text:
                    continue

                speeches = extract_speeches_from_xml(text)
                page = parse_page_from_granule_id(granule_id)
                title = (summary.get('title') or g.get('title') or '').strip()

                for sp in speeches:
                    rec = {
                        'date': pkg_date,
                        'package_id': package_id,
                        'granule_id': granule_id,
                        'chamber': chamber,
                        'page': page,
                        'title': title,
                        **sp,
                    }
                    out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    count_speeches += 1

                count_granules += 1
                if count_granules % 25 == 0:
                    print(f"  - Processed {count_granules} granules, {count_speeches} speeches so far...")
                time.sleep(args.rate_delay)

            if args.max_packages and count_packages >= args.max_packages:
                break

    print(f"Done. Packages: {count_packages}, granules: {count_granules}, speeches: {count_speeches}.")
    print(f"JSONL: {jsonl_path}")

    if args.csv:
        rows = jsonl_to_csv(jsonl_path, csv_path)
        print(f"CSV: {csv_path} ({rows} rows)")


if __name__ == "__main__":
    main()

