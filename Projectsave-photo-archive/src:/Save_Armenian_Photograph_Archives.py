from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

BASE = "https://projectsave.catalogaccess.com"
SEARCH_URL = f"{BASE}/api/search/catalogitems/2/keyword"

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": BASE,
    "Referer": f"{BASE}/photos",
}

DEFAULT_COLUMNS = [
    "id",
    "title",
    "object_id",
    "object_name",
    "other_number",
    "place",
    "date",
    "description",
    "photographer",
    "people",
    "search_terms",
    "credit_line",
    "url",
]

FIELD_MAP = {
    "title": ["title"],
    "object_id": ["object id", "objectid", "object_id"],
    "object_name": ["object name", "objectname"],
    "other_number": ["other number", "othernumber"],
    "place": ["place", "places"],
    "date": ["date", "event date", "year", "period", "created", "date year range"],
    "description": ["description", "abstract", "notes", "caption", "summary"],
    "photographer": ["photographer", "creator", "author", "studio", "artist"],
    "people": ["people", "persons", "person(s)"],
    "search_terms": ["search terms", "keywords", "tags"],
    "credit_line": ["credit line/ name of photo donor", "credit line", "name of photo donor", "credit"],
}


def norm_key(k: str) -> str:
    return re.sub(r"\s+", " ", (k or "")).strip().strip(":").lower()


def norm_text(x):
    if x is None:
        return None
    s = re.sub(r"\s+", " ", str(x)).strip()
    return s if s else None


def pick_from_kv(kv: Dict[str, str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        v = kv.get(c)
        if v:
            return v
    return None


def fetch_all_items(top: int, only_with_images: bool) -> List[dict]:
    all_items = []
    skip = 0
    total = None

    print("Fetching list of all IDs via search API...")
    while True:
        payload = {
            "OnlyWithImages": only_with_images,
            "SearchString": "",
            "Skip": str(skip),
            "Top": str(top),
        }
        resp = requests.post(SEARCH_URL, headers=HEADERS, json=payload, timeout=60)
        if resp.status_code != 200:
            print("Search error:", resp.status_code)
            break

        data = resp.json()
        items = data.get("PageResult", {}).get("Items", []) or []

        if total is None:
            total = data.get("RecordsSearched")
            print("Total records expected:", total)
            if items:
                print("DEBUG keys in first list item:", list(items[0].keys()))

        if not items:
            break

        for rec in items:
            _id = rec.get("Id")
            list_title = rec.get("Title") or rec.get("Name") or "Photo Record"
            url = f"{BASE}/photos/{_id}" if _id is not None else None
            all_items.append({"id": _id, "list_title": list_title, "url": url})

        skip += top

    print("Total items collected:", len(all_items))
    return all_items


def extract_kv_pairs(soup: BeautifulSoup) -> Dict[str, str]:
    kv: Dict[str, str] = {}

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) >= 2:
                k = norm_key(cells[0].get_text(" ", strip=True))
                v_lines = list(cells[1].stripped_strings)
                v = "\n".join(v_lines).strip() if v_lines else None
                if k and v and k not in kv:
                    kv[k] = v

    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            k = norm_key(dt.get_text(" ", strip=True))
            v_lines = list(dd.stripped_strings)
            v = "\n".join(v_lines).strip() if v_lines else None
            if k and v and k not in kv:
                kv[k] = v

    return kv


async def fetch_one_detail(
    context,
    item: dict,
    timeout_ms: int,
    max_retries: int,
    failed_log: str,
) -> Optional[dict]:
    url = item["url"]
    numeric_id = item["id"]

    for attempt in range(1, max_retries + 1):
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            try:
                await page.get_by_text("I agree", exact=False).click(timeout=2000)
            except PWTimeout:
                pass

            try:
                await page.wait_for_selector("table, dl", timeout=15000)
            except PWTimeout:
                pass

            await asyncio.sleep(0.6)
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            kv = extract_kv_pairs(soup)

            rec = {"id": str(numeric_id), "url": url.rstrip("/")}

            page_title = pick_from_kv(kv, [norm_key(x) for x in FIELD_MAP["title"]])
            rec["title"] = page_title or item.get("list_title") or "Photo Record"

            for out_field, keys in FIELD_MAP.items():
                if out_field == "title":
                    continue
                rec[out_field] = pick_from_kv(kv, [norm_key(k) for k in keys])

            for k in list(rec.keys()):
                if rec[k] is None:
                    continue
                if k in {"place", "people", "search_terms", "description"}:
                    lines = [ln.strip() for ln in str(rec[k]).splitlines()]
                    lines = [ln for ln in lines if ln != ""]
                    rec[k] = "\n".join(lines) if lines else None
                else:
                    rec[k] = norm_text(rec[k])

            return rec

        except Exception as e:
            if attempt == max_retries:
                os.makedirs(os.path.dirname(failed_log) or ".", exist_ok=True)
                with open(failed_log, "a", encoding="utf-8") as f:
                    f.write(f"{url}\t{repr(e)}\n")
                return None
            await asyncio.sleep(2 * attempt)
        finally:
            await page.close()

    return None


async def scrape_details(
    items: List[dict],
    concurrency: int,
    timeout_ms: int,
    max_retries: int,
    failed_log: str,
    limit: int = 0,
) -> List[dict]:
    sem = asyncio.Semaphore(concurrency)
    results: List[dict] = []

    if limit and limit > 0:
        items = items[:limit]

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(locale="en-US")
        await context.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})

        async def bound(it):
            async with sem:
                return await fetch_one_detail(
                    context=context,
                    item=it,
                    timeout_ms=timeout_ms,
                    max_retries=max_retries,
                    failed_log=failed_log,
                )

        tasks = [bound(it) for it in items if it.get("url")]
        for fut in asyncio.as_completed(tasks):
            r = await fut
            if r:
                results.append(r)

        await browser.close()

    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out_csv", default="data/projectsave_photos.csv")
    ap.add_argument("--out_jsonl", default="data/projectsave_photos.jsonl")
    ap.add_argument("--failed_log", default="data/failed_urls.txt")
    ap.add_argument("--top", type=int, default=200)
    ap.add_argument("--only_with_images", action="store_true")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--timeout_ms", type=int, default=90000)
    ap.add_argument("--max_retries", type=int, default=3)
    ap.add_argument("--limit", type=int, default=0, help="0 = all")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(args.out_jsonl) or ".", exist_ok=True)

    items = fetch_all_items(top=args.top, only_with_images=args.only_with_images)
    details = asyncio.run(
        scrape_details(
            items=items,
            concurrency=args.concurrency,
            timeout_ms=args.timeout_ms,
            max_retries=args.max_retries,
            failed_log=args.failed_log,
            limit=args.limit,
        )
    )

    df = pd.DataFrame(details)

    if "url" in df.columns:
        df["url"] = df["url"].astype(str).str.rstrip("/")
        df = df.drop_duplicates(subset=["url"], keep="first")

    df = df.reset_index(drop=True)

    with open(args.out_jsonl, "w", encoding="utf-8") as f:
        for r in df.to_dict(orient="records"):
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    cols = [c for c in DEFAULT_COLUMNS if c in df.columns]
    df.to_csv(args.out_csv, index=False, encoding="utf-8", columns=cols)

    print("Saved CSV:", args.out_csv)
    print("Saved JSONL:", args.out_jsonl)
    print("Failed log (if any):", args.failed_log)
    print("Rows:", len(df))


if __name__ == "__main__":
    main()