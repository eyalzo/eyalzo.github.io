import json
import requests
import argparse
import sys
from datetime import datetime, timedelta

# הגדרות
API_KEY = "zqt_WQdPgppS5OLumkFHZXSffKRBLzOS1MmAq-c0HA"
CORPUS_ID = "mmm_docs5"
DATA_VERSION = "2.0"  # גרסת הנתונים הנוכחי


def list_all_docs(api_key, corpus_key):
    url_list = f"https://api.vectara.io/v2/corpora/{corpus_key}/documents"
    headers = {'Accept': 'application/json', 'x-api-key': api_key}

    all_docs = []
    page_key = None
    limit = 100

    print("Starting corpus scan...")

    while True:
        params = {'limit': limit}
        if page_key:
            params['page_key'] = page_key

        try:
            response = requests.get(url_list, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}")
                break

            data = response.json()
            docs = data.get('documents', [])
            metadata = data.get('metadata', {})

            all_docs.extend(docs)
            print(f"Scanned {len(all_docs)} documents so far...")

            page_key = metadata.get('page_key')
            if not page_key:
                break

        except Exception as e:
            print(f"Exception: {e}")
            break

    return all_docs


def parse_date(date_str):
    if not date_str: return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None


def extract_year(doc_meta):
    if doc_meta.get('ez_year'): return int(doc_meta.get('ez_year'))
    date_str = doc_meta.get('ez_date')
    if date_str and len(date_str) >= 4:
        try:
            return int(date_str[:4])
        except:
            pass
    return None


def process_authors(documents):
    authors_temp = {}
    today = datetime.now()
    cutoff_date = today - timedelta(days=365)

    print("Processing authors stats (weighted)...")

    for doc in documents:
        meta = doc.get('metadata', {})
        people_val = meta.get('ez_people')
        people_list = []
        if isinstance(people_val, list):
            people_list = people_val
        elif isinstance(people_val, str):
            try:
                if people_val.startswith('['):
                    people_list = json.loads(people_val.replace("'", '"'))
                else:
                    people_list = [people_val]
            except:
                people_list = [people_val]

        people_list = [str(p).strip() for p in people_list if str(p).strip()]
        num_authors = len(people_list)
        if num_authors == 0: continue

        weight_per_person = 1.0 / num_authors
        doc_year = extract_year(meta)
        doc_date_str = meta.get('ez_date')
        doc_date_obj = parse_date(doc_date_str)

        for person in people_list:
            if person not in authors_temp:
                authors_temp[person] = {
                    "count": 0, "weighted_count": 0.0,
                    "years": set(), "latest_date": None
                }

            authors_temp[person]["count"] += 1
            authors_temp[person]["weighted_count"] += weight_per_person
            if doc_year: authors_temp[person]["years"].add(doc_year)

            curr_date = authors_temp[person]["latest_date"]
            if doc_date_obj:
                if curr_date is None or doc_date_obj > curr_date:
                    authors_temp[person]["latest_date"] = doc_date_obj

    final_list = []
    for name, data in authors_temp.items():
        count = data["count"]
        weighted_count = round(data["weighted_count"], 1)
        years_set = data["years"]
        latest_date = data["latest_date"]

        years_range_str = ""
        span_years = 1
        if years_set:
            min_y = min(years_set)
            max_y = max(years_set)
            span_years = max_y - min_y + 1
            if min_y == max_y:
                years_range_str = str(min_y)
            else:
                years_range_str = f"{min_y}-{max_y}"

        if not years_set: span_years = 1
        avg_per_year = round(weighted_count / span_years, 1)

        is_active = False
        if latest_date and latest_date >= cutoff_date:
            is_active = True

        final_list.append({
            "value": name,
            "count": count,
            "weighted_count": weighted_count,
            "years_range": years_range_str,
            "avg_per_year": avg_per_year,
            "is_active": is_active
        })

    sorted_authors = sorted(final_list, key=lambda x: x['value'])
    return sorted_authors


def main():
    docs = list_all_docs(API_KEY, CORPUS_ID)
    print(f"Total documents found: {len(docs)}")
    authors_data = process_authors(docs)

    final_json = {
        "updated_at": datetime.now().strftime("%d/%m/%Y %H:%M"),  # פורמט ישראלי
        "data_version": DATA_VERSION,
        "total_docs": len(docs),
        "authors": authors_data
    }

    with open("authors_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, indent=2)

    print("Done. authors_data.json updated.")


if __name__ == "__main__":
    main()