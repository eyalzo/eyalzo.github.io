import json
import requests
import argparse
import sys
from datetime import datetime, timedelta

# הגדרות
API_KEY = "zqt_WQdPgppS5OLumkFHZXSffKRBLzOS1MmAq-c0HA"
CORPUS_ID = "mmm_docs5"


def list_all_docs(api_key, corpus_key):
    """ שולף את כל המסמכים מהקורפוס """
    url_list = f"https://api.vectara.io/v2/corpora/{corpus_key}/documents"
    headers = {'Accept': 'application/json', 'x-api-key': api_key}

    all_docs = []
    page_key = None
    limit = 100

    print("Starting corpus scan for topics...")

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


def process_topics(documents):
    """
    עיבוד מילות מפתח
    """
    topics_temp = {}

    today = datetime.now()
    cutoff_date = today - timedelta(days=365)

    print("Processing keywords...")

    for doc in documents:
        meta = doc.get('metadata', {})

        # חילוץ מילות מפתח (ez_keywords)
        kw_val = meta.get('ez_keywords')
        kw_list = []

        if isinstance(kw_val, list):
            kw_list = kw_val
        elif isinstance(kw_val, str):
            try:
                # לפעמים זה מגיע כמחרוזת JSON ולפעמים מופרד פסיקים
                if kw_val.strip().startswith('['):
                    kw_list = json.loads(kw_val.replace("'", '"'))
                elif ',' in kw_val:
                    kw_list = kw_val.split(',')
                else:
                    kw_list = [kw_val]
            except:
                kw_list = [kw_val]

        # נתונים למסמך
        doc_year = extract_year(meta)
        doc_date_str = meta.get('ez_date')
        doc_date_obj = parse_date(doc_date_str)

        for topic in kw_list:
            clean_topic = str(topic).strip()
            # סינון רעשים: מילים קצרות מדי או ריקות
            if len(clean_topic) < 2:
                continue

            if clean_topic not in topics_temp:
                topics_temp[clean_topic] = {
                    "count": 0,
                    "years": set(),
                    "latest_date": None
                }

            # עדכונים
            topics_temp[clean_topic]["count"] += 1

            if doc_year:
                topics_temp[clean_topic]["years"].add(doc_year)

            curr_date = topics_temp[clean_topic]["latest_date"]
            if doc_date_obj:
                if curr_date is None or doc_date_obj > curr_date:
                    topics_temp[clean_topic]["latest_date"] = doc_date_obj

    # בניית הרשימה הסופית
    final_list = []

    for name, data in topics_temp.items():
        count = data["count"]
        years_set = data["years"]
        latest_date = data["latest_date"]

        # טווח שנים
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

        # חישוב ממוצע שנתי
        if not years_set: span_years = 1
        avg_per_year = round(count / span_years, 1)

        is_active = False
        if latest_date and latest_date >= cutoff_date:
            is_active = True

        final_list.append({
            "value": name,
            "count": count,
            "years_range": years_range_str,
            "span_years": span_years,  # נשמור לצורך מיון
            "avg_per_year": avg_per_year,
            "is_active": is_active
        })

    # מיון ברירת מחדל: לפי כמות (הכי נפוץ ראשון)
    sorted_topics = sorted(final_list, key=lambda x: x['count'], reverse=True)
    return sorted_topics


def main():
    docs = list_all_docs(API_KEY, CORPUS_ID)
    print(f"Total documents found: {len(docs)}")

    topics_data = process_topics(docs)
    print(f"Total unique topics found: {len(topics_data)}")

    final_json = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_docs": len(docs),
        "topics": topics_data
    }

    filename = "topics_data.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, indent=2)

    print(f"Done. Saved to {filename}")


if __name__ == "__main__":
    main()