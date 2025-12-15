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
    """ מנסה להמיר מחרוזת תאריך לאובייקט datetime """
    if not date_str:
        return None
    try:
        # פורמט סטנדרטי YYYY-MM-DD
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None


def extract_year(doc_meta):
    """ חילוץ שנה כמספר """
    if doc_meta.get('ez_year'):
        return int(doc_meta.get('ez_year'))

    date_str = doc_meta.get('ez_date')
    if date_str and len(date_str) >= 4:
        try:
            return int(date_str[:4])
        except:
            pass
    return None


def process_authors(documents):
    """
    עיבוד נתונים: ספירה, טווח שנים, זיהוי פעילות וממוצע
    """
    # מבנה זמני: { "Name": { count: 0, years: set(), latest_date: datetime } }
    authors_temp = {}

    today = datetime.now()
    cutoff_date = today - timedelta(days=365)  # שנה אחורה מהיום

    print("Processing authors stats...")

    for doc in documents:
        meta = doc.get('metadata', {})

        # חילוץ רשימת שמות
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

        # נתונים למסמך
        doc_year = extract_year(meta)
        doc_date_str = meta.get('ez_date')
        doc_date_obj = parse_date(doc_date_str)

        # עדכון לכל מחבר
        for person in people_list:
            clean_name = str(person).strip()
            if clean_name:
                if clean_name not in authors_temp:
                    authors_temp[clean_name] = {
                        "count": 0,
                        "years": set(),
                        "latest_date": None
                    }

                # עדכון ספירה
                authors_temp[clean_name]["count"] += 1

                # עדכון שנים
                if doc_year:
                    authors_temp[clean_name]["years"].add(doc_year)

                # עדכון תאריך אחרון (לצורך בדיקת פעילות)
                current_latest = authors_temp[clean_name]["latest_date"]
                if doc_date_obj:
                    if current_latest is None or doc_date_obj > current_latest:
                        authors_temp[clean_name]["latest_date"] = doc_date_obj

    # בניית הרשימה הסופית
    final_list = []

    for name, data in authors_temp.items():
        count = data["count"]
        years_set = data["years"]
        latest_date = data["latest_date"]

        # 1. חישוב טווח שנים
        years_range_str = ""
        span_years = 1

        if years_set:
            min_y = min(years_set)
            max_y = max(years_set)
            span_years = max_y - min_y + 1  # למשל 2020-2020 זה שנה אחת
            if min_y == max_y:
                years_range_str = str(min_y)
            else:
                years_range_str = f"{min_y}-{max_y}"

        # 2. חישוב ממוצע שנתי
        # אם אין שנים ידועות, נניח טווח של 1 כדי לא לחלק ב-0
        if not years_set:
            span_years = 1

        avg_per_year = round(count / span_years, 1)

        # 3. בדיקת פעילות (האם פרסם בשנה האחרונה לפי תאריך מדויק)
        is_active = False
        if latest_date and latest_date >= cutoff_date:
            is_active = True

        final_list.append({
            "value": name,
            "count": count,
            "years_range": years_range_str,
            "avg_per_year": avg_per_year,
            "is_active": is_active
        })

    # מיון לפי שם
    sorted_authors = sorted(final_list, key=lambda x: x['value'])
    return sorted_authors


def main():
    docs = list_all_docs(API_KEY, CORPUS_ID)
    print(f"Total documents found: {len(docs)}")

    authors_data = process_authors(docs)
    print(f"Total unique authors found: {len(authors_data)}")

    final_json = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_docs": len(docs),
        "authors": authors_data
    }

    filename = "authors_data.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, indent=2)

    print(f"Successfully saved data to {filename}")


if __name__ == "__main__":
    main()