import json
import requests
import argparse
import sys
from datetime import datetime

# הגדרות (וודא שה-API Key שלך מעודכן כאן)
API_KEY = "zqt_WQdPgppS5OLumkFHZXSffKRBLzOS1MmAq-c0HA"
CORPUS_ID = "mmm_docs5"


def list_all_docs(api_key, corpus_key):
    """
    שולף את כל המסמכים מהקורפוס
    """
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


def extract_year(doc_meta):
    """
    מנסה לחלץ שנה מתוך המטא-דאטה (ez_year או ez_date)
    """
    # נסיון 1: שדה שנה מפורש
    if doc_meta.get('ez_year'):
        return int(doc_meta.get('ez_year'))

    # נסיון 2: חילוץ מתאריך מלא (YYYY-MM-DD)
    date_str = doc_meta.get('ez_date')
    if date_str and len(date_str) >= 4:
        try:
            return int(date_str[:4])
        except:
            pass

    return None


def process_authors(documents):
    """
    מעבד את המסמכים, סופר מחברים ומחשב טווחי שנים
    """
    # מבנה זמני: { "AuthorName": { "count": 0, "years": {2020, 2021, ...} } }
    authors_temp = {}

    print("Processing authors and years...")
    for doc in documents:
        meta = doc.get('metadata', {})

        # חילוץ רשימת אנשים
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

        # חילוץ שנה למסמך הנוכחי
        doc_year = extract_year(meta)

        # עדכון הנתונים לכל מחבר במסמך
        for person in people_list:
            clean_name = str(person).strip()
            if clean_name:
                if clean_name not in authors_temp:
                    authors_temp[clean_name] = {"count": 0, "years": set()}

                authors_temp[clean_name]["count"] += 1
                if doc_year:
                    authors_temp[clean_name]["years"].add(doc_year)

    # המרה למבנה הסופי עבור ה-JSON
    final_list = []

    for name, data in authors_temp.items():
        years_set = data["years"]
        years_range_str = ""

        if years_set:
            min_y = min(years_set)
            max_y = max(years_set)
            if min_y == max_y:
                years_range_str = str(min_y)
            else:
                years_range_str = f"{min_y}-{max_y}"

        final_list.append({
            "value": name,
            "count": data["count"],
            "years_range": years_range_str  # שדה חדש!
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

    print(f"Successfully saved data (with years) to {filename}")


if __name__ == "__main__":
    main()