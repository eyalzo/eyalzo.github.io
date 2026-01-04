import json
import requests
import sys
from datetime import datetime, timedelta
from itertools import combinations
from collections import Counter

# --- הגדרות ---
API_KEY = "zqt_WQdPgppS5OLumkFHZXSffKRBLzOS1MmAq-c0HA"
CORPUS_ID = "mmm_docs5"
DATA_VERSION = "2.9"

# רשימת ראשי צוותים שלא ייספרו במונה המחברים
TEAM_LEADERS = [
    "ראש צוות 1",
    "ראש צוות 2"
]


def list_all_docs(api_key, corpus_key):
    url_list = f"https://api.vectara.io/v2/corpora/{corpus_key}/documents"
    headers = {'Accept': 'application/json', 'x-api-key': api_key}
    all_docs = []
    page_key = None
    limit = 100
    print("Starting corpus scan...")

    while True:
        params = {'limit': limit}
        if page_key: params['page_key'] = page_key

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
            if not page_key: break
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


def extract_author(doc_meta):
    val = doc_meta.get('ez_author') or doc_meta.get('author')
    if not val: return None
    if isinstance(val, list): return val[0].strip() if val else None
    s = str(val).strip()
    if s.startswith('[') and ']' in s:
        try:
            lst = json.loads(s.replace("'", '"'))
            if lst and isinstance(lst, list): return lst[0].strip()
        except:
            pass
    return s


def extract_title_and_url(doc, meta):
    title = meta.get('title') or meta.get('ez_title') or doc.get('id') or "ללא כותרת"
    url = meta.get('url') or meta.get('ez_url') or "#"
    return title, url


def process_topics(documents):
    topics_temp = {}
    pair_counts = Counter()
    all_system_authors = set()
    today = datetime.now()
    cutoff_date = today - timedelta(days=365)

    print("Processing data...")

    for doc in documents:
        meta = doc.get('metadata', {})
        kw_val = meta.get('ez_keywords')
        kw_list = []
        if isinstance(kw_val, list):
            kw_list = kw_val
        elif isinstance(kw_val, str):
            try:
                if kw_val.strip().startswith('['):
                    kw_list = json.loads(kw_val.replace("'", '"'))
                elif ',' in kw_val:
                    kw_list = kw_val.split(',')
                else:
                    kw_list = [kw_val]
            except:
                kw_list = [kw_val]

        doc_year = extract_year(meta)
        doc_date_str = meta.get('ez_date')
        doc_date_obj = parse_date(doc_date_str)
        doc_author = extract_author(meta)
        doc_title, doc_url = extract_title_and_url(doc, meta)

        if doc_author: all_system_authors.add(doc_author)

        # מידע מזוקק על המסמך
        current_doc_info = {
            "title": doc_title,
            "url": doc_url,
            "date": doc_date_str if doc_date_str else "",
            "author": doc_author if doc_author else ""
        }

        current_doc_clean_topics = set()
        for topic in kw_list:
            clean_topic = str(topic).strip()
            if len(clean_topic) < 2: continue
            current_doc_clean_topics.add(clean_topic)

            if clean_topic not in topics_temp:
                topics_temp[clean_topic] = {
                    "count": 0, "years": set(), "latest_date": None,
                    "authors": set(), "docs_list": []
                }

            topics_temp[clean_topic]["count"] += 1
            if doc_year: topics_temp[clean_topic]["years"].add(doc_year)
            if doc_author: topics_temp[clean_topic]["authors"].add(doc_author)
            topics_temp[clean_topic]["docs_list"].append(current_doc_info)

            curr_date = topics_temp[clean_topic]["latest_date"]
            if doc_date_obj:
                if curr_date is None or doc_date_obj > curr_date:
                    topics_temp[clean_topic]["latest_date"] = doc_date_obj

        if len(current_doc_clean_topics) > 1:
            sorted_kws = sorted(list(current_doc_clean_topics))
            for pair in combinations(sorted_kws, 2):
                pair_counts[pair] += 1

    final_list = []
    for name, data in topics_temp.items():
        count = data["count"]
        years_set = data["years"]
        latest_date = data["latest_date"]
        authors_set = data["authors"]
        docs_list = data["docs_list"]

        valid_authors = [a for a in authors_set if a not in TEAM_LEADERS]
        unique_authors_count = len(valid_authors)

        years_range_str = ""
        span_years = 1  # ברירת מחדל למיון
        if years_set:
            min_y = min(years_set)
            max_y = max(years_set)
            span_years = max_y - min_y + 1  # חישוב הטווח
            years_range_str = str(min_y) if min_y == max_y else f"{min_y}-{max_y}"

        avg_per_year = round(count / span_years, 1)
        is_active = True if (latest_date and latest_date >= cutoff_date) else False

        # מיון מסמכים בתוך הנושא לפי תאריך
        docs_list.sort(key=lambda x: x['date'] if x['date'] else "0000-00-00", reverse=True)

        final_list.append({
            "value": name,
            "count": count,
            "years_range": years_range_str,
            "span_years": span_years,  # שדה קריטי למיון החדש
            "avg_per_year": avg_per_year,
            "is_active": is_active,
            "authors": list(authors_set),
            "author_count": unique_authors_count,
            "documents": docs_list
        })

    sorted_topics = sorted(final_list, key=lambda x: x['count'], reverse=True)

    top_pairs = []
    for pair, joint_count in pair_counts.most_common(200):
        t1, t2 = pair[0], pair[1]
        t1_data = topics_temp.get(t1)
        t2_data = topics_temp.get(t2)
        if t1_data and t2_data:
            top_pairs.append({
                "topic1": t1, "topic2": t2, "count": joint_count,
                "topic1_total": t1_data['count'], "topic2_total": t2_data['count'],
                "combo": f"{t1} + {t2}"
            })

    sorted_all_authors = sorted(list(all_system_authors))
    return sorted_topics, top_pairs, sorted_all_authors


def main():
    docs = list_all_docs(API_KEY, CORPUS_ID)
    topics_data, pairs_data, all_authors = process_topics(docs)
    final_json = {
        "updated_at": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "data_version": DATA_VERSION,
        "total_docs": len(docs),
        "all_authors": all_authors,
        "topics": topics_data,
        "pairs": pairs_data
    }
    with open("topics_data.json", "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, indent=2)
    print("Done. Saved to topics_data.json")


if __name__ == "__main__":
    main()