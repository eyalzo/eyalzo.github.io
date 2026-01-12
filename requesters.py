import pandas as pd
import glob
import argparse
import csv
import json
from datetime import datetime

# הגדרות גרסה
VERSION = "2.1.0"


def get_latest_input_file():
    """מחזיר את קובץ ה-CSV העדכני ביותר בתיקייה"""
    files = glob.glob("scrape_docs_*.csv")
    return sorted(files)[-1] if files else None


def parse_csv_list(text):
    """משמש רק עבור רשימת המחברים (additional_authors) שעדיין קיימת"""
    if pd.isna(text) or not str(text).strip():
        return []
    reader = csv.reader([str(text)], skipinitialspace=True)
    try:
        items = next(reader)
        return [x.strip() for x in items if x.strip()]
    except StopIteration:
        return []


def classify_requester(name):
    """סיווג המבקש"""
    if not name: return "חברי כנסת ואחרים"
    if "מרכז המחקר והמידע" in name:
        return "מרכז המחקר והמידע"
    if "הייעוץ המשפטי לכנסת" in name:
        return "חברי כנסת ואחרים"

    # כיוון שהשם כבר מנורמל ("ועדת X" ולא "הייעוץ ל..."), הבדיקה פשוטה יותר
    if name.startswith("הוועדה") or name.startswith("ועדת"):
        return "ועדות"
    return "חברי כנסת ואחרים"


def get_sorted_unique_list(list_of_lists):
    unique_set = set()
    for sublist in list_of_lists:
        unique_set.update(sublist)
    return sorted([x for x in unique_set if x])


def create_requesters_dashboard(df_exploded, years_data, unique_counts, details_map, output_name):
    creation_time = datetime.now().strftime("%d-%b-%Y %H:%M")

    chart_years = sorted(list(years_data.keys()))

    dataset_mmm = []
    dataset_committees = []
    dataset_others = []

    for year in chart_years:
        total = years_data[year]['total']
        if total > 0:
            dataset_mmm.append(round((years_data[year]['mmm'] / total) * 100, 1))
            dataset_committees.append(round((years_data[year]['committees'] / total) * 100, 1))
            dataset_others.append(round((years_data[year]['others'] / total) * 100, 1))
        else:
            dataset_mmm.append(0)
            dataset_committees.append(0)
            dataset_others.append(0)

    # --- חישוב סטטיסטיקות לטבלה הראשית ---
    stats = df_exploded.groupby(['requester_name', 'requester_type']).agg(
        doc_count=('year', 'size'),
        min_year=('year', 'min'),
        max_year=('year', 'max'),
        active_years=('year', 'nunique'),
        all_authors=('authors', lambda x: list(x)),
        all_teamleaders=('teamleaders', lambda x: list(x))
    ).reset_index()

    stats['avg_per_year'] = stats['doc_count'] / stats['active_years']
    stats['unique_authors_list'] = stats['all_authors'].apply(get_sorted_unique_list)
    stats['unique_teamleaders_list'] = stats['all_teamleaders'].apply(get_sorted_unique_list)
    stats['unique_authors_count'] = stats['unique_authors_list'].apply(len)
    stats['unique_teamleaders_count'] = stats['unique_teamleaders_list'].apply(len)

    stats['avg_per_author'] = stats.apply(
        lambda r: r['doc_count'] / r['unique_authors_count'] if r['unique_authors_count'] > 0 else 0, axis=1
    )

    stats = stats.sort_values(by='doc_count', ascending=False)

    html_table_rows = ""
    for _, row in stats.iterrows():
        type_badge_class = "bg-secondary"
        if row['requester_type'] == "מרכז המחקר והמידע":
            type_badge_class = "bg-primary"
        elif row['requester_type'] == "ועדות":
            type_badge_class = "bg-warning text-dark"
        elif row['requester_type'] == "חברי כנסת ואחרים":
            type_badge_class = "bg-success"

        avg_year_str = f"{row['avg_per_year']:.1f}"
        avg_auth_str = f"{row['avg_per_author']:.1f}"

        authors_html = str(row['unique_authors_count'])
        if row['unique_authors_count'] > 0:
            authors_list_str = ", ".join(row['unique_authors_list']).replace('"', '&quot;')
            authors_html = f'''<span class="interactive-count" data-bs-toggle="popover" title="מחברים ({row['unique_authors_count']})" data-bs-content="{authors_list_str}">{row['unique_authors_count']}</span>'''

        leaders_html = str(row['unique_teamleaders_count'])
        if row['unique_teamleaders_count'] > 0:
            leaders_list_str = ", ".join(row['unique_teamleaders_list']).replace('"', '&quot;')
            leaders_html = f'''<span class="interactive-count" data-bs-toggle="popover" title="ראשי צוותים ({row['unique_teamleaders_count']})" data-bs-content="{leaders_list_str}">{row['unique_teamleaders_count']}</span>'''

        # שם המבקש כקישור למודל
        safe_name = row['requester_name'].replace("'", "\\'")
        requester_link = f'<span class="clickable-name" onclick="openRequesterModal(\'{safe_name}\')">{row["requester_name"]}</span>'

        html_table_rows += f"""
        <tr>
            <td class="fw-bold">{requester_link}</td>
            <td><span class="badge {type_badge_class}">{row['requester_type']}</span></td>
            <td class="text-center bg-light fw-bold">{row['doc_count']}</td>
            <td class="text-center">{row['min_year']}</td>
            <td class="text-center">{row['max_year']}</td>
            <td class="text-center">{row['active_years']}</td>
            <td class="text-center">{avg_year_str}</td>
            <td class="text-center">{authors_html}</td>
            <td class="text-center">{avg_auth_str}</td>
            <td class="text-center">{leaders_html}</td>
        </tr>"""

    hebrew_i18n = """{
        "sProcessing": "מעבד...", "sLengthMenu": "הצג _MENU_ פריטים", "sZeroRecords": "לא נמצאו רשומות",
        "sSearch": "חיפוש:", "oPaginate": { "sFirst": "ראשון", "sPrevious": "קודם", "sNext": "הבא", "sLast": "אחרון" }
    }"""

    html_template = f"""
    <!DOCTYPE html>
    <html lang="he" dir="rtl">
    <head>
        <meta charset="UTF-8">
        <title>Gilat AI - ניתוח מבקשים</title>
        <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.0/css/bootstrap.rtl.min.css">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{ background-color: #f0f2f5; font-family: 'Segoe UI', Tahoma, sans-serif; padding: 15px; }}
            .main-card {{ background: white; padding: 25px; border-radius: 12px; box-shadow: 0 5px 20px rgba(0,0,0,0.05); margin-bottom: 20px; }}

            .header-row {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 2px solid #f0f2f5; padding-bottom: 10px; }}
            .header-right {{ display: flex; align-items: center; gap: 15px; }}
            .header-right h2 {{ color: #1a73e8; font-weight: 800; margin: 0; padding-right: 12px; border-right: 5px solid #1a73e8; }}
            .header-left-meta {{ color: #9aa0a6; font-size: 0.75rem; font-family: monospace; }}

            .nav-btn {{ text-decoration: none; background: #fff; color: #1a73e8; border: 1px solid #1a73e8; padding: 5px 15px; border-radius: 20px; font-weight: bold; font-size: 0.85rem; transition: 0.2s; cursor: pointer; }}
            .nav-btn:hover {{ background: #1a73e8; color: white; }}

            .chart-container {{ position: relative; height: 400px; width: 100%; margin-bottom: 30px; }}
            h4 {{ color: #5f6368; font-weight: 700; margin-bottom: 15px; font-size: 1.1rem; }}

            .custom-legend {{ display: flex; gap: 20px; justify-content: center; margin-bottom: 15px; flex-wrap: wrap; }}
            .legend-item {{ 
                display: flex; align-items: center; gap: 8px; cursor: pointer; 
                padding: 5px 12px; border-radius: 20px; border: 1px solid transparent; transition: 0.2s; background: #f8f9fa;
            }}
            .legend-item:hover {{ background: #e9ecef; border-color: #dee2e6; transform: translateY(-2px); }}
            .legend-color {{ width: 12px; height: 12px; border-radius: 50%; display: inline-block; }}
            .legend-count {{ font-weight: bold; color: #555; margin-right: 4px; }}
            .active-filter {{ border-color: #000; background: #e2e6ea; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}

            table.dataTable thead th {{ background-color: #f8f9fa; border-bottom: 2px solid #dee2e6; font-size: 0.85rem; }}
            table.dataTable tbody td {{ font-size: 0.9rem; vertical-align: middle; }}

            .interactive-count {{ cursor: help; text-decoration: underline; text-decoration-style: dotted; color: #0d6efd; font-weight: bold; }}
            .interactive-count:hover {{ color: #0a58ca; }}

            .clickable-name {{ color: #0d6efd; cursor: pointer; font-weight: 700; }}
            .clickable-name:hover {{ text-decoration: underline; color: #0a58ca; }}
        </style>
    </head>
    <body>
        <div class="main-card">
            <div class="header-row">
                <div class="header-right">
                    <h2>📊 ניתוח מבקשי מחקרים</h2>
                    <a href="analyze_docs.html" class="nav-btn">🔙 חזרה לראשי</a>
                </div>
                <div class="header-left-meta">
                    Gilat AI Requesters v{VERSION} {creation_time}
                </div>
            </div>

            <div class="row">
                <div class="col-12">
                    <h4>מגמות סוגי מבקשים לאורך השנים (באחוזים)</h4>
                    <div class="custom-legend">
                        <div class="legend-item" onclick="filterTable('מרכז המחקר והמידע', this)">
                            <span class="legend-color" style="background: #0d6efd;"></span><span>מרכז המחקר והמידע</span><span class="legend-count">({unique_counts.get('מרכז המחקר והמידע', 0)})</span>
                        </div>
                        <div class="legend-item" onclick="filterTable('ועדות', this)">
                            <span class="legend-color" style="background: #ffc107;"></span><span>ועדות</span><span class="legend-count">({unique_counts.get('ועדות', 0)})</span>
                        </div>
                        <div class="legend-item" onclick="filterTable('חברי כנסת ואחרים', this)">
                            <span class="legend-color" style="background: #198754;"></span><span>חברי כנסת ואחרים</span><span class="legend-count">({unique_counts.get('חברי כנסת ואחרים', 0)})</span>
                        </div>
                        <div class="legend-item" onclick="resetTableFilter(this)"><span>🔄 הצג הכל</span></div>
                    </div>
                    <div class="chart-container"><canvas id="requestersChart"></canvas></div>
                </div>
            </div>
        </div>

        <div class="main-card">
            <h4>פירוט וסטטיסטיקה למבקשים</h4>
            <div class="alert alert-info py-1 px-3 mb-3 d-inline-block" style="font-size: 0.9rem;">
                💡 טיפ: לחץ על שם המבקש לפרטים מלאים, ורחף מעל המונים למידע נוסף.
            </div>
            <table id="requestersTable" class="display table table-sm table-hover" style="width:100%">
                <thead>
                    <tr>
                        <th style="width: 20%">שם המבקש</th>
                        <th style="width: 15%">סיווג</th>
                        <th class="text-center">מסמכים</th>
                        <th class="text-center">משנה</th>
                        <th class="text-center">עד שנה</th>
                        <th class="text-center">שנות פעילות</th>
                        <th class="text-center">ממוצע לשנה</th>
                        <th class="text-center">מחברים</th>
                        <th class="text-center">ממוצע למחבר</th>
                        <th class="text-center">ראשי צוותים</th>
                    </tr>
                </thead>
                <tbody>{html_table_rows}</tbody>
            </table>
        </div>

        <div class="modal fade" id="singleRequesterModal" tabindex="-1" aria-hidden="true">
          <div class="modal-dialog modal-xl modal-dialog-scrollable">
            <div class="modal-content">
              <div class="modal-header">
                <h5 class="modal-title fw-bold" id="requesterModalTitle">פרטי מבקש</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
              </div>
              <div class="modal-body">
                <div class="row">
                    <div class="col-12 mb-4" style="height: 300px;">
                        <canvas id="requesterSpecificChart"></canvas>
                    </div>
                    <div class="col-12">
                        <h5>📜 רשימת מסמכים</h5>
                        <table id="requesterDocsTable" class="table table-sm table-striped table-hover" style="width:100%">
                            <thead><tr><th>תאריך</th><th>כותרת המסמך</th><th>קישור</th></tr></thead>
                            <tbody></tbody>
                        </table>
                    </div>
                </div>
              </div>
              <div class="modal-footer"><button type="button" class="btn btn-secondary" data-bs-dismiss="modal">סגור</button></div>
            </div>
          </div>
        </div>

        <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.0/js/bootstrap.bundle.min.js"></script>
        <script>
            // נתונים מלאים המוזרקים מהפייתון
            const requesterDetails = {json.dumps(details_map)};
            let dataTable;
            let detailedChart = null; // משתנה גלובלי לגרף הספציפי

            function filterTable(type, element) {{
                $('.legend-item').removeClass('active-filter');
                $(element).addClass('active-filter');
                if (dataTable) {{ dataTable.column(1).search(type).draw(); }}
            }}

            function resetTableFilter(element) {{
                $('.legend-item').removeClass('active-filter');
                if (dataTable) {{ dataTable.column(1).search('').draw(); }}
            }}

            // פונקציה לפתיחת המודל הספציפי
            function openRequesterModal(name) {{
                const data = requesterDetails[name];
                if (!data) return;

                // עדכון כותרת
                $('#requesterModalTitle').text('תיק מבקש: ' + name);

                // 1. עדכון הגרף
                const ctx = document.getElementById('requesterSpecificChart').getContext('2d');
                if (detailedChart) {{ detailedChart.destroy(); }}

                const years = Object.keys(data.years).sort();
                const counts = years.map(y => data.years[y]);

                detailedChart = new Chart(ctx, {{
                    type: 'bar',
                    data: {{
                        labels: years,
                        datasets: [{{
                            label: 'מספר מסמכים',
                            data: counts,
                            backgroundColor: '#0d6efd'
                        }}]
                    }},
                    options: {{
                        responsive: true, maintainAspectRatio: false,
                        scales: {{ y: {{ beginAtZero: true, ticks: {{ stepSize: 1 }} }} }}
                    }}
                }});

                // 2. עדכון הטבלה
                if ($.fn.DataTable.isDataTable('#requesterDocsTable')) {{
                    $('#requesterDocsTable').DataTable().destroy();
                }}

                const tbody = $('#requesterDocsTable tbody');
                tbody.empty();
                data.docs.forEach(doc => {{
                    const linkHtml = doc.link ? `<a href="${{doc.link}}" target="_blank">🔗 צפייה</a>` : '-';
                    tbody.append(`<tr><td>${{doc.date}}</td><td>${{doc.title}}</td><td>${{linkHtml}}</td></tr>`);
                }});

                $('#requesterDocsTable').DataTable({{
                    "language": {hebrew_i18n},
                    "order": [[ 0, "desc" ]],
                    "pageLength": 10
                }});

                const modal = new bootstrap.Modal(document.getElementById('singleRequesterModal'));
                modal.show();
            }}

            $(document).ready(function() {{
                $('[data-bs-toggle="popover"]').popover({{ trigger: 'hover', placement: 'auto', html: true }});

                dataTable = $('#requestersTable').DataTable({{
                    "language": {hebrew_i18n}, "order": [[ 2, "desc" ]], "pageLength": 15
                }});

                dataTable.on('draw', function () {{
                    $('[data-bs-toggle="popover"]').popover({{ trigger: 'hover', placement: 'left', html: true }});
                }});

                // הגרף הראשי
                const ctx = document.getElementById('requestersChart').getContext('2d');
                new Chart(ctx, {{
                    type: 'line',
                    data: {{
                        labels: {json.dumps(chart_years)},
                        datasets: [
                            {{ label: 'מרכז המחקר והמידע', data: {json.dumps(dataset_mmm)}, borderColor: '#0d6efd', backgroundColor: '#0d6efd', tension: 0.3, fill: false }},
                            {{ label: 'ועדות', data: {json.dumps(dataset_committees)}, borderColor: '#ffc107', backgroundColor: '#ffc107', tension: 0.3, fill: false }},
                            {{ label: 'חברי כנסת ואחרים', data: {json.dumps(dataset_others)}, borderColor: '#198754', backgroundColor: '#198754', tension: 0.3, fill: false }}
                        ]
                    }},
                    options: {{
                        responsive: true, maintainAspectRatio: false, interaction: {{ mode: 'index', intersect: false }},
                        plugins: {{ legend: {{ display: false }}, tooltip: {{ callbacks: {{ label: function(context) {{ return context.dataset.label + ': ' + context.parsed.y + '%'; }} }} }} }},
                        scales: {{ x: {{ stacked: false }}, y: {{ stacked: false, beginAtZero: true, max: 100, ticks: {{ callback: function(value) {{ return value + "%" }} }} }} }}
                    }}
                }});
            }});
        </script>
    </body>
    </html>
    """

    with open(output_name, 'w', encoding='utf-8') as f:
        f.write(html_template)
    print(f"✅ קובץ הניתוח נוצר בהצלחה: {output_name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    args = parser.parse_args()

    input_file = args.input if args.input else get_latest_input_file()
    if not input_file: return print("❌ שגיאה: לא נמצא קובץ CSV.")

    print(f"קורא נתונים מקובץ: {input_file}...")
    try:
        df = pd.read_csv(input_file, encoding='utf-8')
    except:
        df = pd.read_csv(input_file, encoding='cp1255')

    df['date_dt'] = pd.to_datetime(df['date'], errors='coerce')
    df['year'] = df['date_dt'].dt.year

    # חישוב תאריכים לסטטיסטיקה
    min_date = df['date_dt'].min().strftime('%d/%m/%Y') if not df['date_dt'].isnull().all() else "N/A"
    max_date = df['date_dt'].max().strftime('%d/%m/%Y') if not df['date_dt'].isnull().all() else "N/A"

    flattened_data = []

    # מונה לשורות ללא מבקש
    missing_requester_count = 0

    for _, row in df.iterrows():
        year = row['year']
        if pd.isna(year): continue
        year = int(year)

        # איסוף מחברים (שדה שעדיין מכיל רשימה ב-CSV)
        doc_authors_raw = [str(row.get('author', '')).strip()]
        doc_authors_raw.extend([x for x in parse_csv_list(row.get('additional_authors', ''))])
        doc_authors = [a for a in doc_authors_raw if a and a.lower() != 'nan']

        # איסוף ראש צוות
        leader = str(row.get('teamleader', '')).strip()
        doc_leaders = [leader] if leader and leader.lower() != 'nan' else []

        # קריאת שם המבקש (השדה המנורמל והיחיד כעת)
        req_name = str(row.get('requested_by_normalized', '')).strip()

        # דילוג וספירה אם אין שם מבקש
        if not req_name or req_name.lower() == 'nan':
            missing_requester_count += 1
            continue

        req_type = classify_requester(req_name)

        # הוספה לרשימה (שורה אחת לכל מסמך)
        flattened_data.append({
            'year': year,
            'date': str(row['date']),
            'title': str(row['title']).replace('"', '&quot;'),
            'link': str(row['link']),
            'requester_name': req_name,
            'requester_type': req_type,
            'authors': doc_authors,
            'teamleaders': doc_leaders
        })

    df_exploded = pd.DataFrame(flattened_data)

    if df_exploded.empty:
        print("לא נמצאו נתוני מבקשים תקינים.")
        # גם אם לא מצאנו כלום, עדיין כדאי להדפיס סטטיסטיקה
        print(f"\n--- סטטיסטיקה ---")
        print(f"סה\"כ שורות שנקראו: {len(df)}")
        print(f"שורות תקינות (עם מבקש): 0")
        print(f"שורות פגומות (ללא מבקש): {missing_requester_count}")
        print(f"טווח תאריכים: {min_date} - {max_date}")
        return

    # הכנת המילון המלא עבור ה-Frontend
    details_map = {}
    for _, row in df_exploded.iterrows():
        name = row['requester_name']
        if name not in details_map:
            details_map[name] = {'years': {}, 'docs': []}

        y = int(row['year'])
        details_map[name]['years'][y] = details_map[name]['years'].get(y, 0) + 1

        doc_entry = {
            'date': row['date'],
            'title': row['title'],
            'link': row['link']
        }
        if doc_entry not in details_map[name]['docs']:
            details_map[name]['docs'].append(doc_entry)

    unique_counts = df_exploded.groupby('requester_type')['requester_name'].nunique().to_dict()

    years_agg = df_exploded.groupby(['year', 'requester_type']).size().unstack(fill_value=0)
    for col in ["מרכז המחקר והמידע", "ועדות", "חברי כנסת ואחרים"]:
        if col not in years_agg.columns:
            years_agg[col] = 0

    years_data = {}
    for year, row in years_agg.iterrows():
        mmm_count = int(row["מרכז המחקר והמידע"])
        comm_count = int(row["ועדות"])
        others_count = int(row["חברי כנסת ואחרים"])
        total = mmm_count + comm_count + others_count
        years_data[int(year)] = {
            'mmm': mmm_count,
            'committees': comm_count,
            'others': others_count,
            'total': total
        }

    create_requesters_dashboard(
        df_exploded,
        years_data,
        unique_counts,
        details_map,
        "requesters.html"
    )

    # הדפסת הסטטיסטיקה בסוף הריצה
    print(f"\n--- סטטיסטיקה ---")
    print(f"סה\"כ שורות שנקראו: {len(df)}")
    print(f"שורות תקינות (עם מבקש): {len(flattened_data)}")
    print(f"שורות פגומות (ללא מבקש): {missing_requester_count}")
    print(f"טווח תאריכים: {min_date} - {max_date}")


if __name__ == "__main__":
    main()