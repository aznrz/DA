import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

# =======================
# ⚙️ CONFIG
# =======================

KEYWORDS = ['Data Analyst', 'Аналитик данных', 'BI аналитик', 'System Analyst', 'Аналитик']
EXCLUDE_WORDS = ['маркетолог', 'бухгалтер', 'водитель']
AREA = 40
PER_PAGE = 100
MAX_PAGES = 20
DAYS_BACK = 30

DATA_PATH = 'data/hh_kz_clean_results.csv'

HEADERS = {'User-Agent': 'HH-Analytics-App/1.0'}

# =======================
# 🔌 API FETCH
# =======================

def fetch_hh_data(query):
    url = 'https://api.hh.ru/vacancies'
    all_items = []

    for page in range(MAX_PAGES):
        params = {
            'text': query,
            'area': AREA,
            'per_page': PER_PAGE,
            'page': page,
            'search_field': 'name',
            'period': DAYS_BACK
        }

        for attempt in range(3):  # retry
            try:
                response = requests.get(url, params=params, headers=HEADERS, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    items = data.get('items', [])

                    if not items:
                        return all_items

                    all_items.extend(items)

                    total_pages = data.get('pages', 1)
                    if page >= total_pages - 1:
                        return all_items

                    time.sleep(0.3)
                    break

                else:
                    print(f"❌ API error {response.status_code}")
                    time.sleep(1)

            except Exception as e:
                print(f"⚠️ Retry {attempt + 1}: {e}")
                time.sleep(1)

    return all_items

# =======================
# 📥 LOAD HISTORY
# =======================

def load_history():
    if os.path.exists(DATA_PATH):
        try:
            df = pd.read_csv(DATA_PATH, sep=';', encoding='utf-8-sig')
            df['ID'] = df['ID'].astype(str)
            return df
        except Exception as e:
            print(f"Ошибка загрузки истории: {e}")
    return pd.DataFrame()

# =======================
# 🔄 TRANSFORM DATA
# =======================

def process_data(raw_data):
    rows = []

    for item in raw_data:
        name = item.get('name', '')

        if any(word in name.lower() for word in EXCLUDE_WORDS):
            continue

        salary = item.get('salary') or {}

        rows.append({
            'ID': str(item.get('id')),
            'Название': name,
            'Город': (item.get('area') or {}).get('name'),
            'Компания': (item.get('employer') or {}).get('name'),
            'Зарплата От': salary.get('from'),
            'Зарплата До': salary.get('to'),
            'Валюта': salary.get('currency'),
            'Опыт': (item.get('experience') or {}).get('name'),
            'Дата': item.get('published_at'),
            'Ссылка': item.get('alternate_url'),
            'Статус': 'Активна'
        })

    return pd.DataFrame(rows)

# =======================
# 📊 INCREMENTAL UPDATE
# =======================

def update_dataset(df_history, df_new):
    df = pd.concat([df_history, df_new], ignore_index=True)
    df = df.drop_duplicates(subset=['ID'], keep='last')

    if df.empty:
        return df

    df['Дата_temp'] = pd.to_datetime(df['Дата'], errors='coerce').dt.tz_localize(None)

    threshold_date = datetime.now() - timedelta(days=30)
    new_ids = df_new['ID'].tolist() if not df_new.empty else []

    df.loc[
        (~df['ID'].isin(new_ids)) &
        (df['Дата_temp'] < threshold_date),
        'Статус'
    ] = 'В архиве'

    df['Статус'] = df['Статус'].fillna('Активна')

    return df.drop(columns=['Дата_temp'])

# =======================
# 💾 SAVE DATA
# =======================

def save_data(df):
    try:
        os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
        df.to_csv(DATA_PATH, index=False, sep=';', encoding='utf-8-sig')
        print("✅ Данные сохранены")
    except Exception as e:
        print(f"Ошибка сохранения: {e}")

# =======================
# 🚀 MAIN PIPELINE
# =======================

def main():
    print("🚀 Запуск pipeline...")

    df_history = load_history()
    all_data = []

    for keyword in KEYWORDS:
        print(f"🔎 Загрузка: {keyword}")
        data = fetch_hh_data(keyword)
        all_data.extend(data)

    df_new = process_data(all_data)

    print(f"📊 Новых записей: {len(df_new)}")

    final_df = update_dataset(df_history, df_new)

    save_data(final_df)

    print("🎯 Готово!")

# =======================
# ▶️ RUN
# =======================

if __name__ == "__main__":
    main()
