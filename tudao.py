 import sqlite3
import requests
import pandas as pd

# Inicializar a conexão com o SQLite
conn = sqlite3.connect('wines.db')
cursor = conn.cursor()

# Criar tabela se ela ainda não existir
cursor.execute('''
CREATE TABLE IF NOT EXISTS wines (
    winery TEXT,
    year INTEGER,
    wine_id INTEGER PRIMARY KEY,
    wine_name TEXT,
    rating REAL,
    num_review INTEGER,
    wine_type TEXT,
    wine_region TEXT,
    country TEXT,
    grape TEXT,
    price REAL,
    link TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS reviews (
    year INTEGER,
    wine_id INTEGER,
    user_grade REAL,
    note TEXT,
    created_at TEXT,
    user_id INTEGER,
    followers INTEGER,
    following INTEGER,
    user_ratings INTEGER,
    language TEXT,
    label TEXT,
    FOREIGN KEY (wine_id) REFERENCES wines(wine_id)
)
''')

conn.commit()

def insert_wine_data(data):
    cursor.executemany('''
    INSERT INTO wines (winery, year, wine_name, rating, num_review, wine_type, wine_region, country, grape, price, link)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', data)
    conn.commit()

def insert_review_data(data):
    cursor.executemany('''
    INSERT INTO reviews (year, wine_id, user_grade, note, created_at, user_id, followers, following, user_ratings, language, label)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', data)
    conn.commit()

def get_all_wines():
    cursor.execute('SELECT * FROM wines')
    return cursor.fetchall()

def get_reviews_for_wine(wine_id):
    cursor.execute('SELECT * FROM reviews WHERE wine_id = ?', (wine_id,))
    return cursor.fetchall()

# # Inserir alguns dados fictícios
# sample_wines = [
#     ("Winery A", 2018, "Wine A 2018", 4.5, 100, "Red", "Tuscany", "Italy", "Sangiovese", 20.5, "http://example.com/wineA"),
#     ("Winery B", 2019, "Wine B 2019", 4.0, 150, "White", "Bordeaux", "France", "Chardonnay", 18.0, "http://example.com/wineB")
# ]

# insert_wine_data(sample_wines)

# sample_reviews = [
#     (2018, 1, 4.5, "Great wine!", "2022-05-01", 123, 50, 10, 300, "en", "label1"),
#     (2019, 2, 3.5, "Good wine.", "2022-05-02", 124, 30, 5, 100, "en", "label2")
# ]

# insert_review_data(sample_reviews)

# Consultar todos os vinhos e suas avaliações
wines = get_all_wines()
for wine in wines:
    print(wine)
    reviews = get_reviews_for_wine(wine[2])  # wine[2] é o wine_id
    for review in reviews:
        print("\t", review)


def fetch_wine_details(country="IT", max_price="1500", min_price="500", max_pages=90):
    list_wines = []

    for page in range(1, max_pages + 1):
        try:
            response = requests.get(
                "https://www.vivino.com/api/explore/explore",
                params={
                    "country_code": country,
                    "country_codes[]": country.lower(),
                    "currency_code": "EUR",
                    "grape_filter": "varietal",
                    "min_rating": "1",
                    "order_by": "price",
                    "order": "asc",
                    "page": page,
                    "price_range_max": max_price,
                    "price_range_min": min_price,
                    "wine_type_ids[]": "1",
                },
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
                },
            )

            if not response.json()["explore_vintage"]:
                break

            results = [
                (
                    t["vintage"]["wine"]["winery"]["name"],
                    t["vintage"]["year"],
                    t["vintage"]["wine"]["id"],
                    f'{t["vintage"]["wine"]["name"]} {t["vintage"]["year"]}',
                    t["vintage"]["statistics"]["ratings_average"],
                    t["vintage"]["statistics"]["ratings_count"],
                    type_code[str(t["vintage"]["wine"]["type_id"])],
                    f'{t["vintage"]["wine"]["region"]["country"]["name"]} {t["vintage"]["wine"]["region"]["name"]}',
                    t["vintage"]["wine"]["region"]["country"]["code"],
                    f'{t["vintage"]["wine"]["region"]["country"]["most_used_grapes"][0]["name"]} {t["vintage"]["wine"]["region"]["country"]["most_used_grapes"][1]["name"]}',
                    round(t["price"]["amount"], 2) if t["price"] else "-",
                    f'https://www.vivino.com/{t["vintage"]["seo_name"]}/w/{t["vintage"]["wine"]["id"]}'
                )
                for t in response.json()["explore_vintage"]["matches"]
            ]

            list_wines.extend(results)

        except Exception as e:
            print(f"An error occurred on page {page}: {e}")
            continue

    df = pd.DataFrame(
        list_wines,
        columns=["Winery", "Year", "Wine ID", "Wine", "Rating", "num_review", "Wine type", "Wine region", "Country", "Grape", "price", "link"],
    )

    return df



def write_to_sqlite(df, db_name, table_name):
    """
    Escreve um DataFrame do Pandas em um banco de dados SQLite.

    :param df: DataFrame para ser escrito no SQLite.
    :param db_name: Nome do arquivo do banco de dados SQLite.
    :param table_name: Nome da tabela no SQLite onde o DataFrame será escrito.
    """
    try:
        # Estabelecendo uma conexão com o banco de dados SQLite
        conn = sqlite3.connect(db_name)
        
        # Escrevendo o DataFrame no banco de dados SQLite
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        # Fechando a conexão
        conn.close()
        
        print(f"Data successfully written to {table_name} in {db_name}")
        
    except Exception as e:
        print(f"An error occurred: {e}")


wine_data = fetch_wine_details(country="IT", max_price="1500", min_price="0", max_pages=2)
write_to_sqlite(wine_data, "wine_data.db", "wines")

print(wine_data)
