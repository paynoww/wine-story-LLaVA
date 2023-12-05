import requests
import json
import pandas as pd
import sqlite3

class WineDataLoader:
    def __init__(self, database_path):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
        }
        self.payload = {
            "country_codes[]": ["es","pt"],
            #"currency_code": "EUR",
            #"grape_filter": "varietal",
            #"min_rating": "1",
            #"order_by": "price",
            #"order": "asc",
            #"page": 1,
            #"price_range_max": "5000",
            #"price_range_min": "0",
            #"wine_type_ids[]": "1",
        }#
        self.database_path = database_path

    def create_database(self):
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        # Create a table to store wine data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wine_data (
                winery_name TEXT,
                vintage_year INTEGER,
                wine_id INTEGER,
                wine_name TEXT,
                ratings_average REAL,
                ratings_count INTEGER,
                price REAL,
                country_name TEXT,
                region_name TEXT
            )
        ''')

        # Create a table to store wine reviews
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS wine_reviews (
                wine_id INTEGER,
                note TEXT,
                note_state TEXT,
                rating REAL,
                created_at TEXT
            )
        ''')

        conn.commit()
        conn.close()

    def fetch_reviews(self, wine_id):
        reviews_url = f'https://www.vivino.com/api/wines/{wine_id}/reviews'
        reviews_response = requests.get(reviews_url, headers=self.headers)
        
        if reviews_response.status_code == 200:
            reviews_data = reviews_response.json()
            return reviews_data.get('reviews', [])
        else:
            return []

    def fetch_and_store_data(self, max_pages):
        self.create_database()
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        for page in range(1, max_pages + 1):
            self.payload['page'] = page

            print(f'Requesting data from page: {page}')

            r = requests.get('https://www.vivino.com/api/explore/explore?',
                             params=self.payload, headers=self.headers)

            data = r.json()["explore_vintage"]["matches"]

            for item in data:
                wine_data = (
                    item["vintage"]["wine"]["winery"]["name"],
                    item["vintage"]["year"],
                    item["vintage"]["wine"]["id"],
                    f'{item["vintage"]["wine"]["name"]} {item["vintage"]["year"]}',
                    item["vintage"]["statistics"]["ratings_average"],
                    item["vintage"]["statistics"]["ratings_count"],
                    item["prices"][0]["amount"],
                    item['vintage']['wine']['region']['country']['name'],
                    item['vintage']['wine']['region']['name']
                )

                cursor.execute('''
                    INSERT INTO wine_data (
                        winery_name,
                        vintage_year,
                        wine_id,
                        wine_name,
                        ratings_average,
                        ratings_count,
                        price,
                        country_name,
                        region_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', wine_data)

                # Fetch reviews for the current wine and store them
                reviews = self.fetch_reviews(item["vintage"]["wine"]["id"])

                if reviews:
                    for review in reviews:
                        review_data = (
                            item["vintage"]["wine"]["id"],
                            review["note"],
                            review["rating"],
                            review["created_at"]
                        )

                        cursor.execute('''
                            INSERT INTO wine_reviews (
                                wine_id,
                                note,
                                rating,
                                created_at
                            ) VALUES (?, ?, ?, ?)
                        ''', review_data)

                conn.commit()

        conn.close()


if __name__ == "__main__":
    loader = WineDataLoader("wine_data.db")
    max_pages = 100  # Define the maximum number of pages to fetch
    loader.fetch_and_store_data(max_pages)
