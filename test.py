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

cursor.execute(