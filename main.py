import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

## transform ##
def isValid(df: pd.DataFrame): 
    if df.empty: # verifica se dados foram retornados ou não
        print('Não foram encontradas músicas nas últimas 24 horas.')
        return False

    if pd.Series(df['played_at']).is_unique: # verifica se há chaves primárias repetidas
        pass
    else:
        raise Exception('Há PK repetidas.')

    if df.isnull().values.any(): # verifica se há valores vazios ou nulos
        raise Exception('Há valores vazios/ nulos.')

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df['date_played'].tolist()

    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception('Há músicas que não são das últimas 24 horas.')
    
    return True



## extract ##
database = 'sqlite:///spotify_tracks'
user_id = 'vnihu9uaue7nfbjh80kpp1mug'
token = 'BQDu5XnG0O5AEBukUNxKfYMVY2EjZ6HStG1iFuNQrrgD4jojvc-U8xyQFJTbZJR4xrfiFyyuBGN4maVwC8D8GCsqbZi3jKYouMaJpQvGx_9QqFC0-YCIwKLUZ59Yhy5wweoDGCYhg33MUYtnqFYkR1Y_AiALA2Lm6MRdacXl'

# a api precisa de um parâmetro em milisegundos unix para determinar o tempo na qual a requisição vai rodar
today = datetime.datetime.today()
yesterday = today - datetime.timedelta(days=1)
yesterday_unix = int(yesterday.timestamp()) * 1000 

headers = {
    'Accept': 'application/ json',
    'Content-type' : 'application/json',
    'Authorization' : 'Bearer {token}'.format(token=token)
} 

r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=yesterday_unix), headers = headers)

data = r.json()

song_names = []
artist_names = []
release_dates = []
played_at = []
date_played = []

for song in data['items']:
    song_names.append(song['track']['name'])
    artist_names.append(song['track']['album']['artists'][0]['name'])
    release_dates.append(song['track']['album']['release_date'])
    played_at.append(song['played_at'])
    date_played.append(song['played_at'][0:10])

    song_dict = {
        'song_name': song_names,
        'artist_name': artist_names,
        'release_date': release_dates,
        'played_at': played_at,
        'date_played': date_played
    }

df = pd.DataFrame(song_dict, columns=['song_name', 'release_date', 'artist_name', 'played_at', 'date_played'])

## validação ##
if isValid(df):
    print('Processo de validação concluído. Dados autenticados.')

print(df)