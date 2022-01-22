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
token = 'BQCcLygjMvWhkC_DaDcLjeFm5nGpX4XdUd4zxs0k8kibHp6MExcw_SdqYtcGxDoOTgvB_90dsZpqOJyPqKaHHfiglyiJ0S_CqvV0KEF3TGRDuKxA7Y4mwghjXWwfQXpv3hUKDf3T06hD_Qiv9iRfMpalObA-IOi55H9GE1-P'

# a api precisa de um parâmetro em milisegundos unix para determinar o tempo na qual a requisição vai rodar
today = datetime.datetime.today()
yesterday = today - datetime.timedelta(days=60)
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

## load ##
engine = sqlalchemy.create_engine(database) # uma "engine" é criada para acessar/ criar a db
connection = sqlite3.connect('spotify_tracks.sqlite') # uma conexão é criada para acessar a db
cursor = connection.cursor()
sql_query = """
CREATE TABLE IF NOT EXISTS spotify_tracks(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    release_date VARCHAR(200),
    played_at VARCHAR(200),
    date_played VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
)
"""
cursor.execute(sql_query)
print('Database criada.')

try:
    df.to_sql('spotify_tracks', engine, index = False, if_exists='append') # função que passa um dataframe diretamente para uma tabela SQL
except:
    print("Dados já existentes na tabela")

print('Processo de load finalizado')




## validação ##
if isValid(df):
    print('Processo de validação concluído. Dados autenticados.')

print(df)