import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

# extract
database = 'sqlite:///spotify_tracks'
user_id = 'vnihu9uaue7nfbjh80kpp1mug'
token = 'BQCyZ7JXaS2-LqSX9sxZ-liYuH1cqCwd2jGR9rSS6oSps1L-NUnW7c2x5bQq5RpV7chXDAk9ajwGaPOHel3HYnEFoSz4RGeNR8855Aq3PCVGQtu-foJIP2XtBphTLeFFpIIj43r7jQtR11gQxYCL5xVTnuFIFspN0U-CrYJ1'

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

for song in data['items']:
    song_names.append(song['track']['name'])
    artist_names.append(song['track']['album']['artists'][0]['name'])
    release_dates.append(song['track']['album']['release_date'])
    played_at.append(song['played_at'])

    song_dict = {
        'song_name': song_names,
        'artist_name': artist_names,
        'release_date': release_dates,
        'played_at': played_at 
    }

df = pd.DataFrame(song_dict, columns=['song_name', 'release_date', 'artist_name', 'played_at'])

print(df)