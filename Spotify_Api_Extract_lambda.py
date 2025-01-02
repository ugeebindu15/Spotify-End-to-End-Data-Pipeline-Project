import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):

    client_id=os.environ.get('client_id')
    client_secret=os.environ.get('client_secret')

    client_credentials_manager = SpotifyClientCredentials(
    client_id='0e35f2f89cd740ea870cb37b4b1bade8',  
    client_secret='bdc00aed01fd4a748cec47f80ff86c2f' )
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    playlists=sp.user_playlists('spotify')


    playlist_link='https://open.spotify.com/playlist/1d2fAul5T010POHxi1bQ4i'
    playlist_URI=playlist_link.split('/')[-1]

    spotify_data=sp.playlist_tracks(playlist_URI)


    client=boto3.client('s3')

    filename='spotify_raw_'+ str(datetime.now()) + '.json'

    client.put_object(
        Bucket='spotify-etl-extract-bindu',
        Key='raw-data/to_processed/'+filename,
        Body=json.dumps(spotify_data)
    )

   



    
