import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_artists = ", ".join([artist['name'] for artist in row['track']['artists']])
        album_released_date = row['track']['album']['release_date']
        album_uri = row['track']['uri']
        album_popularity = row['track']['popularity']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        
        album_element = {
            'album_id': album_id,
            'name': album_name,
            'artists': album_artists,
            'released_date': album_released_date,
            'uri': album_uri,
            'popularity': album_popularity,
            'total_tracks': album_total_tracks,
            'url': album_url
        }
        album_list.append(album_element)
    return album_list

def artist(data):
    artist_list = []
    for row in data['items']:
        for artist in row['track']['artists']:
            artist_dict = {
                'artist_id': artist['id'],
                'artist_name': artist['name'],
                'external_url': artist['href']
            }
            artist_list.append(artist_dict)
    return artist_list

def tracks(data):
    track_list = []
    for row in data['items']:
        value = row['track']
        tracks_dict = {
            'track_id': value['id'],
            'track_name': value['name'],
            'track_popularity': value['popularity'],
            'track_uri': value['uri'],
            'track_duration': value['duration_ms'],
            'track_url': value['external_urls'].get('spotify', '')
        }
        track_list.append(tracks_dict)
    return track_list 

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-extract-bindu'
    Key = 'raw-data/to_processed/'

    spotify_data = []
    spotify_keys = []

    # Get the list of objects from the bucket
    response = s3.list_objects_v2(Bucket=Bucket, Prefix=Key)

    # Ensure 'Contents' exists in the response
    if 'Contents' not in response:
        print(f"No objects found in bucket '{Bucket}' with prefix '{Key}'")
        return

    # Iterate over files in the bucket
    for file in response['Contents']:
        file_name = file['Key']
        print(f"Processing file: {file_name}")
        
        # Process only JSON files
        if file_name.split('.')[-1] == 'json':
            obj_response = s3.get_object(Bucket=Bucket, Key=file_name)
            content = obj_response['Body']
            json_object = json.loads(content.read())  # Read and parse JSON content
            spotify_data.append(json_object)
            spotify_keys.append(file_name)

    # Process the Spotify data
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        tracks_list = tracks(data)

        album_df = pd.DataFrame.from_dict(album_list)
        artist_df = pd.DataFrame.from_dict(artist_list)
        track_df = pd.DataFrame.from_dict(tracks_list)

        # Saving the track data to a CSV file and upload to S3
        songs_key = f'transformed-data/songs_data/song_transformed_{timestamp}.csv'
        song_buffer = StringIO()
        track_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body=song_content)

        # Saving the album data to CSV file and upload to S3
        album_key = f'transformed-data/album_data/album_transformed_{timestamp}.csv'
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        # Saving the artist data to CSV file and upload to S3
        artist_key = f'transformed-data/artist_data/artist_transformed_{timestamp}.csv'
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)

    # Move processed files to the "processed_data" folder
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {'Bucket': Bucket, 'Key': key}
        processed_key = f'raw-data/processed/{key.split("/")[-1]}'
        s3_resource.meta.client.copy(copy_source, Bucket, processed_key)
        s3_resource.Object(Bucket, key).delete()
