#!/usr/bin/env python
# coding: utf-8

# In[153]:


import sys
import re 
sys.path.append('/Users/ugeebindu/.local/lib/python3.10/site-packages')
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd


# In[154]:


client_credentials_manager = SpotifyClientCredentials(
    client_id='0e35f2f89cd740ea870cb37b4b1bade8', 
    client_secret='bdc00aed01fd4a748cec47f80ff86c2f'  
)


# In[155]:


sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


# In[156]:


playlist_link='https://open.spotify.com/playlist/1d2fAul5T010POHxi1bQ4i'


# In[157]:


playlist_URI=playlist_link.split('/')[-1]


# In[158]:


playlist_URI


# In[159]:


data=sp.playlist_tracks(playlist_URI)
data


# In[160]:


len(data['items'])


# In[161]:


data['items']


# In[162]:


data['items'][0]


# In[163]:


data['items'][0]['track']['name']


# In[164]:


data['items'][0]['track']['artists']


# In[165]:


data['items'][0]['track']['artists'][0]['name']


# In[166]:


data['items'][0]['track']['album']['name']


# In[167]:


data['items'][0]['track']['album']['release_date']


# In[168]:


data['items'][0]['track']['album']['uri']


# In[169]:


for row in data['items']:
    album_id=row['track']['album']['id']
    album_name=row['track']['album']['name']
    album_artists=", ".join([artist['name'] for artist in row['track']['artists']]) 
    album_released_date=row['track']['album']['release_date']
    album_uri=row['track']['uri']
    album_popularity=row['track']['popularity']
    album_total_tracks=row['track']['album']['total_tracks']
    album_url=row['track']['album']['external_urls']['spotify']
    print(album_name)
    


# In[170]:


for row in data['items']:
    album_id=row['track']['album']['id']
    album_name=row['track']['album']['name']
    album_artists=", ".join([artist['name'] for artist in row['track']['artists']]) 
    album_released_date=row['track']['album']['release_date']
    album_uri=row['track']['uri']
    album_popularity=row['track']['popularity']
    album_total_tracks=row['track']['album']['total_tracks']
    album_url=row['track']['album']['external_urls']['spotify']
    
    album_element={'album_id':album_id,
                   'name':album_name,
                   'artists':album_artists,
                   'released_date':album_released_date,
                   'uri':album_uri,
                   'popularity':album_popularity,
                   'total_tracks':album_total_tracks,
                   'url':album_url}
    print(album_element)


# In[171]:


album_list=[]
for row in data['items']:
    album_id=row['track']['album']['id']
    album_name=row['track']['album']['name']
    album_artists=", ".join([artist['name'] for artist in row['track']['artists']]) 
    album_released_date=row['track']['album']['release_date']
    album_total_tracks=row['track']['album']['total_tracks']
    album_url=row['track']['album']['external_urls']['spotify']
    
    album_element={'album_id':album_id,
                   'name':album_name,
                   'artists':album_artists,
                   'released_date':album_released_date,
                   'total_tracks':album_total_tracks,
                   'url':album_url}
    album_list.append(album_element)
    


# In[172]:


album_list


# In[173]:


data['items'][0]


# In[174]:


data['items'][0]['track']['artists']  #artists has a dictionary inside track


# In[175]:


data['items'][2]['track']['artists']


# In[176]:


data['items'][2]


# In[177]:


#other way of seperating:
artist_list=[]
for row in data['items']:
    for key,value in row.items():
        if key=='track':
            for artist in value['artists']:
                artist_dict={'artist_id':artist['id'],
                             'artist_name':artist['name'],
                             'external_url':artist['href']}
                artist_list.append(artist_dict)


# In[178]:


artist_list


# In[179]:


track_list=[]
for row in data['items']:
    for key,value in row.items():
        if key=='track':
                tracks_dict={'track_id':value['id'],
                              'track_name':value['name'],
                               'track_popularity':value['popularity'],
                               'track_uri':value['uri'],
                               'track_duration':value['duration_ms'],
                               'track_url':value['external_urls']}
                track_list.append(tracks_dict)
    


# In[180]:


track_list


# In[181]:


album_df=pd.DataFrame.from_dict(album_list)


# In[182]:


album_df.head()


# In[183]:


track_df=pd.DataFrame.from_dict(track_list)


# In[184]:


track_df.head()


# In[185]:


artist_df=pd.DataFrame.from_dict(artist_list)


# In[186]:


artist_df


# In[187]:


album_df.info()


# In[190]:


album_df['release_date']=pd.to_datetime(album_df['released_date'])


# In[191]:


album_df.info()


# In[192]:


album_df=album_df.drop_duplicates(subset=['album_id'])


# In[193]:


album_df


# In[ ]:




