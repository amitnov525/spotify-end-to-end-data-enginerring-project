import json
import boto3
import pandas as pd 
from io import StringIO
from datetime import datetime

def albumdata(data):
    l=[]
    for row in data['items']:
        id=row['track']['album']['id']
        name=row['track']['album']['name']
        release_date=row['track']['album']['release_date']
        total_tracks=row['track']['album']['total_tracks']
        spotify=row['track']['album']['external_urls']['spotify']
        d={'id':id,'name':name,'release_date':release_date,'total_tracks':total_tracks,'url':spotify}
        l.append(d)
    return l

def artistdata(data):
    ar_l=[]
    for i in data['items']:
        for key,value in i.items():
            if key=="track":
                for artist in value['artists']:
                    id=artist["id"]
                    name=artist['name']
                    url=artist['href']
                    d={'artist_id':id,'name':name,'url':url}
                    ar_l.append(d)
    print(len(ar_l))
    return ar_l

def songdata(data):
    song_list=[]
    for row in data['items']:
        song_id=row['track']['id']
        song_name=row['track']['name']
        song_duration=row['track']['duration_ms']
        song_url=row['track']['album']['external_urls']['spotify']
        song_popularity=row['track']['popularity']
        song_added=row['added_at']
        album_id=row['track']['album']['id']
        artist_id=row['track']['album']['artists'][0]['id']
        song_element={
            'song_id':song_id,'song_name':song_name,'song_duration':song_duration,'song_url':song_url,'song_popularity':song_popularity,'song_added':song_added,'album_id':album_id,'artist_id':artist_id
            
        }
        song_list.append(song_element)
    return song_list

def lambda_handler(event, context):
    s3=boto3.client("s3")
    Bucket="spotify-etl-project-amit2"
    Key="raw_data/to_processed/"
    spotify_data=[]
    spotify_keys=[]
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key=file['Key']
        if file_key.split('.')[-1]=="json" :
            response=s3.get_object(Bucket=Bucket,Key=file_key)
            content=response['Body']
            jsonObject=json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
    
    for data in spotify_data:
        album_list=albumdata(data)
        artist_list=artistdata(data)
        print(artist_list)
        song_list=songdata(data)
        album_df=pd.DataFrame(album_list)
        album_df.rename(columns={'id':'album_id'},inplace=True)
        album_df=album_df.drop_duplicates(subset=['album_id'])
        album_df['release_date']=pd.to_datetime(album_df['release_date'])
        artist_df=pd.DataFrame(artist_list)
        artist_df=artist_df.drop_duplicates(subset=['artist_id'])
        song_df=pd.DataFrame(song_list)
        song_df=song_df.drop_duplicates(subset=['song_id'])
        song_df['song_added']=pd.to_datetime(song_df['song_added'])
        song_key="transformed_data/songs_data/songs_transformed" + str(datetime.now()) + ".csv"
        song_buffer=StringIO()
        song_df.to_csv(song_buffer,index=False)
        song_content=song_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=song_key,Body=song_content)
        
        album_key="transformed_data/album_data/album_transformed" + str(datetime.now()) + ".csv"
        album_buffer=StringIO()
        album_df.to_csv(album_buffer,index=False)
        album_content=album_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=album_key,Body=album_content)
        
        artist_key="transformed_data/artist_data/artist_transformed" +str(datetime.now()) + ".csv"
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer,index=False)
        artist_content=artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=artist_key,Body=artist_content)
        
        s3_resource=boto3.resource('s3')
    for key1 in spotify_keys:
        copy_source={
            'Bucket':Bucket,
            'Key':key1
            }
        s3_resource.meta.client.copy(copy_source,Bucket,'raw_data/processed/' + key1.split("/")[-1])
        s3_resource.Object(Bucket,key1).delete()
