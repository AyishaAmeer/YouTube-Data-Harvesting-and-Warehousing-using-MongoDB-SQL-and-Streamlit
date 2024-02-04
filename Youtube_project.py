#Importing libraries for Youtube project:

from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
import json

#API key connection
def Api_connect():
    Api_id="AIzaSyDpGxJGPaS3FZUBBE1EUGWBLT_jJViBk30"
    api_service_name="youtube"
    Api_version="v3"
    youtube=build(api_service_name,Api_version,developerKey=Api_id)
    return youtube
Youtube=Api_connect()

#To get channel information:

def get_channel_info(channel_id):
    request=Youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()
    for i in response["items"]:
        data=dict(channel_name=i["snippet"]["title"],
                channel_Id=i["id"],
                subscribers=i["statistics"]["subscriberCount"],
                views=i["statistics"]["viewCount"],
                total_videos=i["statistics"]["videoCount"],
                channel_description=i["snippet"]["description"],
                playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"])
        
        
        return data
    
#To get Playlists information of channel:
    
def get_playlist_info(channel_id):
    next_page_token=None
    playlist_lst=[]
    while True:    
        request=Youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Playlist_Id=item["id"],
                      Title=item["snippet"]["title"],
                      Channel_id=item["snippet"]["channelId"],
                      Channel_name=item["snippet"]["channelTitle"],
                      PublishedAt=item["snippet"]["publishedAt"][:-1],
                      Video_count=item["contentDetails"]["itemCount"]
                      )
            playlist_lst.append(data)
        next_page_token=response.get("nextPageToken")
        if next_page_token is None:
            break
    return playlist_lst

    
#To get every video id in the channel:

def get_video_id(channel_id):
    video_ids=[]

    request=Youtube.channels().list(id=channel_id,
                                    part='contentDetails')
    response=request.execute()
    playlist_id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token=None

    while True:
        res1=Youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token).execute()
        
        for i in range(len(res1["items"])):
            video_ids.append(res1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token=res1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids
#video_final=get_video_id('UChGd9JY4yMegY6PxqpBjpRA')

#To convert duration from string to time:

def convert_dur(s):
  l = []
  f = ''
  for i in s:
    if i.isnumeric():
      f = f+i
    else:
      if f:
        l.append(f)
        f = ''
  if 'H' not in s:
    l.insert(0,'00')
  if 'M' not in s:
    l.insert(1,'00')
  if 'S' not in s:
    l.insert(-1,'00')
  return ':'.join(l)

#To get video information of every video:

def get_video_info(final_video_ids):
    Video_list=[]
    for video_id in final_video_ids:
        request=Youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        for item in response["items"]:
            data=dict(Channel_name=item["snippet"]["channelTitle"],
                    Channel_id=item["snippet"]["channelId"],
                    video_Id=item["id"],
                    Title=item["snippet"]["title"],
                    Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                    Description=item["snippet"].get("description"),
                    Published_date=item["snippet"]["publishedAt"][:-1],
                    Duration=convert_dur(item["contentDetails"]["duration"]),
                    Views=item["statistics"].get("viewCount"),
                    Likes=item["statistics"].get("likeCount"),
                    Comments=item["statistics"].get("commentCount"),
                    Favourites=item["statistics"]["favoriteCount"],
                    Definition=item["contentDetails"]["definition"],
                    Caption_status=item["contentDetails"]["caption"]
                    )
            
            Video_list.append(data)
    return Video_list

#To get comment information of every video:

def get_comment_info(video_final):
    comment_list=[]
    try:
        for video_id in video_final:
            request=Youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50      
            )
            response=request.execute()
            for item in response["items"]:
                data=dict(Comment_id=item["snippet"]["topLevelComment"]["id"],
                        Video_ID=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                        Comment_text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        Comment_Published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"][:-1]
                        )
                comment_list.append(data)
    except:
        pass
    return comment_list


#To connect vscode to MongoDB:

from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/')
db = client['Project1']
def Channel_details(channel_id):
    chn_details=get_channel_info(channel_id)
    ply_details=get_playlist_info(channel_id)
    vid_ids=get_video_id(channel_id)
    vid_details=get_video_info(vid_ids)
    com_details=get_comment_info(vid_ids)

    collection1=db["channel_details"]
    collection1.insert_one({"channel_information":chn_details,
                            "playlist_information":ply_details,
                            "video_information":vid_details,
                            "comment_information":com_details}
                            )
    return "Upload completed successfully"

#To connect vscode to Mysql:

#create and insert query for channel table:

def channel_table():
    config={"user":"root",
            "password":"Aspnas@2020",
            "host":'127.0.0.1',
            "database":"projectdb",
            "port":3306}
    connection=mysql.connector.connect(**config)
    cursor=connection.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    connection.commit()

    try:
        create_query="""create table if not exists channels(channel_name varchar(100),
                                                            channel_id varchar(80) primary key,
                                                            subscribers bigint,
                                                            views bigint,
                                                            total_videos int,
                                                            channel_description text,
                                                            playlist_id varchar(80))"""
        cursor.execute(create_query)
        connection.commit()
    except:
        print("channels table already created")

    ch_list=[]
    db=client['Project1']
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)
    
    for index,row in df.iterrows():
        insert_query='''insert into channels(channel_name,
                                            channel_Id,
                                            subscribers,
                                            views,
                                            total_videos,
                                            channel_description,
                                            playlist_id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row["channel_name"],
                row["channel_Id"],
                row["subscribers"],
                row["views"],
                row["total_videos"],
                row["channel_description"],
                row["playlist_id"])
        try:
            cursor.execute(insert_query,values)
            connection.commit()
        
        except:
            print("channels values are already inserted")

#create and insert query for playlist table:
            
def playlist_table():
    config={"user":"root",
                "password":"Aspnas@2020",
                "host":'127.0.0.1',
                "database":"projectdb",
                "port":3306}
    connection=mysql.connector.connect(**config)
    cursor=connection.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    connection.commit()
    try:
        create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                            Title varchar(100),
                                                            Channel_id varchar(100),
                                                            Channel_name varchar(100),
                                                            PublishedAt timestamp,
                                                            Video_count int)'''
        cursor.execute(create_query)
        connection.commit()
    except:
        print("channels table already created")

    pl_list=[]
    db=client['Project1']
    collection1=db["channel_details"]
    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)

    config={"user":"root",
            "password":"Aspnas@2020",
            "host":'127.0.0.1',
            "database":"projectdb",
            "port":3306}
    connection=mysql.connector.connect(**config)
    cursor=connection.cursor()

    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_id,
                                            Channel_name,
                                            PublishedAt,
                                            Video_count
                                            )
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''

        values=(row["Playlist_Id"],
                row["Title"],
                row["Channel_id"],
                row["Channel_name"],
                row["PublishedAt"],
                row["Video_count"]
                )
        try:
            cursor.execute(insert_query,values)
            connection.commit()
        except:
            print("playlists values already inserted")

#create and insert query for videos table:
            
def videos_table():
    config={"user":"root",
            "password":"Aspnas@2020",
            "host":'127.0.0.1',
            "database":"projectdb",
            "port":3306}
    connection=mysql.connector.connect(**config)
    cursor=connection.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    connection.commit()

    try:
        create_query="""create table if not exists videos(Channel_name varchar(100),
                                                    Channel_id varchar(100),
                                                    video_Id varchar(50) primary key,
                                                    Title varchar(150),
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_date varchar(50),
                                                    Duration TIME,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favourites int,
                                                    Definition varchar(100),
                                                    Caption_status varchar(50) 
                                                    )"""
            
        cursor.execute(create_query)
        connection.commit()
    except:
        print("videos table already created")   

    vi_list=[]
    db=client['Project1']
    collection1=db["channel_details"]
    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)


    config={"user":"root",
            "password":"Aspnas@2020",
            "host":'127.0.0.1',
            "database":"projectdb",
            "port":3306}
    connection=mysql.connector.connect(**config)
    cursor=connection.cursor()

    
    for index,row in df2.iterrows():
        insert_query='''insert into videos(Channel_name,
                                        Channel_id,
                                        video_Id,
                                        Title,
                                        Thumbnail,
                                        Description,
                                        Published_date,
                                        Duration,
                                        Views,
                                        Likes,
                                        Comments,
                                        Favourites,
                                        Definition,
                                        Caption_status 
                                        )                  
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row["Channel_name"],
                row["Channel_id"],
                row["video_Id"],
                row["Title"],
                row["Thumbnail"],
                row["Description"],
                row["Published_date"],
                row["Duration"],
                row["Views"],
                row["Likes"],
                row["Comments"],
                row["Favourites"],
                row["Definition"],
                row["Caption_status"]
                )
        try:
            cursor.execute(insert_query,values)
            connection.commit()
        except:
            print("videos values already inserted")
        
#create and insert query for comment table:
            
def comment_table():
    config={"user":"root",
                "password":"Aspnas@2020",
                "host":'127.0.0.1',
                "database":"projectdb",
                "port":3306}
    connection=mysql.connector.connect(**config)
    cursor=connection.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    connection.commit()

    try:
        create_query="""create table if not exists comments(Comment_id varchar(100) primary key,
                                                            Video_ID varchar(50),
                                                            Comment_text text,
                                                            Comment_Author varchar(150),
                                                            Comment_Published varchar(50)
                                                            )"""
        
        cursor.execute(create_query)
        connection.commit()
    except:
        print("comments table already created")
        
    com_list=[]
    db=client['Project1']
    collection1=db["channel_details"]
    for com_data in collection1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
        insert_query='''insert into comments(Comment_id ,
                                                Video_ID ,
                                                Comment_text,
                                                Comment_Author,
                                                Comment_Published
                                            )                  
                                            values(%s,%s,%s,%s,%s)'''
        values=(row["Comment_id"],
                row["Video_ID"],
                row["Comment_text"],
                row["Comment_Author"],
                row["Comment_Published"]
                )
        try:
            cursor.execute(insert_query,values)
            connection.commit()
        except:
            print("comments values already inserted")

# combine all functions in one function for creating table in sql:
            
def tables():
    channel_table()
    playlist_table()
    videos_table()
    comment_table()
    return "Tables created successfully"
 
#creating functions for viewing tables in streamlit

def view_channels_table():
    ch_list=[]
    db=client['Project1']
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)
    return df



def view_playlist_tables():
    pl_list=[]
    db=client['Project1']
    collection1=db["channel_details"]
    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1
def view_video_tables():
    vi_list=[]
    db=client['Project1']
    collection1=db["channel_details"]
    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2
def view_comment_tables():
    com_list=[]
    db=client['Project1']
    collection1=db["channel_details"]
    for com_data in collection1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3

#streamlit code

header=st.container()
with header:
    st.balloons()
    st.title(":violet[Welcome to my Data Science Project-Youtube Data Harvesting and Warehousing]")
    st.header("Technical Skills")
    st.subheader("Python")
    st.subheader("Data Collection")
    st.subheader("MongoDB")
    st.subheader("API Integration")
    st.subheader("Data Management using MongoDB and SQL")
    st.subheader("Streamlit")
with header:
    st.header("Collecting datas from Youtube:")
channel_id=st.text_input("Enter the channel ID:")

#button for migrating datas to mongodb:

if st.button("Transfer data to MongoDB"):
    ch_ids=[]
    db=client['Project1']
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_Id"])

        if channel_id in ch_ids:
            st.success("Channel Details of the given channel id already exists")
            
        else:
            insert=Channel_details(channel_id)
            st.success(insert)

#Button for migrating datas to sql

if st.button("Transfer data to SQL"):
    Table=tables()
    st.success(Table)
show_table=st.radio("Select the option for view",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

#for radio button:
      
if show_table=="CHANNELS":
    view_channels_table()
    config={"user":"root",
            "password":"Aspnas@2020",
            "host":'127.0.0.1',
            "database":"projectdb",
            "port":3306}
    connection=mysql.connector.connect(**config)
    cursor=connection.cursor()
    chartdata='''select Channel_name as channelname,views as Totalviews from channels 
                 order by views desc'''
    cursor.execute(chartdata)
    #connection.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["channel name","Total views"])
    #st.write(df)
    dft=df.set_index("channel name").T
    
    st.subheader("Graph of channel names with views")
    st.bar_chart(dft)

if show_table=="PLAYLISTS":
    view_playlist_tables()
    

if show_table=="VIDEOS":
    view_video_tables()
    config={"user":"root",
            "password":"Aspnas@2020",
            "host":'127.0.0.1',
            "database":"projectdb",
            "port":3306}
    connection=mysql.connector.connect(**config)
    cursor=connection.cursor()
    chartdata='''select Channel_name as channelname,Likes as TotalLikes from videos'''
    cursor.execute(chartdata)
    #connection.commit()
    t2=cursor.fetchall()
    df1=pd.DataFrame(t2,columns=["channel name","Total likes"])
    st.write(df1)
    dft=df1.set_index("channel name")
    
    st.subheader("Graph of channel names with likes")
    st.bar_chart(dft)
    

if show_table=="COMMENTS":
    view_comment_tables()

#Code for 10 quetions:

config={"user":"root",
            "password":"Aspnas@2020",
            "host":'127.0.0.1',
            "database":"projectdb",
            "port":3306}
connection=mysql.connector.connect(**config)
cursor=connection.cursor()

question=st.selectbox("Select your question:",("1. All videos and channel name",
                                            "2. channels with most number of videos",
                                            "3. 10 most viewed videos",
                                            "4. comments in each videos",
                                            "5. Videos with highest likes",
                                            "6. likes of all videos",
                                            "7. views of each channel",
                                            "8. videos published in the year of 2022",
                                            "9. average duration of all videos in each channel",
                                            "10. videos with highest number of comments"))

if question=="1. All videos and channel name":
    query1='''select Title as videos,Channel_name as channelname from videos'''
    cursor.execute(query1)
    #connection.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_of_videos from channels
            order by total_videos desc '''
    cursor.execute(query2)
    #connection.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","ordered videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select Views as views,Channel_name as channelname,Title as videotitle from videos
            where Views is not null order by Views desc limit 10'''
    cursor.execute(query3)
    #connection.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","video Title",])
    st.write(df3)
elif question=="4. comments in each videos":
    query4='''select Title as videotitle,Comments as no_of_comments from videos 
            where Comments is not null '''
    cursor.execute(query4)
#connection.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["video title","No of comments"])
    st.write(df4)
elif question=="5. Videos with highest likes":
    query5='''select Channel_name as channelname,Title as videotitle,Likes as no_of_likes from videos 
            where Likes is not null order by Likes desc '''
    cursor.execute(query5)
    #connection.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["channel name","video Title","No of likes"])
    st.write(df5)
elif question=="6. likes of all videos":
    query6='''select Likes as likescount,Title as videotitle from videos'''
    cursor.execute(query6)
    #connection.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likescount","videotitle"])
    st.write(df6)
elif question=="7. views of each channel":
    query7='''select channel_name as channelname,views as totalviews from channels'''
    cursor.execute(query7)
    #connection.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channelname","total views"])
    st.write(df7)
elif question=="8. videos published in the year of 2022":
    query8='''select Title as video_title,Published_date as videorelease,Channel_name as channelname from videos
            where extract(year from Published_date)=2022'''
    cursor.execute(query8)
    #connection.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","videorelease","channelname"])
    st.write(df8)
elif question=="9. average duration of all videos in each channel":
    query9='''select Channel_name as channelname,
            SEC_TO_TIME(AVG(TIME_TO_SEC(Duration))) as durationavg from videos
            group by Channel_name'''
    cursor.execute(query9)
    #connection.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","Duration average"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["Duration average"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,AVGduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10. videos with highest number of comments":
    query10='''select Channel_name as channelname,
            Title as videotitle,    
            Comments as No_of_comments from videos
            where Comments is not null order by Comments desc'''
    cursor.execute(query10)
    #connection.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["channel name","video title","no of comments"])
    st.write(df10)





