import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes the song_data and inserts into the following tables:
    - songs
    - artists
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes the data that relates to NextSong then inserts the data into the following tables:
    - time
    - users
    - songplays
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = t.apply(lambda x: [x, x.hour, x.day, x.weekofyear, x.month, x.year, x.weekday()]).tolist()
    column_labels = (['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday'])
    time_df = pd.DataFrame(time_data, columns=column_labels)
    for index, row in time_df.iterrows():
        cur.execute(time_table_insert, (row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
#     temp_file = os.getcwd()+'/'+'temp_data.csv'
#     time_df.to_csv(temp_file, index=False)
#     cur.execute(bulk_time_table_insert, (temp_file,))
#     os.remove(temp_file)

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for index, row in user_df.iterrows():
        cur.execute(user_table_insert, (row[0], row[1], row[2], row[3], row[4]))

    # insert songplay records
    songplay = []
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        songplay.append(songplay_data)

    songplay_df = pd.DataFrame(songplay, columns=['timestamp','user_id','level','song_id','artist_id','session_id','location','user_agent'])

    temp_file = os.getcwd()+'/'+'temp_data.csv'
    songplay_df.to_csv(temp_file, index=False)
    cur.execute(bulk_songplay_table_insert, (temp_file,))
    os.remove(temp_file)

def process_data(cur, conn, filepath, func):
    """
    - Processes the data from 'filepath' via the function 'func'
    
    - Prints the progress of what has been completed
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Connects to database and establishes a connection
    
    - Processes the song_data and saves to the database tables
    
    - Processes the log_data and saves to the database tables
    
    - Closes out the connection
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()