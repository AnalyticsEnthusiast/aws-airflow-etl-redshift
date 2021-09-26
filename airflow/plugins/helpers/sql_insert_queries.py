class InsertSqlQueries:
    
    songplay_table_insert = ("""
        INSERT INTO public.songplays (
         start_time,
         user_id, 
         level, 
         song_id, 
         artist_id, 
         session_id, 
         location, 
         user_agent
        )
        SELECT
                events.start_time,
                events.userid as user_id, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid as session_id, 
                events.location, 
                events.useragent as user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration;
    """)

    users_table_insert = ("""
        INSERT INTO public.users (
        user_id, 
        first_name, 
        last_name, 
        gender,
        level
        )
        SELECT 
        sq.userid as user_id,
        sq.firstname as first_name, 
        sq.lastname as last_name, 
        sq.gender, 
        sq.level
        FROM (
        SELECT distinct 
                userid,
                firstname,
                lastname,
                gender, 
                level,
                ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) as num
                FROM staging_events
                WHERE page='NextSong'
              ) sq
            WHERE sq.num = 1;
    """)

    songs_table_insert = ("""
        INSERT INTO public.songs (
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
        )
        SELECT distinct 
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
        FROM staging_songs;
    """)

    artists_table_insert = ("""
        INSERT INTO public.artists (
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
        )
        SELECT
        sq.artist_id, 
        sq.artist_name, 
        sq.artist_location, 
        sq.artist_latitude, 
        sq.artist_longitude
        FROM (
        SELECT distinct 
                artist_id, 
                artist_name, 
                artist_location, 
                artist_latitude, 
                artist_longitude,
                ROW_NUMBER() OVER(PARTITION BY artist_id ORDER by duration) as num
        FROM staging_songs
              ) sq
          WHERE sq.num = 1;
    """)

    time_table_insert = ("""
        INSERT INTO public."time" (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        day_of_week
        )
        SELECT 
        start_time, 
        extract(hour from start_time) as hour, 
        extract(day from start_time) as day,
        extract(week from start_time) as week, 
        extract(month from start_time) as month, 
        extract(year from start_time) as year, 
        extract(dayofweek from start_time) as day_of_week
        FROM songplays;
    """)