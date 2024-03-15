class SqlQueries:

    staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist          VARCHAR (200),
        auth            VARCHAR (20),
        firstName       VARCHAR (25),
        gender          VARCHAR (1),
        itemInSession   INTEGER,
        lastName        VARCHAR (25),
        length          DOUBLE PRECISION,
        level           VARCHAR (10),
        location        VARCHAR (100),
        method          VARCHAR (5),
        page            VARCHAR (30),
        registration    DOUBLE PRECISION,
        sessionId       INTEGER,
        song            VARCHAR (400),                        
        status          INTEGER,
        ts              BIGINT,
        userAgent       VARCHAR (250),
        userId          INTEGER
        )
    """)

    staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id             VARCHAR(20) NOT NULL DISTKEY,
        num_songs           INTEGER,
        artist_id           VARCHAR(20) NOT NULL,
        artist_latitude     DOUBLE PRECISION,
        artist_longitude    DOUBLE PRECISION,
        artist_location     VARCHAR (400),
        artist_name         VARCHAR (400) NOT NULL,
        title               VARCHAR (400),
        duration            DOUBLE PRECISION,
        year                INTEGER
    );
    """)

    songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id    INTEGER         PRIMARY KEY,
        start_time     TIMESTAMP       SORTKEY          NOT NULL,
        user_id        INTEGER         NOT NULL,
        level          VARCHAR (50)    NOT NULL,
        song_id        VARCHAR(50)     NOT NULL          DISTKEY,
        artist_id      VARCHAR(20)     NOT NULL,
        session_id     INTEGER         NOT NULL,
        location       VARCHAR (500),
        user_agent     VARCHAR (500)   NOT NULL
    );
""")

    user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    INTEGER     PRIMARY KEY  SORTKEY,
        first_name VARCHAR (150)   NOT NULL,
        last_name  VARCHAR         NOT NULL,
        gender     VARCHAR (5)     NOT NULL,
        level      VARCHAR (50)    NOT NULL
        ) DISTSTYLE ALL;
""")
    song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id    VARCHAR(50)     PRIMARY KEY SORTKEY  DISTKEY,
        title      VARCHAR (500)   NOT NULL,
        artist_id  VARCHAR(20)     NOT NULL,
        year       INTEGER         NOT NULL,
        duration   DECIMAL         NOT NULL
        )
""")

    artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id    VARCHAR(20)     PRIMARY KEY SORTKEY,
        name         VARCHAR (500)   NOT NULL,
        location     VARCHAR (500),
        latitude     DOUBLE PRECISION,
        longitude    DOUBLE PRECISION
        ) DISTSTYLE ALL;
""")

    time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY   SORTKEY,
        hour INTEGER      NOT NULL,
        day INTEGER       NOT NULL,
        week INTEGER      NOT NULL,
        month INTEGER     NOT NULL,
        year INTEGER      NOT NULL,
        weekday  INTEGER  NOT NULL
        ) DISTSTYLE ALL;
""")
    songplay_table_insert = ("""
        INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 second', e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
        FROM staging_events e
        JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration
        WHERE e.page = 'NextSong'
    """)

    user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplay
    """)