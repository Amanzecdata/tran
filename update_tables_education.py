import psycopg2
conn = psycopg2.connect(
    host="localhost",
    database="amandb",
    user="aman",
    password="12345"
)
cur = conn.cursor()
print("connection established")
def query_handler():
    print("Copy records to staging table")
    cur.execute("COPY staging2 FROM '/home/hp/Downloads/dim2.csv' DELIMITER ',' CSV HEADER;")

    print("Inserting into dim2")
    cur.execute("""INSERT INTO dim2 (id, tenth_per, twelth_per, UG_per, PG_per, created, 
    updated, is_active, is_deleted)
    SELECT s.id, s.tenth_per, s.twelth_per, s.UG_per, s.PG_per, now(), now(), 1, 0 
    FROM staging2 s
    LEFT JOIN dim2 t
    ON t.id = s.id 
    WHERE t.id IS NULL;
    """)

    print("Inserting Updated records into dim2")
    cur.execute(""" INSERT INTO dim2 (id, tenth_per, twelth_per, UG_per, PG_per, created, 
    updated, is_active, is_deleted)
    SELECT s.id, s.tenth_per, s.twelth_per, s.UG_per, s.PG_per, now(), now(),1 ,0
    FROM staging2 s
    LEFT JOIN dim2 t
    ON t.id = s.id
    WHERE t.id = s.id AND 
    (t.tenth_per <> s.tenth_per OR t.twelth_per <> s.twelth_per OR t.UG_per <> s.UG_per OR 
    t.PG_per <> s.PG_per) 
    AND t.updated >= (SELECT MAX(created) FROM dim2 WHERE t.id = dim2.id)
    AND is_active = 1;
    """)

    print(" updating updated time and is-active status in dim2 ")
    cur.execute(""" UPDATE dim2 
    SET is_active = 0, updated = now()
    FROM staging2 s
    WHERE s.id = dim2.id AND (dim2.tenth_per <> s.tenth_per OR 
    dim2.twelth_per <> s.twelth_per OR s.UG_per <> dim2.UG_per OR dim2.PG_per <> s.pG_per) 
    AND is_active = 1;
    """)
    
    print("Updating is_active and is_deleted for dim2")
    cur.execute("""
    UPDATE dim2
    SET is_active = 0, is_deleted = 1, updated = now()
    WHERE NOT EXISTS (SELECT 1 FROM staging2 WHERE staging2.id = dim2.id);
    """)

    print(" Inserting new record into final from staging ")
    cur.execute(""" INSERT INTO final2 SELECT s.* FROM staging2 s
    LEFT JOIN final2 f 
    on s.id = f.id
    WHERE f.id IS NULL;
    """)

    print("Updating values for final if same record comes ")
    cur.execute(""" UPDATE final2 
    SET updated = now(), tenth_per = s.tenth_per, twelth_per = s.twelth_per, is_active =1 
    FROM staging2 s
    WHERE s.id = final2.id AND (s.tenth_per <> final2.tenth_per OR 
    s.twelth_per <> final2.twelth_per OR s.UG_per <> final2.UG_per 
    OR s.PG_per <> final2.PG_per);
    """)

    print("Updating values if a record is deleted ")
    cur.execute(""" UPDATE final2 
    SET is_active = 0, is_deleted = 1, updated = now()
    WHERE NOT EXISTS (SELECT 1 FROM staging2 WHERE final2.id = staging2.id);
    """)

    print("truncating pginit table")
    cur.execute(""" TRUNCATE TABLE staging2; """)
    conn.commit()
    cur.close()
    conn.close()
    return True
print(query_handler())

