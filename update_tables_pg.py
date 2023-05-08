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
    print("Copy records to staging1 table")
    cur.execute("COPY staging1 FROM '/home/hp/Downloads/dim1.csv' DELIMITER ',' CSV HEADER;")

    print("Inserting into dim1")
    cur.execute("""INSERT INTO dim1 (id, name, subject, marks,  created, 
    updated, is_active, is_deleted)
    SELECT s.id, s.name, s.subject, s.marks, now(), now(), 1, 0 
    FROM staging1 s
    LEFT JOIN dim1 t
    ON t.id = s.id 
    WHERE t.id IS NULL;
    """)

    print("Inserting Updated records into dim1")
    cur.execute(""" INSERT INTO dim1 (id, name, subject, marks, created, 
    updated, is_active, is_deleted)
    SELECT s.id, s.name, s.subject, s.marks, now(), now(),1 ,0
    FROM staging1 s
    LEFT JOIN dim1 t
    ON t.id = s.id
    WHERE t.id = s.id AND 
    (t.name <> s.name OR t.subject <> s.subject OR t.marks <> s.marks) 
    AND t.updated > (SELECT MAX(created) FROM dim1 WHERE t.id = dim1.id);
    """)

    print(" updating updated time and is_active status in dim1 ")
    cur.execute(""" UPDATE dim1 
    SET is_active = 0, updated = now()
    FROM staging1 s
    WHERE s.id = dim1.id AND (dim1.name <> s.name OR 
    dim1.subject <> s.subject OR s.marks <> dim1.marks) 
    AND is_active = 1;
    """)
    
    print("Updating is_active and is_deleted for dim1")
    cur.execute("""
    UPDATE dim1
    SET is_active = 0, is_deleted = 1, updated = now()
    WHERE NOT EXISTS (SELECT 1 FROM staging1 WHERE staging1.id = dim1.id);
    """)

    print("Inserting new record into final from staging ")
    cur.execute(""" INSERT INTO final1 SELECT s.* FROM staging1 s
    LEFT JOIN final1 f 
    on s.id = f.id
    WHERE f.id IS NULL;
    """)

    print("Updating values for final if same record comes ")
    cur.execute(""" UPDATE final1 
    SET updated = now(), name = s.name, subject = s.subject, is_active =1 
    FROM staging1 s
    WHERE s.id = final1.id AND (s.name <> final1.name OR 
    s.subject <> final1.subject OR s.marks <> final1.marks);
    """)

    print("Updating values if a record is deleted ")
    cur.execute(""" UPDATE final1 
    SET is_active = 0, is_deleted = 1, updated = now()
    WHERE NOT EXISTS (SELECT 1 FROM staging1 WHERE final1.id = staging1.id);
    """)

    print("truncating pginit table")
    cur.execute(""" TRUNCATE TABLE staging1; """)
    conn.commit()
    cur.close()
    conn.close()
    return True
print(query_handler())

