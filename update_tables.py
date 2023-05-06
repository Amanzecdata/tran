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
    print("Copy records to initial table")
    cur.execute("COPY pginit FROM '/home/hp/Downloads/third.csv' DELIMITER ',' CSV HEADER;")

    print("Inserting only new records to pgmiddle table")
    cur.execute("""
    INSERT INTO pgmiddle (id, name, subject, marks, created, updated, is_active, is_deleted)
    SELECT s.id, s.name, s.subject, s.marks, now(), now(), 1, 0
    FROM pginit s
    LEFT JOIN pgmiddle t
    ON s.id = t.id
    WHERE t.id IS NULL;
    """)

    print("Inserting updated records in pgmiddle table")
    cur.execute("""
    INSERT INTO pgmiddle (id, name, subject, marks, created, updated, is_active, is_deleted)
    SELECT s.id, s.name, s.subject, s.marks, now(), now(), 1, 0
    FROM pginit s
    LEFT JOIN pgmiddle t
    ON s.id = t.id
    WHERE t.id = s.id AND 
    (t.name <> s.name OR t.subject <> s.subject OR t.marks <> s.marks)
    ANd t.created >= (SELECT MAX(created) FROM pgmiddle WHERE t.id = pgmiddle.id);
    """)

    print("------- updating time ------")   
    cur.execute("""UPDATE pgmiddle
    SET is_active=0, updated = now()
    FROM pginit s
    WHERE pgmiddle.id=s.id and (s.name <> pgmiddle.name OR s.subject <> pgmiddle.subject 
    OR s.marks <> pgmiddle.marks ) and is_active = 1;""")

    print("""Updating values for is_active and is_deleted fields in pgmiddle table 
             if record is deleted""")
    cur.execute("""
    UPDATE pgmiddle
    SET is_deleted=1, is_active=0, updated = now()
    WHERE NOT EXISTS (SELECT 1 FROM pginit WHERE pgmiddle.id = pginit.id  );
    """)

    print("Inserting unique records into pgfinal")
    cur.execute("""
    INSERT INTO pgfinal SELECT s.* FROM pginit s 
    LEFT JOIN pgfinal f 
    on s.id = f.id
    WHERE f.id IS NULL;
    """)

    cur.execute("""UPDATE pgfinal
    SET updated = now(),
    name = s.name, subject = s.subject, marks = s.marks, is_active = 1
    FROM pginit s
    WHERE s.id = pgfinal.id
    and (s.name<>pgfinal.name or s.subject<>pgfinal.subject or s.marks<>pgfinal.marks);""")

    cur.execute("""UPDATE pgfinal
    SET is_deleted = 1, is_active = 0, updated = now()
    WHERE NOT EXISTS (SELECT 1 FROM pginit WHERE pgfinal.id = pginit.id);""")

    print("truncating the pginit table")
    cur.execute(""" TRUNCATE TABLE pginit; """)
    conn.commit()
    cur.close()
    conn.close()
    return True
print(query_handler())

#___________________________ other logics _________________________________


    # cur.execute("""
    # UPDATE pgmiddle
    # SET updated = (SELECT MAX(created) FROM pgmiddle WHERE pgmiddle.id = pgmiddle.id)
    # WHERE pgmiddle.id=pgmiddle.id AND pgmiddle.created = pgmiddle.updated;
    # """)

    # cur.execute(""" 
    # UPDATE pgmiddle 
    # SET is_active = CASE 
    #     WHEN created = (SELECT MAX(created) FROM pgmiddle t WHERE t.id = pgmiddle.id)
    #         THEN 1 
    #         ELSE 0 
    #     END;
    # """)