import psycopg2
import time
import traceback
conn = psycopg2.connect(
    host="localhost",
    database="amandb",
    user="aman",
    password="12345"
)
cur = conn.cursor()
print("connection established")


def merge_final():
    try:
        print("Inserting unique records")
        cur.execute("""
    INSERT INTO merge_final(id, name, subject, marks, tenth_per, twelth_per,
    UG_per, PG_per, is_active, is_deleted)
    SELECT COALESCE(f.id, d.id) AS id, f.name, f.subject, f.marks, d.tenth_per, d.twelth_per,
    d.UG_per,d.PG_per,
    CASE
        WHEN f.is_active = 1 AND d.is_active = 1 THEN 1
		WHEN f.is_active =0 AND d.is_active = 0 THEN 0
        ELSE 1
    END AS is_active,
    CASE
        WHEN f.is_deleted = 1 AND d.is_deleted = 1 THEN 1
		WHEN f.is_deleted = 0 AND d.is_deleted = 0 THEN 0
        ELSE 0
    END AS is_deleted
    FROM final1 AS f
    FULL JOIN final2 AS d ON f.id = d.id
    WHERE COALESCE(f.id, d.id) NOT IN (SELECT id FROM merge_final);""")
        
        print("updating records")
        cur.execute("""UPDATE merge_final m
        SET name = f.name, subject = f.subject, marks=f.marks, tenth_per = d.tenth_per,
        twelth_per=d.twelth_per, PG_per=d.PG_per, UG_per=d.UG_per, updated = now(),
        is_active = CASE
        WHEN f.is_active = 1 AND d.is_active = 1 THEN 1
        WHEN f.is_active =0 AND d.is_active = 0 THEN 0
        ELSE 1
        END,
        is_deleted = CASE
        WHEN f.is_deleted = 1 AND d.is_deleted = 1 THEN 1 
        WHEN f.is_deleted = 0 AND d.is_deleted = 0 THEN 0 
        ELSE 0
        END
        FROM final1 AS f
        FULL JOIN final2 AS d ON f.id=d.id
        WHERE COALESCE(f.id, d.id) = m.id AND (m.name<>f.name OR m.marks<>f.marks OR 
            m.subject <> f.subject OR m.tenth_per <> d.tenth_per OR 
            m.twelth_per <> d.twelth_per OR m.PG_per <> d.PG_per OR m.UG_per <> d.UG_per);
                    """)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(e)
        conn.commit()
        conn.close()
        return e
print(merge_final())