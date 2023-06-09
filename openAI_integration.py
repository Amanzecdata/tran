import openai
import psycopg2

conn = psycopg2.connect(
    host='localhost',
    user='aman',
    password='12345',
    database='amandb',
    port=5432
)
cur = conn.cursor()
create_table_query = '''
    CREATE TABLE IF NOT EXISTS chat_history (
    id serial PRIMARY KEY,
    Role VARCHAR(255),
    Question TEXT,
    Answer TEXT
)
'''
insert_query = '''
        INSERT INTO chat_history (role, question, answer)
        VALUES (%s, %s, %s)
        '''
cur.execute(create_table_query)

openai.api_key = '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'

messages = [
    {"role": "system", "content":"You are a helpful assistance."},
]

while True:
    try:
        message = input("User : ")
        if message:
            if message.lower() in ["quit", "bye", "exit"]:
                print("Ram Ram DADA!!")
                break
            messages.append(
                {"role": "user", "content":message},
            )
            chat = openai.ChatCompletion.create(
                model = "gpt-3.5-turbo",messages=messages
            )
        if chat:
            reply = chat.choices[0].message.content
            print(f"ChatGpt : {reply}")
            Role=messages[-1]['role']
            Question=messages[-1]['content']
            Answer=reply
            messages.append({"role":"assistant", "content":reply})
            cur.execute(insert_query,(Role,Question,Answer))
            conn.commit()
    except Exception as err:     
        print(err)
        conn.commit()
        break
conn.close()
cur.close()

