import openai
import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root12345',
    database='amandb'
)
cur = conn.cursor()
create_table_query = '''
    CREATE TABLE IF NOT EXISTS chat_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
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
            messages.append(
                {"role": "user", "content":message},
            )
            chat = openai.ChatCompletion.create(
                model = "gpt-3.5-turbo", max_tokens = 100 , messages=messages
            )
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

