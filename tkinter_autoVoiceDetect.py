import speech_recognition as sr
import openai
import tkinter as tk
import threading

openai.api_key = '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'
messages = [
    {"role": "system", "content": "You are a helpful assistant."},
]
recording = True  # Variable to track recording status
def ai_reply(message):
    reply = ""
    print("ans")
    try:
        if message:
            messages.append({"role": "user", "content": message})
            chat = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=messages)
        if chat:
            print("found ans")
            reply = chat.choices[0].message.content
            messages.append({"role": "assistant", "content": reply})
    except Exception as err:
        print("error : ", err)
    return reply

def listen_convert():
    global recording
    r = sr.Recognizer()
    with sr.Microphone() as source:
        r.adjust_for_ambient_noise(source)
        print("Listening...")
        all_text = " "
        while recording:
            try:
                audio = r.listen(source, phrase_time_limit=10)
                if audio:
                    text = r.recognize_google(audio) + " "
                    all_text = all_text + text

            except sr.UnknownValueError:
                print("Unable to recognize speech")
                listen_convert()
            except sr.RequestError as e:
                print("Error occurred:", str(e))
                listen_convert()
            if not recording:
                break
        if not recording:
            text_entry.insert(tk.END,"Question : "+ all_text + "\n\n", "user")
            answer = ai_reply(all_text)
            text_entry.insert(tk.END,"Answer : "+ answer + "\n\n", "assistant")
            text_entry.see(tk.END)

def start_recording():
    global recording
    recording = True
    threading.Thread(target=listen_convert).start()

def stop_recording():
    global recording
    recording = False

def exit_program():     
    stop_recording()
    window.destroy()

window = tk.Tk()
text_entry = tk.Text(window, height=30, width=110, wrap=tk.WORD)
text_entry.tag_config("user", foreground="red")
text_entry.tag_config("assistant", foreground="green")
text_entry.pack()

start_button = tk.Button(window, text="Start Recording", command=start_recording)
start_button.pack()

convert_button = tk.Button(window, text="Convert Text", command=stop_recording)
convert_button.pack()

exit_button = tk.Button(window, text="Exit", command=exit_program)
exit_button.pack()

window.mainloop()
