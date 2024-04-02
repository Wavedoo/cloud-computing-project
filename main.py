# GUI Imports
import time
import threading
from tkinter import *
import json;
import io;
import os
from google.cloud import pubsub_v1      #pip install google-cloud-pubsub


# Global Variables
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./project.json"
project_id = "windy-album-413500"
topic_id = "project_requests"
subscription_id = "project_results-sub"
    
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

canvas = None
outputLabel = None
window = None
trackField = None
timeField = None

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    data = json.loads(message.data)
    window.after(0, lambda: changeOutputLabelText(data))
    message.ack()


def changeOutputLabelText(newOutputLabelText):
    # Reference Global Variables
    global outputLabel

    canvas.itemconfig(outputLabel, text=newOutputLabelText, fill="black")  # Change text to the selected director
    print("Changed Output Text")


def submitInformation():
    trackNumber = trackField.get()
    timeNumber = timeField.get()

    print("Submitted Track:", trackNumber)
    print("Submitted Time:", timeNumber)

    topic_path = publisher.topic_path(project_id, topic_id)
    message = {}
    message['second'] = min(int(timeNumber), 2) #Limits choices to 1 or 2
    message['track'] = min(int(trackNumber), 900) #Limits choices to 1-900
    future = publisher.publish(topic_path, json.dumps(message).encode('utf-8'))

    streaming_pull_future2 = subscriber.subscribe(subscription_path, callback=callback)
    with subscriber:
        try:
            streaming_pull_future2.result()
        except KeyboardInterrupt:
            streaming_pull_future2.cancel(await_msg_callbacks=True)  # blocks until done



def createWindow():
    # Reference Global Variables
    global canvas
    global outputLabel
    global window
    global trackField
    global timeField

    # Creating the GUI Window
    window = Tk()  # Creating the window object

    iconImage = ""  # The image displayed on the top left of the window select bar
    window.iconbitmap(iconImage)  # Display the icon image as the application's icon

    title = window.title(' Traffic Congestion App')  # Window title on the top left

    canvas = Canvas(window, width=500, height=700)  # Window size
    canvas.configure(background='pink')  # Background colour

    window.resizable(0, 0)  # Disable enlarging window
    canvas.pack()  # Applying the GUI window properties

    # Initializing GUI Elements
    # --Track Field
    trackField = Entry(window, width=50)  # Creating the link field where the user will enter the track number
    canvas.create_text(250, 170, text="Enter Track: ",
                       font=('Arial bold', 15))  # Text label above the link field

    # --Time Field
    timeField = Entry(window, width=50)  # Creating the link field where the user will enter the track number
    canvas.create_text(250, 280, text="Enter Time: ",
                       font=('Arial bold', 15))  # Text label above the link field

    # --Preview Button
    submitButtonImage = PhotoImage(file="")
    submitButton = Button(window, command=submitInformation,
                          borderwidth=20)  # Creating the submit button which users click to send the information

    outputLabel = canvas.create_text(250, 500, text="Output",
                                     font=('Arial bold', 15))  # Text label above
    # the select button

    # --Adding GUI Elements to the Window
    canvas.create_window(250, 220, window=trackField)  # Centering the link field in the window
    canvas.create_window(250, 330, window=timeField)  # Centering the link field in the window
    canvas.create_window(250, 400, window=submitButton)  # Placing the preview button beside the link field

    window.mainloop()  # Execute window


uiThread = threading.Thread(target=createWindow)
uiThread.start()

