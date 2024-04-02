# GUI Imports
import time
import threading
from tkinter import *
import json;
import io;
import warnings
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

# Global Variables
canvas = None
outputLabel1 = None
outputLabel2 = None
window = None
trackField = None
timeField = None

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    data = json.loads(message.data)
    print(f"Received {data}.")
    window.after(0, lambda: changeOutputLabelText(str(data['east']), str(data['west'])))
    message.ack()

def WaitForInformation():
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    with subscriber:
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel(await_msg_callbacks=True)  # blocks until done

def changeOutputLabelText(newOutputLabel1Text, newOutputLabel2Text):
    # Reference Global Variables
    global outputLabel1, outputLabel2

    canvas.itemconfig(outputLabel1, text=newOutputLabel1Text, fill="black")  # Change text to the selected director
    canvas.itemconfig(outputLabel2, text=newOutputLabel2Text, fill="black")  # Change text to the selected director

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

    # WaitForInformation()
    # streaming_pull_future2 = subscriber.subscribe(subscription_path, callback=callback)
    # with subscriber:
    #     try:
    #         streaming_pull_future2.result()
    #     except RuntimeError:
    #         warnings.warn(
    #             "Scheduling a callback after executor shutdown.",
    #            category=RuntimeWarning,                
    #            stacklevel=2,
    #         )
        

    # streaming_pull_future2 = subscriber.subscribe(subscription_path, callback=callback)
    # with subscriber:
    #     try:
    #         streaming_pull_future2.result()
    #     except KeyboardInterrupt:
    #         streaming_pull_future2.cancel(await_msg_callbacks=True)  # blocks until done



def createWindow():
    # Reference Global Variables
    global canvas
    global outputLabel1
    global outputLabel2
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
    # --Background Image
    backgroundImage = PhotoImage(file='Background.png')  # Initializing the background image to use
    canvas.create_image(250, 350, image=backgroundImage)  # Centering the background image in the window

    # --Track Field
    trackField = Entry(window, width=50)  # Creating the link field where the user will enter the track number
    canvas.create_text(250, 170, text="Enter Track: ",
                       font=('Arial bold', 15))  # Text label above the link field

    # --Time Field
    timeField = Entry(window, width=50)  # Creating the link field where the user will enter the track number
    canvas.create_text(250, 280, text="Enter Time: ",
                       font=('Arial bold', 15))  # Text label above the link field

    # --Preview Button
    submitButtonImage = PhotoImage(file="SubmitButton.png")
    submitButton = Button(window, image=submitButtonImage, command=submitInformation,
                          borderwidth=0)  # Creating the submit button which users click to send the information

    outputLabel1 = canvas.create_text(250, 500, text="",
                                      font=('Arial bold', 15))  # Text label above the select button

    outputLabel2 = canvas.create_text(250, 560, text="",
                                      font=('Arial bold', 15))  # Text label above the select button

    # --Adding GUI Elements to the Window
    canvas.create_window(250, 220, window=trackField)  # Centering the link field in the window
    canvas.create_window(250, 330, window=timeField)  # Centering the link field in the window
    canvas.create_window(250, 400, window=submitButton)  # Placing the preview button beside the link field

    window.mainloop()  # Execute window


uiThread = threading.Thread(target=createWindow)
uiThread.start()
streaming_pull_future2 = subscriber.subscribe(subscription_path, callback=callback)
with subscriber:
    try:
        streaming_pull_future2.result()
    except KeyboardInterrupt:
        streaming_pull_future2.cancel(await_msg_callbacks=True)  # blocks until done

