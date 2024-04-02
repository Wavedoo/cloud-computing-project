# environment variable setup for private key file
import os
import pandas as pd   #pip install pandas  ##to install

from google.cloud import pubsub_v1    #pip install google-cloud-pubsub  ##to install
import time
import json;
import io;
import random

# TODO : fill the credential json and project id 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./data/project.json"
project_id = "windy-album-413500"
topic_id = "project_requests"


publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


for i in range(20):
    message = {}
    message['second'] = random.randint(1, 900)
    message['track'] = random.randint(1,2) #Only first two are implemented rn
    future = publisher.publish(topic_path, json.dumps(message).encode('utf-8'))
    print("Message for " +str(message['track']) + " second "+str(message["second"])+" is sent")
    time.sleep(1)