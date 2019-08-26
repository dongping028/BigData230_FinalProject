import json
import requests

from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers='localhost:9092')


r = requests.get('http://stream.meetup.com/2/rsvps', stream=True)

for line in r.iter_lines():
    # filter out keep-alive new lines
    if line:
        try:
            decoded_line = line.decode('utf-8')
            record = json.loads(decoded_line)
            group_topics = []
            for topic in record["group"]["group_topics"]:
                group_topics.append(topic["topic_name"])
 
            message = {
                "guests" : record["guests"],
                "group_topics" : group_topics,
                "event_name" : record["event"]["event_name"],
                "group_city" : record["group"]["group_city"],
                "group_country" : record["group"]["group_country"]
            }           
            print (json.dumps(message))
            
            producer.send('meetup', key=bytes("message", encoding='utf-8'), value=bytes(json.dumps(message), encoding='utf-8'))
        except:
            continue