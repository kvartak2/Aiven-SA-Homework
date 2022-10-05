import json
import uuid
import random

from faker import Faker
from datetime import datetime
from kafka import KafkaProducer

SERICE_URI ="kafka-covid-test-stream-kaths2407-bdae.aivencloud.com:22250"
KAFKA_TOPIC = "covid-test-stream"

fake = Faker()

def get_gender():
    return random.choice(["M","F"])

def get_name(gender):
    if(gender=="M"):
        return fake.name_male()
    else:
        return fake.name_female()

def get_test_result():
    return random.choice(["Positive","Negative"])

def generate_data():
    gender = get_gender()
    covid_data = {
        "Name" : get_name(gender),
        "Gender" : gender,
        "Date of Birth" : str(fake.date_of_birth(maximum_age=90)),
        "Address" : fake.address(),
        "Email Id" : fake.email(),
        "Phone" : fake.numerify('##########'),
        "Test result" : get_test_result(),
        "Timestamp" : datetime.now().isoformat()
    }
    return covid_data

def encode_json(data):
    return json.dumps(data)

def main():
    while True:
        key = {
            "id": str(uuid.uuid4())
        }
        covid_data = generate_data()
        print(f'{json.dumps(key, indent=4)},{json.dumps(covid_data, indent=4)}')
        producer = KafkaProducer(
            bootstrap_servers=SERICE_URI,
            security_protocol="SSL",
            ssl_cafile="ca.pem",
            ssl_certfile="service.cert",
            ssl_keyfile="service.key",
            api_version=(2, 0, 2)
        )

        producer.send(KAFKA_TOPIC, key=json.dumps(key).encode('utf-8'),value=json.dumps(covid_data).encode('utf-8'))
        producer.flush()

if __name__ == "__main__":
    main()