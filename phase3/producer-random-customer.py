from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import date
import random

from datetime import date


first_names=('John','Andy','Joe','Mary','Denise','Bridget')
last_names=('Johnson','Smith','Williams','Gomez','Shang','Andrews')

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: dumps(m).encode('ascii'))

    def emit(self):
        data = {'custid' : random.randint(98,100),
            'createdate': int(date.today().strftime('%m%d%Y')),
            'fname': random.choice(first_names),
            'lname': random.choice(last_names)
            }
        return data

    def generateRandomXactions(self, n=1000):
        for _ in range(n):
            data = self.emit()
            print('sent', data)
            self.producer.send('bank-customer-new', value=data)
            sleep(1)

if __name__ == "__main__":
    p = Producer()
    p.generateRandomXactions(n=5)
