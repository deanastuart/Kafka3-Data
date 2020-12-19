from kafka import KafkaConsumer, TopicPartition
from kafka.structs import TopicPartition
from json import loads
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

password = os.getenv('MYSQLPW')
engine = create_engine('mysql+mysqlconnector://root:' + password + '@localhost/zipbank')
Base = declarative_base()


class Transaction(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    bankid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        self.consumer.assign([TopicPartition('bank-customer-events', 1)])
        self.ledger = {}
        self.custBalances = {}


    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)

            record = Transaction(custid=message['custid'], type=message['type'], bankid=message['bankid'], date=message['date'],
                                 amt=message['amt'])
            Session = sessionmaker()
            Session.configure(bind=engine)
            session = Session()
            session.add(record)
            session.commit()

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()


