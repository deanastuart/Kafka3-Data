from kafka import KafkaConsumer, TopicPartition
from json import loads
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from statistics import mean

Base = declarative_base()


class Transaction(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        self.deposit =[]
        self.withdrawal = []
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
    def meand(self):
        try:
            return round(sum(self.deposit)/len(self.deposit),2)
        except ZeroDivisionError:
            return 0

    def meanw(self):
        try:
            return round(sum(self.withdrawal)/len(self.withdrawal), 2)
        except ZeroDivisionError:
            return 0

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
                self.deposit.append(message['amt'])
            else:
                self.custBalances[message['custid']] -= message['amt']
                self.withdrawal.append(message['amt'])
            print(self.custBalances)
            print("Average deposit amount: " + str(XactionConsumer.meand(self)))
            print("Average withdrawal amount: " + str(XactionConsumer.meanw(self)))

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()