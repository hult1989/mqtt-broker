import sys
import os
import re
import paho.mqtt.client as mqtt
from threading import Thread
from random import randint

def on_connect(client, userdata, rc):
    print "connected with result code ", rc
    client.subscribe(client.userId, 1)

def on_disconnect(client, userdata, rc):
    print "receive DISCONNECT from server"
    sys.exit(0)
    
def upgrade(client):
    def process(line):
        if line[0] == 'p':
            t, p = line[2:].split(',')
            print '[{}] publish "{}" on "{}"'.format(client.userId, p, t)
            client.publish(t.strip(), payload=p.strip(), qos=1)
        elif line[0] == 's':
            topic = line[2:].strip()
            client.subscribe(topic, qos=1)
            print '[{}] subscribe "{}"'.format(client.userId, topic)
    return process


def read2q(client):
    processor = upgrade(client)
    while True:
        inputs = raw_input('Input Message Action: ')
        for line in inputs.splitlines():
            if line.find('exit') != -1:
                os._exit(1)
            elif line[0] not in {'p', 's'}:
                print 'Invalid Input'
            else:
                try:
                    processor(line)
                except:
                    print 'Invalid Input'




def on_message(client, userdata, msg):
    print '{} RECEIVE [{}: {}]'.format(client._client_id, msg.mid, msg.payload)

def init_client(host):
    #userId = str(randint(10000, 100000))
    userId = 'alex'
    client = mqtt.Client(client_id=userId, clean_session=True)
    client.userId = userId
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.connect(host, 1883, 60)
    t = Thread(target=read2q, args=(client,))
    t.start()
    client.loop_forever()


if __name__ == '__main__':
    host = 'localhost'
    for i in xrange(1):
        t = Thread(target=init_client, args=(host,))
        t.start()
    '''
        host = sys.argv[1]
        print host
        init_client(host)
    '''
