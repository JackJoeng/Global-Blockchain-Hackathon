import array
import time
import socket
import boto3
import hashlib
import sys
from select import select
import re
import logging
from threading import Thread
import signal
from base64 import b64encode
from AWSIoTPythonSDK.MQTTLib import AWSIotMQTTClient as mqtt

db = boto3.client('dynamodb')
tableName = 'transactionDB'

s3 = boto3.resource('s3')
mqttHost = 'enter mqtt endpoint'
mqttTopic = 'posTopic'

mqttClient = mqtt(mqttTopic)
mqttClient.configureEndpoint(mqttHost, 8883)
mqttClient.configureCredentials("root-CA.crt", 'private key goes here', 'cert.pem goes here')
mqttClient.configureAutoReconnectBackoffTime(1, 32, 20)
mqttClient.configureConnectDisconnectTimeout(10) #in seconds
mqttClient.configureMQTTOperationTimeout(5) #in seconds
mqttClient.configureDrainingFrequency(2) #in Hz


class WebSocket(object):
    handshake = (
        "HTTP/1.1 101 Web Socket Protocol Handshake\r\n"
        "Upgrade: WebSocket\r\n"
        "Connection: Upgrade\r\n"
        "WebSocket-Origin: %(origin)s\r\n"
        "WebSocket-Location: ws://%(bind)s:%(port)s/\r\n"
        "Sec-Websocket-Accept: %(accept)s\r\n"
        "Sec-Websocket-Origin: %(origin)s\r\n"
        "Sec-Websocket-Location: ws://%(bind)s:%(port)s/\r\n"
        "\r\n"
    )
    def __init__(self, client, server):
        self.client = client
        self.server = server
        self.handshakeDone = False
        self.header = ""
        self.data = ""

    def feed(self, data):
        if not self.handshakeDone:
            self.header += str(data)
            if self.header.find('\\r\\n\\r\\n') != -1:
                parts = self.header.split('\\r\\n\\r\\n', 1)
                self.header = parts[0]
                if self.dohandshake(self.header, parts[1]):
                    logging.info("Handshake successful")
                    self.handshakeDone = True
        else:
            self.data += data.decode("UTF-8", "ignore")
            playloadData = data[6:]
            mask = data[2:6]
            unmasked = array.array("B", playloadData)
            for i in range(len(playloadData)):
                unmasked[i] = unmasked[i] ^ mask[i % 4]
            self.onmessage(bytes(unmasked).decode("UTF-8", "ignore"))

    def dohandshake(self, header, key=None):
        logging.debug("Initiating handshake with server: %s" % header)
        digitRe = re.compile(r'[^0-9]')
        spacesRe = re.compile(r'\s')
        part = part_1 = part_2 = origin = None
        for line in header.split('\\r\\n')[1:]:
            name, value = line.split(': ', 1)
            if name.lower() == "sec-websocket-key1":
                key_number_1 = int(digitRe.sub('', value))
                spaces_1 = len(spacesRe.findall(value))
                if spaces_1 == 0:
                    return False
                if key_number_1 % spaces_1 != 0:
                    return False
                part_1 = key_number_1 / spaces_1
            elif name.lower() == "sec-websocket-key2":
                key_number_2 = int(digitRe.sub('', value))
                spaces_2 = len(spacesRe.findall(value))
                if spaces_2 == 0:
                    return False
                if key_number_2 % spaces_2 != 0:
                    return False
                part_2 = key_number_2 / spaces_2
            elif name.lower() == "sec-websocket-key":
                part = bytes(value, 'UTF-8')
            elif name.lower() == "origin":
                origin = value
        if part:
            sha1 = hashlib.sha1()
            sha1.update(part)
            sha1.update("258EAFA5-E914-47DA-95CA-C5AB0DC85B11".encode('UTF-8'))
            accept = (b64encode(sha1.digest())).decode("UTF-8", "ignore")
            handshake = WebSocket.handshake % {
                'accept': accept,
                'origin': origin,
                'port': self.server.port,
                'bind': self.server.bind
            }

        else:
            logging.warning("Not using challenge + response")
            handshake = WebSocket.handshake % {
                'origin': origin,
                'port': self.server.port,
                'bind': self.server.bind
            }
        logging.debug("Handshake generated, initiating with client: %s" % handshake)
        self.client.send(bytes(handshake, 'UTF-8'))
        return True

    def onmessage(self, data):
        logging.info("Data received: %s" % data)
        #parse data here
        #commit to blockchain on success
        

    def send(self, data):
        logging.info("Data sent: %s" % data)
        self.client.send("\x00%s\xff" % data)

    def close(self):
        self.client.close()

class WebSocketServer(object):
    def __init__(self, bind, port, cls):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((bind, port))
        self.bind = bind
        self.port = port
        self.cls = cls
        self.connections = {}
        self.listeners = [self.socket]

    def listen(self, backlog=1):
        self.socket.listen(backlog)
        logging.info("Listening: %s" % self.port)
        self.running = True
        while self.running:
            rList, wList, xList = select(self.listeners, [], self.listeners, 1)
            for ready in rList:
                if ready == self.socket:
                    logging.debug("Client Connected")
                    client, address = self.socket.accept()
                    fileno = client.fileno()
                    self.listeners.append(fileno)
                    self.connections[fileno] = self.cls(client, self)
                else:
                    logging.debug("Client ready %s" % ready)
                    client = self.connections[ready].client
                    data = client.recv(1024)
                    fileno = client.fileno()
                    if data:
                        self.connections[fileno].feed(data)
                    else:
                        logging.debug("Ending connection")
                        self.connections[fileno].close()
                        del self.connections[fileno]
                        self.listeners.remove(ready)
            for failed in xList:
                if failed == self.socket:
                    logging.error("Socket broken!")
                    for fileno, conn in self.connections:
                        conn.close()
                    self.running = False

def mqttCallback(client, userdata, data):
    logging.info("Data from IoT device: " + data)
    payload = message.payload
    restData = json.loads(payload)
    #parse data here

    success = False
    if restData[1]['transaction']['status'] == 'success':
        success == True

    bankId = restData[0]['id']
    bank = restData[0]['bank']
    otherAccId = restData[0]['other_account']['id']
    otherAccMeta = restData[0]['other_account']['metadata']
    transactionType = restData[1]['transaction']['type']
    transactionAmount = restData[1]['transaction']['amount']
    transactionCurrency = restData[1]['transaction']['currency']

    
    photoURL = restData[1]['transaction']['details']['photo']
    comment = restData[1]['transaction']['details']['comment']

    time = time.now()

    new item = db.put_itm(
        TableName = tableName,
        Item = {
            'bankId':{'S':bankId},
            'bank':{'S':bank},
            'recipient':{'S':otherAccId},
            'recipientDetails':{'S':otherAccMeta},
            'amount':{'N':transactionAmount},
            'currency':{'S':currency},
            'type':{'S':transactionType},
            'status':{'B':success},
            'photo':{'S':photoURL},
            'details':{'S':comment}
            'time':{'S':time}
        }
    )

    #commit data to blockchain


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    mqttClient.connect()
    mqttClient.subscri(mqttTopic, 1, mqttCallback)

    server = WebSocketServer("localhost", 8888, WebSocket)
    server_thread = Thread(target=server.listen, args=[5])
    server_thread.start()
    # Add SIGINT handler for killing the threads
    def signal_handler(signal, frame):
        logging.info("Shutting down...")
        server.running = False
        sys.exit()
    signal.signal(signal.SIGINT, signal_handler)
    while True:
        time.sleep(100)