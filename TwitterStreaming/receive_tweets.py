import tweepy
from tweepy.auth import OAuthHandler
from tweepy import StreamingClient
from tweepy.streaming import StreamResponse
import socket
import json


# Twitter Consumer API keys
consumer_key    = "9riKrY4rn5mc8uuiE8v22YYvV"
consumer_secret = "NE4zoPkDVIihJxhxAglH0Axbb68sRsfm8Qhnzipb5QIA8lLUTb"

# Twitter Access token & access token secret
access_token    = "1789128264515993600-F7kc2il8nkJB6D3fH4aVTy6rerzTBR"
access_secret   = "mwbsVW6aHVS1AJEja6VfbYH0tMDDh1xbeW9nVun2uhdDK"

# we create this class that inherits from the StreamListener in tweepy StreamListener
class TweetsListener(StreamingClient):

    def __init__(self, csocket):
        self.client_socket = csocket
    # we override the on_data() function in StreamListener
    def on_data(self, data):
        try:
            print(data)
            message = json.loads( data )
            print( message['text'].encode('utf-8') )
            self.client_socket.send( message['text'].encode('utf-8') )
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            
        print("else block")
        return True

    def if_error(self, status):
        print(status)
        return True
    
def send_tweets(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    twitter_stream = StreamingClient("AAAAAAAAAAAAAAAAAAAAAK73tgEAAAAA5%2BJC7E3DuMSLZ7sKPTugBjqzeeI%3DPF9reD3K1hYBPGl9vN1aL9Tc1Z5rLP5cHSlB6YfhUUQP26RzmG")
    twitter_stream.add_rules(['India',"elections","cricket","football","coding"]) #we are interested in this topic.
        
if __name__ == "__main__":
    new_skt = socket.socket()         # initiate a socket object
    host = "localhost"     # local machine address
    port = 5001                 # specific port for your service.
    new_skt.bind((host, port))        # Binding host and port

    print("Now listening on port: %s" % str(port))

    new_skt.listen(5)                 #  waiting for client connection.
    c, addr = new_skt.accept()        # Establish connection with client. it returns first a socket object,c, and the address bound to the socket

    print("Received request from: " + str(addr))
    # and after accepting the connection, we aill sent the tweets through the socket
    send_tweets(c)