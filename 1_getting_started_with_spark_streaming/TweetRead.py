import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json


consumer_key='AYYI7CvstplEi3fBAJ24vLVBA'
consumer_secret='OGFsmodX5DnHBcmZA3OrwFIeXS0gSUfPXcZVGTUOdffItb5Z0N'
access_token ='2988835149-OgWXTLsBiosW74ZJi563l64WQ8f6tNudQKpLudp'
access_secret='eOpeUX1wqL5sq9UK7yFbOPZ7ydYRZVqJ2Q2W7w3b1si7V'


class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads( data )
          print( msg['text'].encode('utf-8') )
          self.client_socket.send( msg['text'].encode('utf-8') )
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
  auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
  auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['soccer'])

if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "127.0.0.1"     # Get local machine name
  port = 5555                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print( "Received request from: " + str( addr ) )

  sendData( c )
