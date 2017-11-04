ACCESS_TOKEN = '2988835149-OgWXTLsBiosW74ZJi563l64WQ8f6tNudQKpLudp'
ACCESS_SECRET = 'eOpeUX1wqL5sq9UK7yFbOPZ7ydYRZVqJ2Q2W7w3b1si7V'
CONSUMER_KEY = 'AYYI7CvstplEi3fBAJ24vLVBA'
CONSUMER_SECRET = 'OGFsmodX5DnHBcmZA3OrwFIeXS0gSUfPXcZVGTUOdffItb5Z0N'


import socket
import sys
import requests
import requests_oauthlib
import json


my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            tcp_connection.send(tweet_text + '\n')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('locations', '-130,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp,conn)