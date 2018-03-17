#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

access_token = "3198614666-wTNzyI5lmDTbHz2JdI0ccoN5M6mRTgcWiH5UHgz"
access_token_secret =  "WX4ykGMbHLx4Ti5UpeP0EwNXwgnyem5vUfyYO7KhHn1IK"
consumer_key =  "XEKwwGtXNByzHv44Qvg66aJxN"
consumer_secret =  "FZ62zqnW5erG9aNnC3FRLRkxu31jLzvedttUKiHK9aDNrYBe1K"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("tweets", data.encode('utf-8'))
        print data
        #fh = open("tweets.json", "a")
        #fh.write(data)
        #fh.close
        return True
    def on_error(self, status):
        print status

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
# stream.filter(track=["Stephen Hawking"])
stream.filter(track=["Stormy Daniels"])
# stream.filter(track=["Xijinping"])
