#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API 
access_token = "3198614666-wTNzyI5lmDTbHz2JdI0ccoN5M6mRTgcWiH5UHgz"
access_token_secret = "WX4ykGMbHLx4Ti5UpeP0EwNXwgnyem5vUfyYO7KhHn1IK"
consumer_key = "XEKwwGtXNByzHv44Qvg66aJxN"
consumer_secret = "FZ62zqnW5erG9aNnC3FRLRkxu31jLzvedttUKiHK9aDNrYBe1K"


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print data
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['python', 'javascript', 'ruby'])
