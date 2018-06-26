# Databricks notebook source
import requests
import json
import urllib2
import base64
import time
from operator import itemgetter
import unicodedata
#from azure.eventhub import EventHubClient, Sender, EventData
from kafka import KafkaProducer

def getEventHubToken(sb_name, eh_name, sas_name, sas_value):
    """
    Returns an authorization token dictionary 
    for making calls to Event Hubs REST API.
    """
    uri = urllib.quote_plus("https://{}.servicebus.windows.net/{}" \
                                  .format(sb_name, eh_name))
    sas = sas_value.encode('utf-8')
    expiry = str(int(time.time() + 10000))
    string_to_sign = (uri + '\n' + expiry).encode('utf-8')
    signed_hmac_sha256 = hmac.HMAC(sas, string_to_sign, hashlib.sha256)
    signature = urllib.quote(base64.b64encode(signed_hmac_sha256.digest()))
    return  {"sb_name": sb_name,
             "eh_name": eh_name,
             "token":'SharedAccessSignature sr={}&sig={}&se={}&skn={}' \
                     .format(uri, signature, expiry, sas_name)
            }
  
def getTwitterToken(encodedKey):
  url = 'https://api.twitter.com/oauth2/token?grant_type=client_credentials'
  payload = {}
  headers = {
      "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
      "Authorization": "Basic " + encodedKey
  }
  r = requests.post(url, data=json.dumps(payload), headers=headers)
  jdata = json.loads(r.content) 
  token = jdata['access_token']
  return token

def getTwitterTweets(bearerToken, sinceID, searchString):
  #Limit is 450 every 15 min window
  url = 'https://api.twitter.com/1.1/search/tweets.json?q={0}&result_type=recent&count=1&lang=en&since_id={1}'.format(searchString,str(sinceID))
  print("URL: {0}".format(url))
  req = urllib2.Request(url)
  req.add_header('Authorization', 'Bearer {0}'.format(bearerToken))
  resp = urllib2.urlopen(req)
  content = resp.read()
  #searchCount += 1
  #print ("Search Count: {0}".format(str(searchCount)))
  jdata = json.loads(content) 
  return jdata

def removeControlCharacters(s):
    return "".join(ch for ch in s if unicodedata.category(ch)[0]!="C")

def getTweetsJson(tweets): 
  iteratorMaxTweetID = 0
  outputArray = ""
  tweetArray = tweets['statuses']
  if tweetArray is not None:
    for tweet in tweetArray:
      user = tweet['user']
      jsonString = '{ "id" : "'+ str(tweet['id_str']) +'", "createdat" : "'+ str(tweet['created_at']) +'", "text" : "'+ removeControlCharacters(tweet['text']).encode("utf8").replace('"','') +'", "source" : "'+ str(tweet['source']).replace('"','') +'", "favorited" : "'+ str(tweet['favorited']) +'", "retweeted" : "'+ str(tweet['retweeted']) +'", "coordinates" : "'+ str(tweet['coordinates']) +'", "place" : "'+ str(tweet['place']) +'", "userscreenname" : "'+ str(user['screen_name']) +'", "userfollowers" : "'+ str(user['followers_count']) + '" },'
      if jsonString is not None:
        outputArray += jsonString
        if tweet['id_str'] > iteratorMaxTweetID:
          iteratorMaxTweetID = tweet['id_str']
  if len(outputArray) > 1:
    outputArray = outputArray[:-1] + ''
  else:
    outputArray = outputArray + ''
  try:   
    print("Record Count: {0}".format(len(tweetArray)))
    print("Max ID: {0}".format(str(iteratorMaxTweetID)))
    return([outputArray, iteratorMaxTweetID])
  except Exception as e:
    print(outputArray)
    print(e)

def sendTwitterTweetsToEventHub(eventHubtoken, body):
  #Limit is 450 every 15 min window
  url = 'https://marslandingdeveventhubs.servicebus.windows.net/marslandingtwitterpoc/messages'
  req = urllib2.Request(url, body)
  req.add_header('Authorization', eventHubtoken)
  req.add_header('Host', "marslandingdeveventhubs.servicebus.windows.net")
  req.add_header('Content-Type', "application/vnvd.microsoft.servicebus.json")
  resp = urllib2.urlopen(req) 
  return resp

def sendTwitterTweetsToKafka(body):
  producer = KafkaProducer(bootstrap_servers="10.0.0.9:9092, 10.0.0.12:9092, 10.0.0.6:9092, 10.0.0.14:9092")
  resp = producer.send('marsstreaming', body)
  producer.flush()
  return resp


#### Start of Code ####

## Parameters ##
consumerKey = "1Q3aLRMy32GdozJzYTmcvfw30"
consumerSecret = "dNljVtF5cODM6YKNwkRZfqgWYblFbQX8mf0hc8dSnHEwzzA1mr"
searchString = "chocolate"
refreshSeconds = 10

## Variables ##
maxID = 0
#searchCount = 0

## Process ##
encodedKey = base64.b64encode(consumerKey+":"+consumerSecret)
print(encodedKey)
twitterToken = getTwitterToken(encodedKey)
eventHubtoken = getEventHubToken("marslandingdeveventhubs","marslandingtwitterpoc","RootManageSharedAccessKey","bcE/RVn2XlDnvRlcdTpMDSmkvuNVFlWvrVtRHSvgWqs=")['token']

# Run for 15 minutes
t_end = time.time() + 60 * 15
print("Run Until: {0}".format(str(t_end)))

while time.time() < t_end:
  tweets = getTwitterTweets(twitterToken, maxID, searchString)
  getTweetsResponse = getTweetsJson(tweets)
  tweetsJson = getTweetsResponse[0]
  if len(tweetsJson) > 2:
    #maxID = max(tweetsJson, key=itemgetter('id'))['id']
    maxID = getTweetsResponse[1]
    print(tweetsJson)
    sendTwitterTweetsToEventHub(eventHubtoken,tweetsJson)
    sendTwitterTweetsToKafka(tweetsJson)
  time.sleep(refreshSeconds)


# COMMAND ----------

import requests
import json
import urllib2
import base64
import time
import unicodedata

# Variables that contains the user credentials to access Twitter API 
TWITTER_KEY = '1335260851-bSpdodJhvxALcCjpyEaLp4kLwYbuYvV7xMM0qhB'
TWITTER_SECRET = 'wOqTLUrF0eMHTKN3Sci2mxIqFxe4rxEFZdEIvMii0hcsh'
TWITTER_APP_KEY = 'MgCUd4kRYeqeE9Sp2kug7FvaT'
TWITTER_APP_SECRET = 'lJAGyotNZKBwfzcekS7zdNMG5Ovp8GtVuFhfYI3eWZHqMabm0a'

def getEventHubToken(sb_name, eh_name, sas_name, sas_value):
    uri = urllib.quote_plus("https://{}.servicebus.windows.net/{}".format(sb_name, eh_name))
    sas = sas_value.encode('utf-8')
    expiry = str(int(time.time() + 10000))
    string_to_sign = (uri + '\n' + expiry).encode('utf-8')
    signed_hmac_sha256 = hmac.HMAC(sas, string_to_sign, hashlib.sha256)
    signature = urllib.quote(base64.b64encode(signed_hmac_sha256.digest()))
    return  {"sb_name": sb_name,
             "eh_name": eh_name,
             "token":'SharedAccessSignature sr={}&sig={}&se={}&skn={}'.format(uri, signature, expiry, sas_name)
            }

def sendTwitterTweetsToEventHub(eventHubtoken, body):
  url = 'https://marslandingdeveventhubs.servicebus.windows.net/marslandingtwitterpoc/messages'
  req = urllib2.Request(url, body)
  req.add_header('Host', "marslandingdeveventhubs.servicebus.windows.net")
  req.add_header('Content-Type', "application/vnvd.microsoft.servicebus.json")
  try:
    req.add_header('Authorization', eventHubtoken)
    resp = urllib2.urlopen(req) 
  except:
    eventHubtoken = getEventHubToken("marslandingdeveventhubs","marslandingtwitterpoc","RootManageSharedAccessKey","bcE/RVn2XlDnvRlcdTpMDSmkvuNVFlWvrVtRHSvgWqs=")['token']
    req.add_header('Authorization', eventHubtoken)
    resp = urllib2.urlopen(req) 
  return resp

eventHubtoken = getEventHubToken("marslandingdeveventhubs","marslandingtwitterpoc","RootManageSharedAccessKey","bcE/RVn2XlDnvRlcdTpMDSmkvuNVFlWvrVtRHSvgWqs=")['token']

auth = tweepy.OAuthHandler(TWITTER_APP_KEY, TWITTER_APP_SECRET)
auth.set_access_token(TWITTER_KEY, TWITTER_SECRET)
api = tweepy.API(auth)

class StreamListener(tweepy.StreamListener):
    def on_status(self, status):        
        json_str = json.dumps(status._json)
        #print(json_str)
        print(status.lang + ' - ' + status.text) 
        sendTwitterTweetsToEventHub(eventHubtoken, json_str)
    def on_error(self, status_code):
       if status_code == 420:
            return False
          
stream_listener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
#stream.filter(track=["trump", "clinton", "hillary clinton", "donald trump"])
stream.filter(track=["marsbar", "snickers", "galaxychocolate", "m&ms"])