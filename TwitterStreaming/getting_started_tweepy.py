import tweepy
# Twitter Consumer API keys
consumer_key    = "9riKrY4rn5mc8uuiE8v22YYvV"
consumer_secret = "NE4zoPkDVIihJxhxAglH0Axbb68sRsfm8Qhnzipb5QIA8lLUTb"

# Twitter Access token & access token secret
access_token    = "1789128264515993600-F7kc2il8nkJB6D3fH4aVTy6rerzTBR"
access_token_secret   = "mwbsVW6aHVS1AJEja6VfbYH0tMDDh1xbeW9nVun2uhdDK"

auth = tweepy.OAuth1UserHandler(
    consumer_key, consumer_secret, access_token, access_token_secret
)

api = tweepy.API(auth)

public_tweets = api.home_timeline()
for tweet in public_tweets:
    print(tweet.text)