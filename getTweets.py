import twitter, time, sys, math, json

def getNextID(r):
    min_id = r[0]['id']
    for tweet in r:
        if tweet['id'] < min_id:
            min_id = tweet['id']
    return min_id

def getTweets(r):
    tweets = []
    for i in range(len(r)):
        tweet = {}
        tweet['id'] = r[i].id
        tweet['date'] = r[i].created_at
        tweet['text'] = r[i].text.replace("\n", " ")
        tweet['bounding_box'] = r[i].place['bounding_box']['coordinates'][0]
        tweet['user'] = r[i].user.id
        tweets.append(tweet.copy())
    return tweets

sys.stdout.write('0%')
sys.stdout.flush()

api = twitter.Api (
    consumer_key='JFgkGKs8OffbbtOJntD10wePd',
    consumer_secret='EI6O4tPU9hopEb05diKSeU1891eLaLBp0Sc1dZe6LyqKmN1IaB',
    access_token_key='1200341688-fEVmJFhqxvVy6QSVXHBnFZ3uKe598cHR8gp2Lpn',
    access_token_secret='7SJGvvcNr7jNQ8AHkREEsASvgc4YM8afcOKzYGgF6CTJl'
)

results = getTweets(api.GetSearch(geocode=(51.501,-0.1275,'10mi'), count=100))
next_id = getNextID(results)

for i in range(100):
    flag = 0
    try:
        next_results = getTweets(api.GetSearch(geocode=(51.501,-0.1275,'10mi'), count=100, max_id=next_id))
        next_id = getNextID(next_results)
    except twitter.error.TwitterError:
        print "error"
        next_id = next_id - 1

    results += next_results
    sys.stdout.write('\b'*(len(str(int(math.floor(i*100/151))))+1)  + str(int(math.floor((i+1)*100/151)))+'%')
    sys.stdout.flush()
    
f = open('londonTweets_'+ str(int(time.time())) + '.json', 'w')
f.write(json.dumps(results) + '\n')
f.close()