#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 18:50:12 2020

@author: godson
"""
#The role of this collector is to collect enough tweets about
#particular topic to do some data analysis


#To be removed before uploading
access_token = '************************'
access_token_secret = '**********************'
consumer_key = '****************************'
consumer_secret = '*****************************'

import tweepy
import sys
import pandas as pd
import time

#Authentification
#auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)#This one is used to increase the rate limit
api = tweepy.API(auth, wait_on_rate_limit=True,
				   wait_on_rate_limit_notify=True)

if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)

#auth.set_access_token(access_token, access_token_secret)
#api = tweepy.API(auth)


def stream(data, file_name):
    #Ce dataframe contiendra les tweets qui seront sauvergardés en fichier csv
    df = pd.DataFrame(columns = ['content','source', 'User', 'User_description','User_statuses_count', 
                             'user_followers', 'User_location', 'User_verified'
                             , 'rt_count', 'fav_count', 'tweet_date','tweet_coordinates'])
    i = 0
    file_i = 0
    file_name_actual = file_name
    sinceId = None
    max_id = -1
    tweetCount = 0
    #searchQuery = '#someHashtag'  # this is what we're searching for
    maxTweets = 1000000 # Some arbitrary large number
    tweetsPerQry = 100  # this is the max the API permits
    
    #print("Downloading max {0} tweets".format(maxTweets))
    while tweetCount < maxTweets:
        try:
            if (max_id <= 0):
                if (not sinceId):
                    new_tweets = api.search(q=data, count=tweetsPerQry)
                else:
                    new_tweets = api.search(q=data, count=tweetsPerQry,
                                            since_id=sinceId)
            else:
                if (not sinceId):
                    new_tweets = api.search(q=data, count=tweetsPerQry,
                                            max_id=str(max_id - 1))
                else:
                    new_tweets = api.search(q=data, count=tweetsPerQry,
                                            max_id=str(max_id - 1),
                                            since_id=sinceId)
            if not new_tweets:
                print("No more tweets found")
                break
            for tweet in new_tweets:
                if (tweet.user.location and str(tweet.user.location) != 'nan' and tweet.retweet_count > 100):
                    #print(i,"\n")
                    df.loc[i, 'id'] = tweet.id
                    df.loc[i, 'content'] = tweet.text
                    df.loc[i, 'source'] = tweet.source #De quel plateforme (web, mobile le tweet a t il été envoyé)
                    df.loc[i, 'User'] = tweet.user.name
                    df.loc[i, 'User_description'] = tweet.user.description
                    df.loc[i, 'User_statuses_count'] = tweet.user.statuses_count #Number of tweets (and retweets) that have been made by the user
                    df.loc[i, 'user_followers'] = tweet.user.followers_count
                    df.loc[i, 'User_location'] = tweet.user.location
                    df.loc[i, 'User_verified'] = tweet.user.verified
                    df.loc[i, 'rt_count'] = tweet.retweet_count
                    if tweet.favorite_count : df.loc[i, 'fav_count'] = tweet.favorite_count
                    df.loc[i, 'tweet_date'] = tweet.created_at
                    if tweet.coordinates : df.loc[i, 'tweet_coordinates'] = str(tweet.coordinates)
                    i+=1
                    #Below are only available for premium
                    #df.loc[i, 'tweet_quote_count'] = tweet.quote_count
                    #df.loc[i, 'tweet_reply_count'] = tweet.reply_count
            df.to_csv('{}.csv'.format(file_name_actual))
            
            if i > 5000 :
                tweetCount += i
                i = 0
                file_i = file_i + 1
                df = df[0:0]
                print("english",file_i)
                file_name_actual = file_name + str(file_i)
            else:
                pass
            #print("Downloaded {0} tweets".format(i))
            max_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            # Just exit if any error
            print("some error : " + str(e))
            break
    
    
        
try :
    stream(data = ['COVID-19 OR %22Corona%20virus%22 OR COVID2019 OR Coronavirus'], file_name = 'our_tweets')
except tweepy.TweepError as e:
    print(e.reason)
    #time.sleep(60)
