import json
import pandas as pd


ff = open('twitter-data-small.json')
tweet = json.load(ff)

ff1 = open('sal.json')
location = json.load(ff1)



## get a dict for great city (不确定每个City名字算不算在内 如果不算可以删了)
def get_city_no(location_file):
    city_dict = {'1gsyd':['Sydney'],'2gmel':['Melbourne'],'3gbri':['Brisbane'],'4gade':['Adelaide'],'5gper':['Preth'],'6ghob':['Hobart'],
                 '7gdar':['Darwin'],'8acte':['Canberra'],'9oter':['Australia External Territory']}
    location_keys = list(location_file.keys())
    g_city_keys = list(city_dict.keys())
    #print(g_city_keys)
    for ele in location_keys:
        gcc = location[ele]['gcc']
        if gcc in g_city_keys:
            city_dict[gcc].append(ele)
    return city_dict

city_dic = get_city_no(location)
#print(city_dic)


# Get the top10 city with the most tweets
def tweets_in_city(location,tweet_file):
    city_count = {'1gsyd':0,'2gmel':0,'3gbri':0,'4gade':0,'5gper':0,'6ghob':0,'7gdar':0,
                  '8acte':0,'9oter':0}
    tweets_size = len(tweet_file)
    for i in range(0,tweets_size):
        t = tweet_file[i]
        includes = t['includes']
        place_name = includes.get('places')[0].get('full_name')
        names = place_name.split(',')
        for ele in names:
            ele = ele.lower()
            if ele in location['1gsyd']:
                city_count['1gsyd'] += 1
                break
            elif ele in location['2gmel']:
                city_count['2gmel'] += 1
                break
            elif ele in location['3gbri']:
                city_count['3gbri'] += 1
                break
            elif ele in location['4gade']:
                city_count['4gade'] += 1
                break
            elif ele in location['5gper']:
                city_count['5gper'] += 1
                break
            elif ele in location['6ghob']:
                city_count['6ghob'] += 1
                break
            elif ele in location['7gdar']:
                city_count['7gdar'] += 1
                break
            elif ele in location['8acte']:
                city_count['8acte'] += 1
                break
            elif ele in location['9oter']:
                city_count['9oter'] += 1
                break
    sorted_city = sorted(city_count.items(), key=lambda item: item[1])
    sorted_city.reverse()
    return sorted_city

city_name = ['(Greater Sydney)','(Greater Melbourne)','(Greater Brisbane)','(Greater Adelaide)','(Greater Preth)',
             '(Greater Hobart)','(Greater Darwin)','(Australian Capital Territory)','(Others)']
city_tweets_count = tweets_in_city(city_dic,tweet)
print ("{:<25} {:<25}".format('Greater Capital City','Number of Tweets Made'))
for i in range(0,9):
    gcc = city_tweets_count[i][0]
    city_index = int(gcc[0]) - 1
    name = gcc + city_name[city_index]
    num = city_tweets_count[i][1]
    print ("{:<35} {:<25}".format(name,num))



# get the top10 author with most tweet
def get_author_info(tweet_file):
    author_dict = {}
    size = len(tweet_file)
    for i in range(0,size):
        author_id = tweet_file[i]['data']['author_id']
        if author_id in author_dict:
            author_dict[author_id] += 1
        else:
            author_dict[author_id] = 1
    author_dict = sorted(author_dict.items(), key=lambda item: item[1])
    author_dict.reverse()
    return author_dict

top10 = get_author_info(tweet)[0:10]
# print(top10)
print ("{:<25} {:<25} {:<25}".format('Rank', 'Author ID', 'Number of Tweets Made'))
for i in range(0,11):
    rank = '#' + str(i+1)
    author_id = top10[i][0]
    num = top10[i][1]
    print ("{:<25} {:<25} {:<25}".format(rank, author_id, num))


# def author_with_most_gcc(city_dict,tweet_file):
    