import copy
import json
import re
from filecmp import cmp

import pandas as pd
import time
from mpi4py import MPI

com = MPI.COMM_WORLD
rank = com.Get_rank()
size = com.Get_size()

CITY_COUNT = 0
TOTAL_TWEETS = 1
NUM_UNIQUE_CITY = 2

# print(location)

begin_time = time.time()
author_dict = {}


# analyze the json object, filter and count 分析每一个author
def analyze(tweet):
    author_id = tweet.get('data').get('author_id')
    full_name = tweet.get('includes').get('places')[0].get('full_name')
    full_name_list = full_name.split(',')
    for i in range(0, len(full_name_list)):
        full_name_list[i] = full_name_list[i].lstrip()

    # print(full_name_list)
    return author_id, full_name_list


with open('./sal.json', 'r', encoding='utf-8') as sal_file:
    city_dict = {'1gsyd': ['Sydney'], '2gmel': ['Melbourne'], '3gbri': ['Brisbane'],
                 '4gade': ['Adelaide'], '5gper': ['Preth'], '6ghob': ['Hobart'], '7gdar': ['Darwin'],
                 '8acte': ['Canberra'], '9oter': ['Australia External Territory']}

    # 用这个不用city_dic因为我觉得这样会快一些，对于task3因为可以直接通过get得到，而city_dic需要不断的遍历list
    location = json.load(sal_file)
    city_belong_dic = {}
    location_keys = list(location.keys())
    # {'abbotsbury': '1gsyd', 'airds': '1gsyd', ...}
    for ele in location_keys:
        gcc = location[ele]['gcc']
        # 忽略不是capital city发来的消息
        if gcc in city_dict.keys():
            city_belong_dic[ele] = gcc

# print(city_dict)


with open('./twitter-data-small.json', 'r', encoding='utf-8') as tweets_file:
    tweet_str = ''
    # skip the beginning '['
    tweets_file.readline()
    # initialize variable count
    count = 0

    # city_count = {'1gsyd': 0, '2gmel': 0, '3gbri': 0, '4gade': 0, '5gper': 0, '6ghob': 0, '7gdar': 0,
    #               '8acte': 0, '9oter': 0}
    city_count = dict.fromkeys(['1gsyd', '2gmel', '3gbri', '4gade', '5gper', '6ghob', '7gdar', '8acte', '9oter'], 0)
    city_count_keys = city_count.keys()
    num_unique_city = set()
    total_tweets = 0
    author_info = [city_count, total_tweets, num_unique_city]
    while True:
        new_line = tweets_file.readline()
        # indicate the end of a json string
        if new_line == '  },\n' or new_line == '  }\n':
            # only if count % size == rank this process will analyze this tweet
            # count += 1
            # if count % size != rank:
            #     tweet_str = ''
            #     continue

            tweet_str += new_line.split(',')[0]
            tweet_json = json.loads(tweet_str)
            author_id, full_name_list = analyze(tweet_json)

            # print(full_name_list)

            # print(city_belong_dic.keys())

            # 如果author_id不存在在author_dict，则初始化一个city_count。存在则将city_count替换
            if author_id not in author_dict.keys():
                author_dict[author_id] = copy.deepcopy(author_info)

            for place_name in full_name_list:
                place_name = place_name.lower()
                if place_name in city_belong_dic.keys():
                    capital_city = city_belong_dic.get(place_name)
                    # print(capital_city)
                    author_dict[author_id][CITY_COUNT][capital_city] += 1

                    author_dict[author_id][TOTAL_TWEETS] += 1
                    author_dict[author_id][NUM_UNIQUE_CITY].add(capital_city)

            # break

            # reset to empty string
            tweet_str = ''
        # reach the end of file(EOF)
        elif not new_line:
            break
        else:
            tweet_str += new_line

    # 先按发过tweet的城市数量排序
    sorted_city = sorted(author_dict.items(), key=lambda item: len(item[1][NUM_UNIQUE_CITY]))
    sorted_city.reverse()

    # 如果发过tweet的城市数量相同，则按总发过tweets的数量排序
    sorted_city = sorted(sorted_city, key=lambda item: item[1][TOTAL_TWEETS])
    sorted_city.reverse()
    # print(author_dict.items(),"\n")
    #print(sorted_city)

    # 以表格形式输出
    print("{:<25} {:<25} {:<25}".format('Rank', 'Author ID', 'Number of Unique City Locations and #Tweets'))
    for i in range(0, 10):
        count = 0
        rank = '#' + str(i + 1)
        author_id = sorted_city[i][0]
        num_str = str(len(sorted_city[i][1][NUM_UNIQUE_CITY])) + '(#' + str(
            sorted_city[i][1][TOTAL_TWEETS]) + ' tweets - '
        for ele in sorted_city[i][1][NUM_UNIQUE_CITY]:
            count += 1
            if count == (len(sorted_city[i][1][NUM_UNIQUE_CITY])):
                num_str += '#' + str(sorted_city[i][1][CITY_COUNT][ele]) + ele[1:] + ')'
            else:
                num_str += '#' + str(sorted_city[i][1][CITY_COUNT][ele]) + ele[1:] + ', '

        print("{:<25} {:<25} {:<25}".format(rank, author_id, num_str))

    # for key in author_dict.keys():
    #     #author_dict.get(key)
    #     # author_dict = sorted(author_dict.items(), key=lambda item: item[1])
    #     # print(key, author_dict.get(key))
    #     # num_unique_city = len(author_dict.get(key)[NUM_UNIQUE_CITY])
    #
    #     print(key, author_dict[key])
    #     break

print(time.time() - begin_time)

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
    location_keys = list(location.keys())
    tweets_size = len(tweet_file)
    for i in range(0,tweets_size):
        t = tweet_file[i]
        includes = t['includes']
        place_name = includes.get('places')[0].get('full_name')
        names = place_name.split(',')
        for i in range(0, len(names)):
            names[i] = names[i].lstrip()
        #print(names)
        for ele in names:
            ele = ele.lower()
            print(ele)
            if ele in location_keys:
                gcc = location[ele]
                city_count[gcc] += 1
                break
#                 city_count['1gsyd'] += 1
#                 break
#             elif ele in location['2gmel']:
#                 city_count['2gmel'] += 1
#                 break
#             elif ele in location['3gbri']:
#                 city_count['3gbri'] += 1
#                 break
#             elif ele in location['4gade']:
#                 city_count['4gade'] += 1
#                 break
#             elif ele in location['5gper']:
#                 city_count['5gper'] += 1
#                 break
#             elif ele in location['6ghob']:
#                 city_count['6ghob'] += 1
#                 break
#             elif ele in location['7gdar']:
#                 city_count['7gdar'] += 1
#                 break
#             elif ele in location['8acte']:
#                 city_count['8acte'] += 1
#                 break
#             elif ele in location['9oter']:
#                 city_count['9oter'] += 1
#                 break
    sorted_city = sorted(city_count.items(), key=lambda item: item[1])
    sorted_city.reverse()
    return sorted_city

city_name = ['(Greater Sydney)','(Greater Melbourne)','(Greater Brisbane)','(Greater Adelaide)','(Greater Preth)',
             '(Greater Hobart)','(Greater Darwin)','(Australian Capital Territory)','(Others)']
city_tweets_count = tweets_in_city(city_belong_dic,tweet)
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
for i in range(0,10):
    rank = '#' + str(i+1)
    author_id = top10[i][0]
    num = top10[i][1]
    print ("{:<25} {:<25} {:<25}".format(rank, author_id, num))
