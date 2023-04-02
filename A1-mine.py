import json
import re

import pandas as pd
import time
from mpi4py import MPI

com = MPI.COMM_WORLD
rank = com.Get_rank()
size = com.Get_size()


# print(location)

begin_time = time.time()

# analyze the json object, filter and count 分析每一个author
def analyze(tweet):
    author_id = tweet.get('data').get('author_id')
    full_name = tweet.get('includes').get('places')[0].get('full_name')
    full_name_list = full_name.split(',')
    for i in range(0,len(full_name_list)):
        full_name_list[i] = full_name_list[i].lstrip()

    #print(full_name_list)
    return author_id, full_name_list

with open('./sal.json', 'r', encoding='utf-8') as sal_file:
    city_dict = {'1gsyd': ['Sydney'], '2gmel': ['Melbourne'], '3gbri': ['Brisbane'],
                 '4gade': ['Adelaide'], '5gper': ['Preth'], '6ghob': ['Hobart'],'7gdar': ['Darwin'],
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



#print(city_dict)


with open('./twitter-data-small.json', 'r', encoding='utf-8') as tweets_file:
    tweet_str = ''
    # skip the beginning '['
    tweets_file.readline()
    # initialize variable count
    count = 0

    author_dict = {}
    city_count = {'1gsyd': 0, '2gmel': 0, '3gbri': 0, '4gade': 0, '5gper': 0, '6ghob': 0, '7gdar': 0,
                  '8acte': 0, '9oter': 0}
    city_count_keys = city_count.keys()
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

            #print(full_name_list)

            #print(city_belong_dic.keys())

            # 如果author_id不存在在author_dict，则初始化一个city_count。存在则将city_count替换
            if author_id in author_dict:
                city_count = author_dict[author_id]
            else:
                author_dict[author_id] = city_count

            for place_name in full_name_list:
                #print(place_name)
                place_name = place_name.lower()
                if place_name in city_belong_dic.keys():
                    capital_city = city_belong_dic.get(place_name)
                    #print(capital_city)
                    city_count[capital_city] += 1


            #break

            # reset to empty string
            tweet_str = ''
        # reach the end of file(EOF)
        elif not new_line:
            break
        else:
            tweet_str += new_line


    for key in author_dict.keys():

        author_dict.get(key).get()
        #author_dict = sorted(author_dict.items(), key=lambda item: item[1])
        #print(key, author_dict.get(key))
        #print(author_dict.items())






# def author_with_most_gcc(city_belong_dic, tweet_file):
#     city_count = {'1gsyd': 0, '2gmel': 0, '3gbri': 0, '4gade': 0, '5gper': 0, '6ghob': 0, '7gdar': 0,
#                   '8acte': 0, '9oter': 0}
#     city_count_keys = city_count.keys()
#
#     for i in range(0, tweets_size):
#         t = tweet_file[i]
#         includes = t['includes']
#         place_name = includes.get('places')[0].get('full_name')
#         if place_name in city_belong_dic.keys():
#             capital_city = city_belong_dic.get(place_name)
#             if city_count.get(capital_city) < 1:
#                 city_count[capital_city] = 1
#             else:
#                 continue
#
#     for key in city_count_keys:
#         if :
#
#
# author_with_most_gcc(city_belong_dic, tweets_file)

# # task 2
# # get a dict for great city (不确定每个City名字算不算在内 如果不算可以删了)
# def get_city_no(location_file):
#     city_dict = {'1gsyd': ['Sydney'], '2gmel': ['Melbourne'], '3gbri': ['Brisbane'], '4gade': ['Adelaide'],
#                  '5gper': ['Preth'], '6ghob': ['Hobart'],
#                  '7gdar': ['Darwin'], '8acte': ['Canberra'], '9oter': ['Australia External Territory']}
#     location_keys = list(location_file.keys())
#     g_city_keys = list(city_dict.keys())
#
#     # print(g_city_keys)
#     for ele in location_keys:
#         gcc = location[ele]['gcc']
#         if gcc in g_city_keys:
#             city_dict[gcc].append(ele)
#     return city_dict
#
#
# city_dic = get_city_no(location)
# tweets_size = len(tweets_file)
#
#
# # print(city_dic)
#
#
# # Get the top10 city with the most tweets
# def tweets_in_city(location, tweet_file):
#     city_count = {'1gsyd': 0, '2gmel': 0, '3gbri': 0, '4gade': 0, '5gper': 0, '6ghob': 0, '7gdar': 0,
#                   '8acte': 0, '9oter': 0}
#     for i in range(0, tweets_size):
#         t = tweet_file[i]
#         includes = t['includes']
#         place_name = includes.get('places')[0].get('full_name')
#         names = place_name.split(',')
#         print(names)
#         for ele in names:
#             ele = ele.lower()
#             if ele in location['1gsyd']:
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
#     sorted_city = sorted(city_count.items(), key=lambda item: item[1])
#     sorted_city.reverse()
#     return sorted_city
#
#
# city_name = ['(Greater Sydney)', '(Greater Melbourne)', '(Greater Brisbane)', '(Greater Adelaide)', '(Greater Preth)',
#              '(Greater Hobart)', '(Greater Darwin)', '(Australian Capital Territory)', '(Others)']
# city_tweets_count = tweets_in_city(city_dic, tweets_file)
# print("{:<25} {:<25}".format('Greater Capital City', 'Number of Tweets Made'))
#
# for i in range(0, 9):
#     gcc = city_tweets_count[i][0]
#     # print(city_tweets_count)
#     city_index = int(gcc[0]) - 1
#     name = gcc + city_name[city_index]
#     num = city_tweets_count[i][1]
#     print("{:<35} {:<25}".format(name, num))
#
# print(time.time() - begin_time)
#
#
# # task 1
# # get the top10 author with most tweet
# def get_author_info(tweet_file):
#     author_dict = {}
#     size = len(tweet_file)
#     for i in range(0, size):
#         author_id = tweet_file[i]['data']['author_id']
#         if author_id in author_dict:
#             author_dict[author_id] += 1
#         else:
#             author_dict[author_id] = 1
#     author_dict = sorted(author_dict.items(), key=lambda item: item[1])
#     author_dict.reverse()
#     return author_dict
#
#
# top10 = get_author_info(tweets_file)[0:10]
# # print(top10)
# print("{:<25} {:<25} {:<25}".format('Rank', 'Author ID', 'Number of Tweets Made'))
# for i in range(0, 10):
#     rank = '#' + str(i + 1)
#     author_id = top10[i][0]
#     num = top10[i][1]
#     print("{:<25} {:<25} {:<25}".format(rank, author_id, num))
#
# print(time.time() - begin_time)


