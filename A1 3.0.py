import copy
import json
from filecmp import cmp

import time
from mpi4py import MPI

com = MPI.COMM_WORLD
rank = com.Get_rank()
size = com.Get_size()

CITY_COUNT = 0
TOTAL_TWEETS = 1
NUM_UNIQUE_CITY = 2

begin_time = time.time()
author_dict = {}


def analyze(tweet):
    author_id = tweet.get('data').get('author_id')
    full_name = tweet.get('includes').get('places')[0].get('full_name')
    full_name_list = full_name.split(',')
    for i in range(0, len(full_name_list)):
        full_name_list[i] = full_name_list[i].lstrip()

    # print(full_name_list)
    return author_id, full_name_list

with open('./sal.json', 'r', encoding='utf-8') as sal_file:
    gcc_li = ['1gsyd','2gmel','3gbri','4gade','5gper','6ghob','7gdar','8acte','9oter']
    location = json.load(sal_file)
    city_belong_dic = {}
    location_keys = list(location.keys())
    # {'abbotsbury': '1gsyd', 'airds': '1gsyd', ...}
    for ele in location_keys:
        gcc = location[ele]['gcc']
        # ignore the information not in greater city
        if gcc in gcc_li:
            city_belong_dic[ele] = gcc



with open('./twitter-data-small.json', 'r', encoding='utf-8') as tweets_file:
    tweet_str = ''
    # skip the beginning '['
    tweets_file.readline()
    # initialize variable count
    count = 0

    city_count = dict.fromkeys(['1gsyd', '2gmel', '3gbri', '4gade', '5gper', '6ghob', '7gdar', '8acte', '9oter'], 0)
    top_author = {} 
    top_city = dict.fromkeys(['1gsyd', '2gmel', '3gbri', '4gade', '5gper', '6ghob', '7gdar', '8acte', '9oter'], 0) 
    city_count_keys = city_count.keys()
    num_unique_city = set()
    total_tweets = 0
    author_info = [city_count, total_tweets, num_unique_city]
    while True:
        new_line = tweets_file.readline()
        # indicate the end of a json string
        if new_line == '  },\n' or new_line == '  }\n':

            tweet_str += new_line.split(',')[0]
            tweet_json = json.loads(tweet_str)
            author_id, full_name_list = analyze(tweet_json)

            if author_id not in author_dict.keys():
                author_dict[author_id] = copy.deepcopy(author_info)

            # Task 1 top10 author with the most tweets
            if author_id in top_author:
                top_author[author_id] += 1

            if author_id not in top_author:
                top_author[author_id] = 1


            # Task 2 sorted greater city by the number of tweets
            for place_name in full_name_list:
                place_name = place_name.lower()
                if place_name in city_belong_dic.keys():
                    gcc = city_belong_dic[place_name]
                    top_city[gcc] += 1
                    break

            # Task 3 top 10 tweeters making tweets from the most different locations
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


    # Task 1 output
    # sort by the number of tweets
    sort_author = sorted(top_author.items(), key=lambda item: item[1])
    sort_author.reverse()

    # get top 10 
    top10_author = sort_author[0:10]
    print ("{:<25} {:<25} {:<25}".format('Rank', 'Author ID', 'Number of Tweets Made'))
    for i in range(0,10):
        rank = '#' + str(i+1)
        author_id = top10_author[i][0]
        num = top10_author[i][1]
        print ("{:<25} {:<25} {:<25}".format(rank, author_id, num))
    
    print('\n','\n')


    # Task 2  output
    sorted_city = sorted(top_city.items(), key=lambda item: item[1])
    sorted_city.reverse()
    city_name = ['(Greater Sydney)','(Greater Melbourne)','(Greater Brisbane)','(Greater Adelaide)','(Greater Preth)',
             '(Greater Hobart)','(Greater Darwin)','(Australian Capital Territory)','(Others)']
    print ("{:<25} {:<25}".format('Greater Capital City','Number of Tweets Made'))
    for i in range(0,9):
        gcc = sorted_city[i][0]
        city_index = int(gcc[0]) - 1
        name = gcc + city_name[city_index]
        num = sorted_city[i][1]
        print ("{:<35} {:<25}".format(name,num))
    print('\n','\n')


    # Task 3  output
    # sort by number of city
    sorted_city = sorted(author_dict.items(), key=lambda item: len(item[1][NUM_UNIQUE_CITY]))
    sorted_city.reverse()

    # If the number of city are the same, sort by the sum of tweets
    sorted_city = sorted(sorted_city, key=lambda item: item[1][TOTAL_TWEETS])
    sorted_city.reverse()
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