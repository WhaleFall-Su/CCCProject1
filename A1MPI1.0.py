import copy
import json
import os
from filecmp import cmp
from functools import reduce

import time
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

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


def sum_dict(a, b):
    temp = dict()
    # python3,dict_keys类似set； | 并集
    for key in a.keys() | b.keys():
        temp[key] = sum([d.get(key, 0) for d in (a, b)])
    return temp


def sum_dict_task3(a,b):
    temp = dict()
    num_list = []
    set_combine = set()
    city_cnt_list = []
    # python3,dict_keys类似set； | 并集
    for key in a.keys() | b.keys():
        temp[key] = [{}, 0, set()]
        for d in (a,b):
            # print(key)
            # print(d)
            # print("d.get(key) is ", d.get(key))
            # print('\n')
            if d.get(key) is not None:
                city_cnt_list.append(d.get(key)[0])
                num_list.append(d.get(key)[1])
                for v in d.get(key)[2]:
                    set_combine.add(v)

        temp[key][0] = reduce(sum_dict, city_cnt_list)
        temp[key][1] = sum(num_list)
        temp[key][2] = set_combine

        city_cnt_list = []
        num_list = []
        set_combine = set()
    return temp


with open('./sal.json', 'r', encoding='utf-8') as sal_file:
    gcc_li = ['1gsyd', '2gmel', '3gbri', '4gade', '5gper', '6ghob', '7gdar', '8acte', '9oter']
    location = json.load(sal_file)
    city_belong_dic = {}
    location_keys = list(location.keys())
    # {'abbotsbury': '1gsyd', 'airds': '1gsyd', ...}
    for ele in location_keys:
        gcc = location[ele]['gcc']
        # ignore the information not in greater city
        if gcc in gcc_li:
            city_belong_dic[ele] = gcc

# 得到整个文件的大小
total_bytes = os.path.getsize('./twitter-data-small.json')
# 总大小除以进程数就是每个进程需要负责的byte数
each_bytes = total_bytes // size
begin_position = rank * each_bytes
end_position = (rank + 1) * each_bytes

with open('./twitter-data-small.json', 'r', encoding='utf-8') as tweets_file:
    tweet_str = ''
    tweets_file.seek(begin_position)
    if rank == 0:
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
            # print(tweet_str)
            try:
                tweet_json = json.loads(tweet_str)
                author_id, full_name_list = analyze(tweet_json)
            except:
                tweet_str = ''
                author_id = ''
                full_name_list = []

            # 防止出现空id的情况
            if author_id != '':

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
            # 每个进程负责自己所分配的文件段的下一个json文件的位置，因为可能存在比如说进程1分配的字段是520，但是这个字段在一个json段中的中间，
            # 该json并未完全读取，而我们需要把这个json也完全读取进来，进程1的任务才算结束。
            # 每次读完属于自己的字段后，检查是否有没完全读完的json
            if tweets_file.tell() >= end_position:
                break
            # reset to empty string
            tweet_str = ''
        # reach the end of file(EOF)
        elif not new_line:
            break
        else:
            tweet_str += new_line

    # Task 1 output


    top_author_list = comm.gather(top_author, root=0)
    top_city_list = comm.gather(top_city, root=0)
    author_dict_list = comm.gather(author_dict, root=0)
    if rank == 0:

        # merge dict 解决不同进程处理的文件段中存在author id重复的情况
        merged_dict = reduce(sum_dict, top_author_list)

        # print(merged_dict)
        sort_author = sorted(merged_dict.items(), key=lambda item: item[1])
        sort_author.reverse()

        # get top 10
        top10_author = sort_author[0:10]
        print("{:<25} {:<25} {:<25}".format('Rank', 'Author ID', 'Number of Tweets Made'))
        for i in range(0, 10):
            rank = '#' + str(i + 1)
            author_id = top10_author[i][0]
            num = top10_author[i][1]
            print("{:<25} {:<25} {:<25}".format(rank, author_id, num))

        print('\n', '\n')
        print(time.time() - begin_time)

        # Task 2  output
        # merge dict 解决不同进程处理的文件段中存在author id重复的情况
        merged_dict = reduce(sum_dict, top_city_list)
        sorted_city = sorted(merged_dict.items(), key=lambda item: item[1])
        sorted_city.reverse()
        city_name = ['(Greater Sydney)', '(Greater Melbourne)', '(Greater Brisbane)', '(Greater Adelaide)',
                     '(Greater Preth)',
                     '(Greater Hobart)', '(Greater Darwin)', '(Australian Capital Territory)', '(Others)']

        print("{:<25} {:<25}".format('Greater Capital City', 'Number of Tweets Made'))
        for i in range(0, 9):
            gcc = sorted_city[i][0]
            city_index = int(gcc[0]) - 1
            name = gcc + city_name[city_index]
            num = sorted_city[i][1]
            print("{:<35} {:<25}".format(name, num))
        print('\n', '\n')
        print(time.time() - begin_time)

        # Task 3  output
        # merge dict 解决不同进程处理的文件段中存在author id重复的情况
        merged_dict = reduce(sum_dict_task3, author_dict_list)

        sorted_city = sorted(merged_dict.items(), key=lambda item: len(item[1][NUM_UNIQUE_CITY]))
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
        print(time.time() - begin_time)
