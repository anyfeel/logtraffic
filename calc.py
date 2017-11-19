import re
import os
import signal
import math
import sys
import time
import copy
import traceback
import multiprocessing
from subprocess import PIPE, Popen
from multiprocessing import cpu_count
from multiprocessing import Manager

manager = Manager()

'''
eg: x= '220.202.152.119 - - [30/Oct/2017:00:00:01 +0800] "GET http://p0.meituan.net/110.110/shaitu/c6323dac8fe3c2a84440915bfcdc66eb242369.jpg.webp HTTP/1.1" 200 3924 "-" "%E7%BE%8E%E5%9B%A2/1717 CFNetwork/887 Darwin/17.0.0" "image/webp" 0 Miss "U/200" Static "max-age=691200" 0.091 220.202.121.10'

split:
['220.202.152.119', '-', '-', '30/Oct/2017:00:00:01 +0800', 'GET http://p0.meituan.net/110.110/shaitu/c6323dac8fe3c2a84440915bfcdc66eb242369.jpg.webp HTTP/1.1', '200', '3924', '-', '%E7%BE%8E%E5%9B%A2/1717 CFNetwork/887 Darwin/17.0.0', 'image/webp', '0', 'Miss', 'U/200', 'Static', 'max-age=691200', '0.091', '220.202.121.10']

'''
def shell(command):
     process = Popen(
         args=command,
         stdout=PIPE,
         shell=True
     )

     return process.communicate()[0]

r = re.compile('\[([^\[\]]+)\]|"([^"]+)"| ')

def split(log):
    if not log:
        return []

    s = filter(lambda x: x and x != ' ' and x != '', re.split(r, log))

    return s

def insert(content, key, value):
    if content.get(key):
        content[key] = content[key] + value
    else:
        content[key] = value

def task(file_name, content):
    try:
        ungzip_name = file_name[:-3] + "_ungzip"
        shell("gzip -d %s -c > %s"  % (file_name, ungzip_name))

        file_ob = open(ungzip_name, "r")

        for line in file_ob:
            split_line = split(line)

            if not split_line:
                continue

            try:
                timestamp = split_line[3].split(" ")[0]
                key = str(int(time.mktime(time.strptime(timestamp, "%d/%b/%Y:%H:%M:%S"))))
                body_bytes_sent = int(split_line[6])
                request_time = int(math.ceil(float(split_line[-2])))

                if request_time > 1:
                    avg_body_bytes_sent = body_bytes_sent / request_time
                    for i in range(0, request_time):
                        insert(content, str(int(key) + 1), avg_body_bytes_sent)
                else:
                    insert(content, key, body_bytes_sent)
            except Exception:
                pass
    except Exception as e:
        pass

    finally:
        file_ob.close()
        shell('rm %s' % ungzip_name)

def merge_file(path):
    sec = []
    pool = multiprocessing.Pool()
    for file_name in os.listdir(path):
        if file_name.find("ungzip") != -1:
            continue

        print file_name
        result = manager.dict()
        sec.append(result)

        pool.apply_async(task, args=(path + "/" + file_name, result))

    pool.close()
    pool.join()

    try:
        for i in range(0, len(sec)):
            sec[i] = dict(sec[i])
    except Exception:
        pass

    return sec


def merge_sec_task(dst, src, result):
    try:
        for k, v in src.iteritems():
            if not result.get(k):
                result[k] = v
            else:
                result[k] = result[k] + v

        for k, v in dst.iteritems():
            if not result.get(k):
                result[k] = v
            else:
                result[k] = result[k] + v
    except Exception:
        pass


def merge_sec(sec):
    while len(sec) > 1:
        merge_round = []
        pool = multiprocessing.Pool()
        len_sec = len(sec)

        for i in range(0, len_sec, 2):
            try:
                if i + 1 == len_sec:
                    sec_round = sec[i]
                    merge_round.append(sec_round)
                    break
                else:
                    sec_round = manager.dict()
                    merge_round.append(sec_round)
                    pool.apply_async(merge_sec_task, args=(sec[i], sec[i + 1], sec_round))
            except Exception:
                continue

        pool.close()
        pool.join()

        for i in range(0, len(merge_round)):
            merge_round[i] = dict(merge_round[i])

        sec = merge_round

    return sec[0]


def merge_5min_task(content, result):
    for t in content:
        try:
            raw_time = int(t[0])
            if raw_time % 300 != 0:
                raw_time = (raw_time / 300 + 1) * 300

            key = time.strftime("%H:%M", time.localtime(raw_time))

            if result.get(key):
                result[key] = result[key] + t[1]
            else:
                result[key] = t[1]

        except Exception as e:
            pass

def merge_5min(sec):
    if not sec:
        return {}

    sec_list = []
    pool = multiprocessing.Pool()

    for t in sec.items():
        sec_list.append(t)

    min_result = []
    start = 0
    step = len(sec_list) / cpu_count() if len(sec_list) > cpu_count else len(sec_list)

    while start < len(sec_list):
        tmp = manager.dict()
        min_result.append(tmp)

        try:
            if start + step > len(sec_list):
                content= sec_list[start:]
            else:
                content = sec_list[start:(start + step)]
        except Exception:
            pass

        pool.apply_async(merge_5min_task, args=(content, tmp))
        start = start + step

    pool.close()
    pool.join()

    for i in range(0, len(min_result)):
        try:
            min_result[i] = dict(min_result[i])
        except Exception:
            continue

    new_result = {}
    for result in min_result:
        try:
            for k, v in  dict(result).iteritems():
                if new_result.get(k):
                    new_result[k] = new_result[k] = v
                else:
                    new_result[k] = v
        except Exception:
            pass

    return new_result


def on_signal(a, b):
     os.kill(0, signal.SIGKILL)
     os._exit(0)

if __name__ == '__main__':
    path = sys.argv[1]

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGQUIT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    merge_file_result = merge_file(path)
    merge_sec_result = merge_sec(merge_file_result)
    min_result = merge_5min(merge_sec_result)

    print min_result

