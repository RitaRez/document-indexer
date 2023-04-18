import json, os, time, logging, math, subprocess

from preprocesser import preprocesser, get_word_frequency
from collections import OrderedDict

from multiprocessing import  Pool




def encode_to_string(word: int, doc_freqs: list[int, int]) -> str:

    line = "{\"word\":\"" + str(word) + "\",\"docs\": ["
    for doc, freq in doc_freqs:
        line += "{\"id\":\"" + str(doc) + "\",\"freq\":\"" + str(freq) + "\"},"
    line = line[:-1]
    line += "]}\n" 

    return line


def download_index_shard(index: OrderedDict, index_path: str, file_name: str):
    """Downloads the index of a shard to a file"""

    seconds = time.time()
    with open(index_path + "indexes/" + file_name, 'w') as fp:
        for word in sorted(index):

            # line = encode_to_binary(word, index[word])
            line = encode_to_string(word, index[word])
            fp.write(line)

    logging.info(f"Time to save index of shard {file_name}: {time.time() - seconds} seconds")


def index_shard(file_name: str, index_path: str) -> None:
    """Indexes a shard of the corpus"""

    seconds = time.time()
    index = OrderedDict()
    with open(index_path + "shards/" + file_name, 'r') as f:
        for line in f:
            try:
                doc = json.loads(line)
                cleaned_text = preprocesser(doc['text'])
                word_freq = get_word_frequency(cleaned_text)
                for word, freq in word_freq.items():	
                    if word not in index:
                        index[word] = []
                    index[word].append((doc['id'],freq))
            except:
                print("-------------------------JSONDecodeError-------------------------")
                print(line)
                print("-------------------------JSONDecodeError-------------------------")

    logging.info(f"Time to create index of shard {file_name}: {time.time() - seconds} seconds")
   
    download_index_shard(index, index_path, file_name)


def run_indexer_thread_pool(index_path: str, number_of_threads: int, number_of_shards: int):
    """Creates a thread pool with the number of threads we want to use"""
    
    notice_files = os.listdir(index_path + "shards/")
    count = 0
    while len(notice_files) < number_of_shards:
        notice_files = os.listdir(index_path + "shards/")
        time.sleep(1)
        count += 1

    # notice_files = []
    # print(index_path + "shards/")
    # for address, dirs, files in os.walk(index_path + "shards/"):
    #     for name in files:
    #         notice_files.append(name)
            
    logging.info(f"Found {len(notice_files)} shards")


    if not os.path.isdir(index_path + "indexes/"):
        os.mkdir(index_path + "indexes/") 

    with Pool(number_of_threads) as pool:
        pool.starmap(index_shard, [(file_name, index_path) for file_name in notice_files])