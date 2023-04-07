import json, os

from preprocesser import preprocesser, get_word_frequency
from multiprocessing import Pool, Lock

lock = Lock()


def index_shard(file_name: str, index_path: str) -> None:

    print("Indexing " + file_name)

    index = {}
    with open(index_path + "shards/" + file_name, 'r') as f:
        for line in f:
            doc = json.loads(line)
            cleaned_text = preprocesser(doc['text'])
            word_freq = get_word_frequency(cleaned_text)
            for word, freq in word_freq.items():	
                if word not in index:
                    index[word] = []
                index[word].append((doc['id'],freq))


    print("Downloading the indexing " + file_name)

    with open(index_path + "indexes/" + file_name, 'w') as fp:
        json.dump(index, fp)

    print("Finished indexing " + file_name)


def create_thread_pool(index_path: str, number_of_threads: int):
    """Creates a thread pool with the number of threads we want to use"""
    
    notice_files = os.listdir(index_path + "shards/")

    with Pool(number_of_threads) as pool:
        pool.starmap(index_shard, [(file_name, index_path) for file_name in notice_files])

