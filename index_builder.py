import json, os, time, logging, math

from preprocesser import preprocesser, get_word_frequency
from collections import OrderedDict

from multiprocessing import  Pool



# def encode_to_binary(word_id: int, doc_freqs: list[int, int]) -> bytearray:
#     """Encodes a word and its document frequencies to binary"""
    
#     bin_array = bytearray()
#     bin_array += word_id.to_bytes(4, "big")
#     bin_array += len(doc_freqs).to_bytes(4, "big")

#     for doc, freq in doc_freqs:
#         bin_array += int(doc).to_bytes(4, "big")
#         bin_array += int(freq).to_bytes(2, "big")


#     return bin_array


# def decode_from_binary(file):   #Vou ter que corrigir a leitura do arquivo
#     """Decodes a word and its document frequencies from binary"""

#     index = OrderedDict()

#     while file.read(1) != b'':
#         file.seek(-1, 1)

#         word_id = int.from_bytes(file.read(1), "big")
#         doc_freqs = []
#         for _ in range(int.from_bytes(file.read(1), "big")):
#             doc_freqs.append((int.from_bytes(file.read(1), "big"), int.from_bytes(file.read(1), "big")))

#         index[word_id] = doc_freqs

#     return index


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
            doc = json.loads(line)
            cleaned_text = preprocesser(doc['text'])
            word_freq = get_word_frequency(cleaned_text)
            for word, freq in word_freq.items():	
                if word not in index:
                    index[word] = []
                index[word].append((doc['id'],freq))

    logging.info(f"Time to create index of shard {file_name}: {time.time() - seconds} seconds")
   
    download_index_shard(index, index_path, file_name)


def run_indexer_thread_pool(index_path: str, number_of_threads: int):
    """Creates a thread pool with the number of threads we want to use"""
    
    notice_files = os.listdir(index_path + "shards/")
    logging.info(f"Found {len(notice_files)} shards")

    os.mkdir(index_path + "indexes/") 

    with Pool(number_of_threads) as pool:
        pool.starmap(index_shard, [(file_name, index_path) for file_name in notice_files])