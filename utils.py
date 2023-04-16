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


    with Pool(number_of_threads) as pool:
        pool.starmap(index_shard, [(file_name, index_path) for file_name in notice_files])


def merger(index_path, first_file, second_file, last_merge=False):
    """Merges two files into one"""    

    index_folder = "indexes/"

    output_file = index_path + "final_index" if last_merge else index_path + "indexes/" + first_file + second_file

    print(f"Merging {index_path + index_folder + first_file} and {index_path + index_folder + second_file} into {output_file}")

    with open(index_path + index_folder + first_file, 'r') as ff, open(index_path + index_folder + second_file, 'r') as fs, open(output_file, 'w') as output_file:    
        
        ff_line = ff.readline()
        fs_line = fs.readline()

        while True:
            if ff_line == "":                                           # Caso o primeiro arquivo tenha acabado
                print("Acabou o primeiro arquivo")
                for line in fs.readlines():                             # Escreve o restante do segundo arquivo
                    output_file.write(json.dumps(line))
                    output_file.write("\n")
                break
            
            elif fs_line == "":                                         # Caso o segundo arquivo tenha acabado
                print("Acabou o segundo arquivo")
                for line in fs.readlines():                             # Escreve o restante do primeiro arquivo
                    output_file.write(json.dumps(line))
                    output_file.write("\n")
                break

            elif ff_line == "" and fs_line == "":            # Caso os dois arquivos tenham acabado
                break
            
            else:                                           # Caso nenhum dos arquivos tenha acabado
                ff_doc = json.loads(ff_line)
                fs_doc = json.loads(fs_line)
                
                if  ff_doc['word'] == fs_doc['word']:          
                    ff_doc['docs'] += fs_doc['docs']
                    output_file.write(json.dumps(ff_doc))
                    output_file.write("\n")
                    ff_line = ff.readline()
                    fs_line = fs.readline()
                
                elif ff_doc['word'] < fs_doc['word']:
                    output_file.write(json.dumps(ff_doc))
                    ff_line = ff.readline()
                    output_file.write("\n")
                
                else:
                    output_file.write(json.dumps(fs_doc))
                    fs_line = fs.readline()
                    output_file.write("\n")
                

    os.remove(os.path.join(index_path + index_folder, first_file))
    os.remove(os.path.join(index_path + index_folder, second_file))


def clear_output_folders(index_path: str):
    """Deletes all files in the output folders"""

    for file in os.listdir(index_path + "indexes/"):
        os.remove(os.path.join(index_path + "indexes/", file))

    for file in os.listdir(index_path + "shards/"):
        os.remove(os.path.join(index_path + "shards/", file))

    os.rmdir(index_path + "shards/")
    os.rmdir(index_path + "indexes/")


def run_merger_thread_pool(index_path: str, number_of_threads: int):
#     """Creates a thread pool with the number of threads we want to use"""

    seconds = time.time()

    while True:
    
        notice_files = os.listdir(index_path + "indexes/")
        number_of_files = math.floor(len(notice_files)/2) * 2

        if len(notice_files) == 1:
            break

        elif len(notice_files) == 2:
            merger(index_path, notice_files[0], notice_files[1], True)
            break

        with Pool(number_of_threads) as pool:
            pool.starmap(merger, [(index_path, notice_files[i], notice_files[i+1], False) for i in range(0, number_of_files, 2)]) 


    print(f"Time to merge all files: {time.time() - seconds} seconds")

    clear_output_folders(index_path)



# merger("../output/", "t00", "t01", last_merge=False)
run_merger_thread_pool("../output/", 8)

