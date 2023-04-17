
import json, os, time, logging, math

from collections import OrderedDict
from multiprocessing import Pool

logging.basicConfig(filename='main.log', level=logging.INFO)


def save_term_lexicon(index_path: str, term_lexicon: dict):
    """Saves the term lexicon to a file"""

    with open(index_path + "term_lexicon.txt", 'w') as fp:
        for word in term_lexicon:
            fp.write(word + " " + str(term_lexicon[word]) + "\n")


def builds_term_lexicon(last_merge: bool, doc: dict, term_lexicon: dict, number_of_words: int):
    """Builds the term lexicon"""
    
    if last_merge:                                                  # Caso seja a última junção, cria o lexicon  
        number_of_words += 1
        term_lexicon[doc['word']] = number_of_words                 #Adiciona as palavras restantes no lexicon
    
    return number_of_words, term_lexicon

def finishes_reading_index(f, output_file, term_lexicon: dict, last_merge: bool, number_of_words: int):
    """Finishes reading the index and writes the rest of the lines to the output file"""    
    
    for line in f.readlines():                                          # Escreve o restante do segundo index
        doc = json.loads(line)
        output_file.write(json.dumps(doc) + "\n")
        number_of_words, term_lexicon = builds_term_lexicon(last_merge, doc, term_lexicon, number_of_words)
        
    return number_of_words, term_lexicon

def merger(index_path: str, first_file: str, second_file: str, last_merge=False):
    """Merges two files into one"""    
    
    number_of_words = 0; term_lexicon = {}
    output_file = index_path + "final_index" if last_merge else index_path + "indexes/" + first_file + second_file
    
    logging.info(f"Merging {index_path}indexes/{first_file} and {index_path}indexes/{second_file} into {output_file}")

    with open(index_path + "indexes/" + first_file, 'r') as ff, open(index_path + "indexes/" + second_file, 'r') as fs, open(output_file, 'w') as output_file:    
        
        ff_line = ff.readline()
        fs_line = fs.readline()
        while True:
            
            if ff_line == "":                                                                          # Caso o primeiro index tenha acabado
                number_of_words, term_lexicon = finishes_reading_index(fs, output_file, term_lexicon, last_merge, number_of_words)     # Escreve o restante do segundo index
                break
            
            elif fs_line == "":                                                                        # Caso o segundo arquivo tenha acabado
                number_of_words, term_lexicon = finishes_reading_index(ff, output_file, term_lexicon, last_merge, number_of_words)     # Escreve o restante do primeiro arquivo
                break
            
            elif ff_line == "" and fs_line == "":                                                      # Caso os dois arquivos tenham acabado juntos
                break
            
            else:                                                                                      # Caso nenhum dos arquivos tenha acabado
                try:
                    ff_doc = json.loads(ff_line); fs_doc = json.loads(fs_line)                             # Carrega ambas as linhas
                except:
                    print("Error loading json:")
                    print("-----------------------------------------------------------------")
                    print("-" + ff_line + "-")
                    print("------------------------------------------------------------------")
                    print("-" + fs_line + "-")
                    print("-----------------------------------------------------------------")
                    break  
                if  ff_doc['word'] == fs_doc['word']:                                                  # Caso as palavras sejam iguais   
                    ff_doc['docs'] += fs_doc['docs']                                                   # Junta os documentos
                    
                    output_file.write(json.dumps(ff_doc) + "\n")                                       # Escreve a linha no arquivo de saída    
                    number_of_words, term_lexicon = builds_term_lexicon(last_merge, ff_doc, term_lexicon, number_of_words)             # Adiciona a palavra no lexicon

                    ff_line = ff.readline(); fs_line = fs.readline()                                   # Lê as próximas linhas dos arquivos
                
                elif ff_doc['word'] < fs_doc['word']:                                                  # Caso a palavra do primeiro arquivo seja menor
                    output_file.write(json.dumps(ff_doc) + "\n")                                       # Escreve a linha no arquivo de saída
                    number_of_words, term_lexicon = builds_term_lexicon(last_merge, ff_doc, term_lexicon, number_of_words)             # Adiciona a palavra no lexicon

                    ff_line = ff.readline()                                                            # Lê a próxima linha do primeiro arquivo
                
                else:                                                                                  # Caso a palavra do segundo arquivo seja menor
                    output_file.write(json.dumps(fs_doc) + "\n")                                       # Escreve a linha no arquivo de saída
                    number_of_words, term_lexicon = builds_term_lexicon(last_merge, fs_doc, term_lexicon, number_of_words)             # Adiciona a palavra no lexicon

                    fs_line = fs.readline()                                                            # Lê a próxima linha do segundo arquivo
                

    os.remove(os.path.join(index_path + "indexes/", first_file))                                       # Apaga os arquivos que foram mesclados
    os.remove(os.path.join(index_path + "indexes/", second_file))

    if last_merge:
        save_term_lexicon(index_path, term_lexicon)                                                                # Salva o lexicon no arquivo term_lexicon.txt

    time.sleep(1)

def clear_output_folders(index_path: str):
    """Deletes all files in the output folders"""

    for file in os.listdir(index_path + "indexes/"):
        os.remove(os.path.join(index_path + "indexes/", file))

    for file in os.listdir(index_path + "shards/"):
        os.remove(os.path.join(index_path + "shards/", file))

    os.rmdir(index_path + "shards/")
    os.rmdir(index_path + "indexes/")


def run_merger_thread_pool(index_path: str, number_of_threads: int):
    """Creates a thread pool for merging indexes"""

    seconds = time.time()

    while True:
    
        notice_files = os.listdir(index_path + "indexes/")
        number_of_files = math.floor(len(notice_files)/2) * 2

        if len(notice_files) == 2:
            merger(index_path, notice_files[0], notice_files[1], True)
            break

        with Pool(number_of_threads) as pool:
            pool.starmap(merger, [(index_path, notice_files[i], notice_files[i+1], False) for i in range(0, number_of_files, 2)]) 


    logging.info(f"Time to merge all files: {time.time() - seconds} seconds")

    clear_output_folders(index_path)



