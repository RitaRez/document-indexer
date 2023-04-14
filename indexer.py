import sys, resource, argparse, os, logging, json, os, math, subprocess, time
from preprocesser import breaks_corpus, get_term_lexicon
from utils import index_shard, run_indexer_thread_pool
from collections import OrderedDict 

logging.basicConfig(filename='min_corpus_main.log', level=logging.INFO)


MEGABYTE = 1024 * 1024
def memory_limit(value):
    limit = value * MEGABYTE
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))


def main(memory_limit: str, corpus_path: str, index_path: str, verbose: bool, number_of_threads: int):
    """
    Your main calls should be added here
    """

    logging.info(f"Indexing corpus {corpus_path} to an indexer in {index_path} with {number_of_threads} threads and {memory_limit} MB of memory")

    # seconds = time.time()
    # # Amount of pieces we can manage to process for each thread due to memory limit
    # file_size = int(str(subprocess.check_output(f"du -s {corpus_path}", shell=True), 'utf-8').split("\t")[0])
    # # number_of_divisions = math.ceil((file_size)/(memory_limit*1024)) * number_of_threads
    # number_of_divisions = number_of_threads
    # # number_of_divisions = 1
    
    # logging.info(f"Were divinding the corpus in  {str(number_of_divisions)} pieces")
    # logging.info(f"Time to get the number of divions:  {time.time() - seconds} seconds")

    # seconds = time.time()
    # shards_path = breaks_corpus(corpus_path, index_path, number_of_divisions)
    # logging.info(f"Time to split the corpus: {time.time() - seconds} seconds")


    # seconds = time.time()
    # term_lexicon, number_of_words = get_term_lexicon(corpus_path, index_path)
    # logging.info(f"Time to create lexicon: {time.time() - seconds} seconds")
    # print(number_of_words)

    seconds = time.time()    
    run_indexer_thread_pool(term_lexicon, index_path, number_of_threads)
    logging.info(f"Time to index all shards: {time.time() - seconds} seconds")


    logging.info(f"------------------------------------------------------------------------------------------------------------------------------------")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        '-m',
        dest='memory_limit',
        action='store',
        required=True,
        type=int,
        help='memory available'
    )
    parser.add_argument('-c',dest='corpus_path',action='store',required=True,type=str)
    parser.add_argument('-i',dest='index_path',action='store',required=True,type=str)
    parser.add_argument('-v',dest='verbose',action='store',required=False,type=bool,default=False)
    parser.add_argument('-t',dest='number_of_threads',action='store',required=False,type=int,default=8)

    args = parser.parse_args()
    memory_limit(args.memory_limit)
    try:
        main(args.memory_limit, args.corpus_path, args.index_path, args.verbose, args.number_of_threads)
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)
        logging.info(f"MEMORY ERROR")
        logging.info(f"------------------------------------------------------------------------------------------------------------------------------------")



# You CAN (and MUST) FREELY EDIT this file (add libraries, arguments, functions and calls) to implement your indexer
# However, you should respect the memory limitation mechanism and guarantee
# it works correctly with your implementation