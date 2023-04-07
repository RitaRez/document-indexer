import sys, resource, argparse, os
import json, os, math, subprocess, time

from preprocesser import breaks_corpus
from utils import index_shard, create_thread_pool

MEGABYTE = 1024 * 1024
def memory_limit(value):
    limit = value * MEGABYTE
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))



# def thread_creator(nb_threads = 8) -> None:
#     """Iterates through all notice ids and downloads the notices"""

    
#     threads = []
#     for thread_id in range(nb_threads):
#         print(f"Thread {thread_id} will go from {right_range} to {left_range + 1}")
#         t = Thread(target=, 
#                    args=()
#                    )
#         threads.append(t)
#         t.start()

#     for t in threads:
#         t.join()



def main(memory_limit: str, corpus_path: str, index_path: str):
    """
    Your main calls should be added here
    """

    number_of_threads = 8

    seconds = time.time()
    cmd = f"du -s {corpus_path}"
    file_size = int(str(subprocess.check_output(cmd, shell=True), 'utf-8').split("\t")[0])
    number_of_divisions = math.ceil((1.5*file_size)/(memory_limit*1024))              #Amount of pieces we can manage to process
    number_of_divisions = number_of_divisions * number_of_threads                     #What we can process to each thread
    print("Were divinding the corpus in " + str(number_of_divisions) + " pieces")
    print(f"Time to get the number of divions: {time.time() - seconds} seconds")


    # seconds = time.time()
    # shards_path = breaks_corpus(corpus_path, index_path, number_of_divisions)
    # print(f"Time to split the corpus: {time.time() - seconds} seconds")

    # seconds = time.time()
    # index_shard(2, 2, "../output/shards/t00", index_path)
    # print(f"Time to index one shard of the  corpus: {time.time() - seconds} seconds")
    
    create_thread_pool(index_path, number_of_threads)

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
    parser.add_argument(
        '-c',
        dest='corpus_path',
        action='store',
        required=True,
        type=str
    )
    parser.add_argument(
        '-i',
        dest='index_path',
        action='store',
        required=True,
        type=str
    )

    args = parser.parse_args()
    memory_limit(args.memory_limit)
    try:
        main(args.memory_limit, args.corpus_path, args.index_path)
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)


# You CAN (and MUST) FREELY EDIT this file (add libraries, arguments, functions and calls) to implement your indexer
# However, you should respect the memory limitation mechanism and guarantee
# it works correctly with your implementation