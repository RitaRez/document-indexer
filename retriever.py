import math, re
from nltk.stem import PorterStemmer

def preprocesser(word):
    """Does stemming and removes stopwords and punctuation"""
    
    text = re.sub(r'\n|\r', ' ', word)       #Removes breaklines
    text = re.sub(r'[^\w\s]', ' ', word)       #Removes punctuation

    ps = PorterStemmer()
    word = ps.stem(word)

    return word

def get_word_byte_start(word, index_path) -> (str, str, str, str):
    """Binary search for word in lexicon file."""    

    word = preprocesser(word)
    
    with open(index_path + "term_lexicon.txt", 'r') as lexicon_file:
        lexicon = lexicon_file.read().splitlines()
        low = 0; high = len(lexicon) - 1

        while (low <= high):
            mid = math.floor((low + high)/2)
            current_word = lexicon[mid].split(" ")[0]
            
            if (word == current_word):
                return lexicon[mid].split(" ")
            
            elif (word > current_word):
                low = mid + 1
            
            else:
                high = mid - 1

    return [None] * 4


def decode_from_binary(byte_array):
    word_id = int.from_bytes(byte_array[:4], "big")
    number_of_docs = int.from_bytes(byte_array[4:8], "big")
    docs = []
    for i in range(number_of_docs):
        doc_id = int.from_bytes(byte_array[8 + i*6:12 + i*6], "big")
        freq = int.from_bytes(byte_array[12 + i*6:14 + i*6], "big")
        docs.append({"id": doc_id, "freq": freq})
    return word_id, docs


def retrieve_word_index(index_path: str, word_id, byte_start: int, byte_end: int) -> dict:
    with open(index_path + "inverted_index", 'rb') as index_file:
        index_file.seek(byte_start)
        byte_array = index_file.read(byte_end - byte_start)
        retrieved_word_id, retrieved_docs = decode_from_binary(byte_array)

        print(retrieved_word_id)
        print(retrieved_docs)


if __name__ == "__main__":
    
    word, word_id, byte_start, byte_offset = get_word_byte_start("working", "../output/")
    print(word, word_id, byte_start, byte_offset)

    byte_start = int(byte_start)
    byte_offset = int(byte_offset)

    retrieve_word_index("../output/", word_id, byte_start, byte_offset)