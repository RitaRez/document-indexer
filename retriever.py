
def get_word_id_byte_start(word, lexicon_path):
    with open(lexicon_path, 'r') as lexicon_file:
        for line in lexicon_file:
            word_id, word = line.split()
            if word == word:
                return word_id
        return -1

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
    with open(index_path + "final_index", 'rb') as index_file:
        index_file.seek(byte_start)
        byte_array = index_file.read(byte_end - byte_start)
        retrieved_word_id, retrieved_docs = decode_from_binary(byte_array)

        print(retrieved_word_id)
        print(retrieved_docs)


#zero 253666 32110668 32111342

retrieve_word_index("../output/", 253666, 32110668, 32111342)