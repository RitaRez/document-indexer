import json, os, math, subprocess, re, nltk

from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

nltk.download('stopwords')
nltk.download('punkt')

def preprocesser(text: str) -> list:
    """Does stemming and removes stopwords and punctuation"""
    
    stop_words = set(stopwords.words('english'))
    ps = PorterStemmer()

    text = re.sub(r'[^\w\s]', ' ', text)       #Removes punctuation
    words = word_tokenize(text.lower() )       #Tokenizes the text

    filtered_sentence = []
    for w in words:
        if w not in stop_words:
            filtered_sentence.append(ps.stem(w))

    return filtered_sentence


def get_word_frequency(cleaned_text: list) -> dict:
    """Iterates through the text and returns a dictionary with the word frequency"""
    
    word_freq = dict()
    for word in cleaned_text:
        if word not in word_freq:
            word_freq[word] = 0 
        word_freq[word] += 1

    return word_freq


def breaks_corpus(corpus_path: str, index_path: str, number_of_divisions: int) -> str:
    """Will read the jsonl file and break it into smaller chunks"""

    number_of_documents = 4641784
    size_of_division = math.ceil(number_of_documents / number_of_divisions)

    cmd = f'split --lines={size_of_division} --numeric-suffixes --suffix-length=2 {corpus_path} {index_path + "shards/"}t'
    os.system(cmd)

    return index_path + "/shards/"
          

