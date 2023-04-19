import nltk
from nltk.corpus import wordnet

import os

os.environ[os.environ.get('cert_file_default')] = os.environ.get('cert_file_name')


def get_categories(word):
    # Download the WordNet corpus
    nltk.download('wordnet')
    categories = set()
    synsets = wordnet.synsets(word)
    for synset in synsets:
        for hypernym in synset.hypernyms():
            category = hypernym.name().split('.')[0]
            categories.add(category)
    return list(categories)