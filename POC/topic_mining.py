import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
import os

os.environ[os.environ.get('cert_file_default')] = os.environ.get('cert_file_name')
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

def preprocess_corpus(corpus):
    """
    Preprocesses a corpus of text by tokenizing, lowercasing, removing stop words, and lemmatizing.
    Returns a list of lists of tokens for each document in the corpus.
    """
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()
    tokenized_corpus = []
    for doc in corpus:
        tokens = word_tokenize(doc.lower())
        tokens = [token for token in tokens if token not in stop_words]
        tokens = [lemmatizer.lemmatize(token) for token in tokens]
        tokenized_corpus.append(tokens)
    return tokenized_corpus
