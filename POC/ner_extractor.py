import nltk
import os

os.environ[os.environ.get('cert_file_default')] = os.environ.get('cert_file_name')
nltk.download('maxent_ne_chunker')

# Sample text
text = 'Text To Check '

# Tokenize the text
tokens = nltk.word_tokenize(text)

# Part-of-speech (POS) tag the tokens
tagged_tokens = nltk.pos_tag(tokens)

# Perform named entity recognition (NER)
ne_tree = nltk.ne_chunk(tagged_tokens)

# Print the named entities
for subtree in ne_tree.subtrees():
    print(subtree.leaves())
