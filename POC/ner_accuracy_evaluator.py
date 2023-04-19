import nltk

# Load the tagged corpus
tagged_corpus = nltk.corpus.treebank.tagged_sents()

# Split the corpus into training and testing sets
train_size = int(len(tagged_corpus) * 0.8)
train_sents = tagged_corpus[:train_size]
test_sents = tagged_corpus[train_size:]

# Train the tagger using multiple POS taggers
unigram_tagger = nltk.UnigramTagger(train_sents)
bigram_tagger = nltk.BigramTagger(train_sents, backoff=unigram_tagger)
trigram_tagger = nltk.TrigramTagger(train_sents, backoff=bigram_tagger)

# Evaluate the tagger using the test set
accuracy_score = trigram_tagger.evaluate(test_sents)
print(test_sents)
print(train_sents)
print("Accuracy score:", accuracy_score)
print("Completed Successfully")