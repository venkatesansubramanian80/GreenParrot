from gensim import corpora, models
from topic_mining import preprocess_corpus
from TopicCatagorizer import get_categories

# Step 1: Preprocess corpus
corpus = [
    'Text To Check'
    ]

# ... (preprocess corpus as needed)
corpus = preprocess_corpus(corpus)

# Step 2: Create dictionary and bag-of-words representation
dictionary = corpora.Dictionary(corpus)
corpus_bow = [dictionary.doc2bow(doc) for doc in corpus]

# Step 3: Train LDA model
num_topics = 5  # set number of topics
lda_model = models.LdaModel(corpus_bow, num_topics=num_topics, id2word=dictionary, passes=10)

# Step 4: Interpret LDA model output
for i in range(num_topics):
    topic_words = lda_model.show_topic(i, topn=10)  # get top 10 words for topic i
    print(f"Topic {i}: {[word for word, _ in topic_words]}")  # print top words for topic i
    print(f"Category {i}: {['|'.join(get_categories(word)) for word, _ in topic_words]}")  # print top words for topic i

for doc in corpus_bow:
    topic_probs = lda_model.get_document_topics(doc, minimum_probability=0.0)  # get topic distribution for document
    print(f"Document topics: {topic_probs}")  # print topic distribution for document

print("Fully Completed Successfully")