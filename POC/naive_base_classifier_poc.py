#import nltk and pandas libraries
import nltk
import pandas as pd
import random
import pickle
from nltk.corpus import wordnet as wn

#def function to save classifier to a pickle file
def save_classifier(classifier, file_name):
    '''
    :param classifier: The trained classifier
    :param file_name: Pickle file name
    :return: None
    '''
    f = open(file_name, 'wb')
    pickle.dump(classifier, f)
    f.close()

#def function to load classifier from a pickle file
def load_classifier(file_name):
    '''
    :param file_name: Pickle file name
    :return:  The trained classifier
    '''
    f = open(file_name, 'rb')
    classifier = pickle.load(f)
    f.close()
    return classifier

#Define a feature extractor function that counts the freequency of each word in the title
def feature_extractor(title):
    title_words = set(title)
    features = {}
    for word in all_words:
        features['contains({})'.format(word)] = (word in title_words)
    return features

#Def method to get ambiguous words for the text using Word sense disambiguation
def get_ambiguous_words(text):
    '''
    :param text: The text to be disambiguated
    :return: The list of ambiguous words
    '''
    #Get the list of ambiguous words
    ambiguous_words = [word for word in text if len(wn.synsets(word)) > 1]
    return ambiguous_words

#read the news_info.csv file
df = pd.read_csv('news_info.csv')

#Get list of all the titles from the csv file to a list
titles = df['Title'].tolist()

#Random shuffle the list
random.shuffle(titles)

#Get the list of ambiguous words
ambiguous_words = get_ambiguous_words([word.lower() for title in titles for word in nltk.word_tokenize(title)])

#Perform and get the frequency distribution of the words
all_words = nltk.FreqDist([word.lower() for title in titles for word in nltk.word_tokenize(title)])

#Get the feature set for each title
featuresets = [(feature_extractor(title), category) for (title, category) in zip(titles, df['sentiment'].tolist())]

#Split the data into training and testing data
train_set, test_set = featuresets[int(round((len(featuresets)/2),0)):], featuresets[:int(round((len(featuresets)/2),0))]

#Train the classifier
classifier = nltk.NaiveBayesClassifier.train(train_set)

#Print the accuracy of the classifier
print(nltk.classify.accuracy(classifier, test_set))

#Print the most informative features
classifier.show_most_informative_features(10)

#Print the sentiment of the title
print(classifier.classify(feature_extractor("Bill Gross Scoops up Regional Bank Stocks".split())))