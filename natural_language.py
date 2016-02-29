from nltk.corpus import brown 
def process(sentence):
	for (w1,t1), (w2,t2), (w3,t3) in nltk.trigrams(sentence):
		if (t1.startswith('V') and t2 == 'TO' and t3.startswith('V')):
			print w1, w2, w3

for tagged_sent in brown.tagged_sents(): 
	rocess(tagged_sent)

# Movie Reviews Corpus, categorizes each review as positive or negative
from nltk.corpus import movie_reviews
documents = [(list(movie_reviews.words(fileid)),category)
for category in movie_reviews.categories()
for fileid in movie_reviews.fileids(category)]
random.shuffle(documents)
# feature extraction
all_words = nltk.FreqDist(w.lower() for w in movie_reviews.words())  
# output <FreqDist: ',': 77717, 'the': 76529, '.': 65876, 'a': 38106, 'and': 35576, 'of': 34123, 'to': 31937, "'": 30585, 'is': 25195, 'in': 21822, ...>
word_features = all_words.keys()[:2000]  # construct a list of the 2000 most frequent words in the overall corpus
def document_features(document): # Define a feature extractor
	document_words = set(document) 
	features = {}
	for word in word_features:
		features['contains(%s)' % word] = (word in document_words)
	return features

print document_features(movie_reviews.words('pos/cv957_8737.txt'))




###########################
# Import your own files as corpus
from nltk.corpus import PlaintextCorpusReader
corpus_root = "C:/Data/Files/"
wordlists = PlaintextCorpusReader(corpus_root, '.*\.txt')
wordlists.fileids()[:3]
wordlists.words('rec.autos/101551.txt')

# Tokenize a paragraph into sentences.
para ="Hellow World. It's good to see you. Thanks you for buying this book." 
from nltk.tokenize import sent_tokenize
sents = sent_tokenize(para)
sents
#['Hellow World.', "It's good to see you.", 'Thanks you for buying this book.']

# Tokenize a sentence into words.
from nltk.tokenize import word_tokenize
word_tokenize('Hello World.')
['Hello', 'World', '.']

# Filter stopwords from a sentence
from nltk.corpus import stopwords
english_stops = set(stopwords.words('english'))
words = ["Can't",'is','a','contraction']
[word for word in words if word not in english_stops]
#["Can't",'contraction']

# Create a list of all lowercased words in the text, and then produce a Bigram collection finder.
from nltk.corpus import webtext
from nltk.collocations import BigramCollocationFinder
from nltk.metrics import BigramAssocMeasures
words = [w.lower() for w in webtext.words('grail.txt')]
bcf = BigramCollocationFinder.from_words(words)
bcf.nbest(BigramAssocMeasures.likelihood_ratio, 4)
#[("'", 's'), ('arthur', ':'), ('#', '1'), ("'", 't')]

# Remove the stop words and words length smaller than 3.
from nltk.corpus import stopwords
stopset = set(stopwords.words('english'))
filter_stops = lambda w: len(w) < 3 or w in stopset
bcf.apply_word_filter(filter_stops)
bcf.nbest(BigramAssocMeasures.likelihood_ratio, 4)
#[('black', 'knight'), ('clop', 'clop'), ('head', 'knight'), ('mumble',
'mumble')]


# Trigram
from nltk.collocations import TrigramCollocationFinder
from nltk.metrics import TrigramAssocMeasures
words = [w.lower() for w in webtext.words('singles.txt')]
tcf = TrigramCollocationFinder.from_words(words)
tcf.apply_word_filter(filter_stops)
tcf.apply_freq_filter(3)
tcf.nbest(TrigramAssocMeasures.likelihood_ratio, 4)
[('long', 'term', 'relationship')]


# Performing stemming to remove affixes from a word.
from nltk.stem import PorterStemmer
stemmer = PorterStemmer()
stemmer.stem('cooking')
#'cook'
stemmer.stem('cookery')
#'cookeri'

# LancasterStemmer
#The LancasterStemmer functions just like the PorterStemmer, 
#but can produce slightly different results. It is known to be slightly more aggressive than the PorterStemmer.
from nltk.stem import LancasterStemmer
stemmer = LancasterStemmer()
stemmer.stem('cooking')
#'cook'
stemmer.stem('cookery')
#'cookery'


# Read in file/fileids>>> from nltk.corpus import names
names.fileids()
['female.txt', 'male.txt']
len(names.words('female.txt'))
len(names.words('male.txt'))
   2943

# The NLTK classifiers expect dict style feature sets, so we must therefore transform our text into a dict. 
#The Bag of Words model is the simplest method; it constructs a word presence feature set from all the words of an instance.

def bag_of_words(words):
	return dict([(word,True) for word in words])
bag_of_words(['the', 'quick', 'brown', 'fox'])
#{'quick': True, 'brown': True, 'the': True, 'fox': True}

# Remove frequent words
def bag_of_words_not_in_set(words, badwords):
	return bag_of_words(set(words) - set(badwords))

bag_of_words_not_in_set(['the', 'quick', 'brown', 'fox'],['the'])
#{'quick': True, 'brown': True, 'fox': True}

# Filter stop words
from nltk.corpus import stopwords
	def bag_of_non_stopwords(words, stopfile='english'):
		badwords = stopwords.words(stopfile)
		return bag_of_words_not_in_set(words, badwords)

bag_of_non_stopwords(['the', 'quick', 'brown', 'fox'])
#{'quick': True, 'brown': True, 'fox': True}

# Including significant Bigramfrom nltk.collocations import BigramCollocationFinder
from nltk.metrics import BigramAssocMeasures
def bag_of_bigrams_words(words, score_fn=BigramAssocMeasures.chi_sq,n=200):
	bigram_finder = BigramCollocationFinder.from_words(words)
    bigrams = bigram_finder.nbest(score_fn, n)
    return bag_of_words(words + bigrams)


bag_of_bigrams_words(['the', 'quick', 'brown', 'fox'])
#{'brown': True, ('brown', 'fox'): True, ('the', 'quick'):True, 'fox': True, ('quick', 'brown'): True, 'quick': True,'the': True}





# Training
# 1. create a list of labeled feature sets. label : feature sets.
import collections
def label_feats_from_corpus(corp, feature_detector=bag_of_words):
	label_feats = collections.defaultdict(list)
	for label in corp.categories():
		for fileid in corp.fileids(categories=[label]):
        	feats = feature_detector(corp.words(fileids=[fileid]))
         	label_feats[label].append(feats)
	return label_feats

# 2. construct a list of labeled training instances and testing instances.

def split_label_feats(lfeats, split=0.75):
    train_feats = []
    test_feats = []
    for label, feats in lfeats.iteritems():
    	cutoff = int(len(feats) * split)
        train_feats.extend([(feat, label) for feat in
    feats[:cutoff]])
    	test_feats.extend([(feat, label) for feat in
    feats[cutoff:]])
	return train_feats, test_feats



