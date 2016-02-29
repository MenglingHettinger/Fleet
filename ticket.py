import nltk
import string
import os
import collections
import re
import numpy as np
import ast
import pandas as pd
import itertools as it

from sklearn import metrics
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.stem.porter import PorterStemmer
from nltk.stem import LancasterStemmer
from nltk.corpus import PlaintextCorpusReader
from nltk.collocations import BigramCollocationFinder
from nltk.metrics import BigramAssocMeasures
from nltk.collocations import TrigramCollocationFinder
from nltk.metrics import TrigramAssocMeasures
from nltk.corpus import stopwords
from nltk.probability import FreqDist, ConditionalFreqDist
from nltk.classify import MaxentClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn import preprocessing
from sklearn.feature_extraction import DictVectorizer
from sklearn.cross_validation import train_test_split
from sklearn.ensemble import RandomForestClassifier as RF
from sklearn.multiclass import OneVsRestClassifier
from sklearn.metrics import confusion_matrix
from sklearn import tree
from sklearn.externals.six import StringIO 
from sklearn.neighbors import NearestNeighbors
"""

def get_equip_equip(inPath,equip_map,outPath):

	use old data to extract the map between equipment and entity-sid and apply to the new data, 
	for the ones can not detected in the above method, we then build a list with all possible equipment,
	then check if entity contains the string. If not, then assign the equipment to unknow.

	outFile = open(outPath,'w')
	outFile.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
				'sys','tktNm','crePriority','creWrkList','actWrkList','rpt_chr','createDate','crTime','resolvDate','rsTime',
				'entity','entity_remove','entity_clean','equip','firstCond','lastCond','sID','ne_Lang','channel','source','lastFClrUser','threshType','wfa_TR',
				'msg1','msg2','note','resCode','resText','restTextAdd'))
	counter = 0
	print('equip_map',type(equip_map),len(equip_map))


	equipList = ['lsc','alit','ama','clk','cp','senders bsy','rcan failure','serv loss','congestion','isolation',
	'no paths','cpu','dli','dsf','lgc','fp','ltc','mcc','mdc','mic','mp','power','pwr','rng','rpl','rsu','slc',
	'ss7','tpp','tst','aic','ali','cogn','iptrusta','regen scheduled','backup','fault code','bldg','blk','ccg',
	'fault astn','channel','ne direct','cp flt','digital path','dipflt','diu','dlu','dly stat','dst sw',
	'em fault','emg','ewsd','door','ext alm','hf archive','filesys','sg oper','man svewsd','ost una',
	'group switch fault','isolation','lost','lss','ltg','man busy','man sv','mdd','metallic test errors','mod',
	'nstart','omt','overeng','parse''power','route','rsu','software errors','slct','sp unit flt','lkset','swinv',
	'system operator','tech support','test assist','dpc','x25link','annc','extalm','dtc','rotl tgn','ss','umbcl',
	'unknown message','parse']

	#print equip_map['pcrktxpcdc0']
	#print equip_map['mrcdtxmedc0']
	with open(inPath,'Ur') as inFile:
		for line in inFile:
			if line.startswith('Sys'):
				continue

			sys,tktNm,crePriority,creWrkList,actWrkList,rpt_chr,createDate,crTime,resolvDate,rsTime,entity,firstCond,lastCond,sID,ne_Lang,channel,source,lastFClrUser,threshType,wfa_TR,msg1,msg2, note,resCode,resText,resTextAdd = line.strip().split(',')
			entity = entity.strip()
			entity = re.sub("[^a-zA-Z]"," ", entity)
			sID = re.sub("[^a-zA-Z]"," ", sID)
			msg1 = re.sub("[^a-zA-Z]"," ", msg1)
			print("before",msg1)
			msg1 = msg1.lower()
			print("after", msg1)
			entity = ' '.join(entity.split())
			sID = ' '.join(sID.split())
			msg1 = ' '.join(msg1.split())
			resCode = resCode.lower()
			entity_remove = re.sub(sID," ", entity)
			entity_remove = ' '.join(entity_remove.split())
			entity_clean = ''
			words = entity_remove.strip().split(" ")
			for word in words:
				if 100>len(word)>1:
					entity_clean = entity_clean + ' '+ word
			try:
				equip = equip_map[entity_clean]
				#print entity,equip
			except:
				for substr in equipList:
					if substr in entity_clean:
						equip = substr.upper()
						#print substr
						#print equip
						break
					else:
						equip = 'UNKNOWN'
			
			#if entity_clean not in equip_map:
			#	print equip

			outFile.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
			sys,tktNm,crePriority,creWrkList,actWrkList,rpt_chr,createDate,crTime,resolvDate,rsTime,
			entity,entity_remove,entity_clean,equip,firstCond,lastCond,sID,ne_Lang,channel,source,lastFClrUser,threshType,wfa_TR,
			msg1,msg2,note,resCode,resText,resTextAdd))

	outFile.close()


def parse_target(inPath,outPath):
	stemmer = LancasterStemmer()
	outFile = open(outPath,'w')
    outFile.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % 
    	('sys','tktNm','crePriority','creWrkList','actWrkList','rpt_chr','createDate','crTime','resolvDate','rsTime',
    	'entity','equip','firstCond','lastCond','sID','ne_Lang','channel','source','lastFClrUser','threshType','wfa_TR',
    	'msg1','msg2','note','resCode','resText','resTextAdd'))
	with open(inPath,'Ur') as inFile:
		for line in inFile:
			equip = ''
			if line.startswith('Sys'):
				continue
			else:
				#items = line.strip().split(',')
				#print len(items)
				
				sys,tktNm,crePriority,creWrkList,actWrkList,rpt_chr,createDate,crTime,resolvDate,rsTime,entity,firstCond,lastCond,sID,ne_Lang,channel,source,lastFClrUser,threshType,wfa_TR,msg1,msg2, note,resCode,resText,resTextAdd = line.strip().split(',')
				entity = entity.replace("/"," ")
				entity = entity.replace("-"," ")
				entity = entity.replace(":"," ")
				entity = entity.replace("="," ")
				entity = entity.replace("_"," ")
				firstCond = firstCond.replace("/"," ")
				firstCond = firstCond.replace("-"," ")
				firstCond = firstCond.replace(":"," ")
				firstCond = firstCond.replace("="," ")
				firstCond = firstCond.replace("_"," ")
				' '.join(entity.split())
				' '.join(firstCond.split())
				print(entity)
				items = entity.split(' ')
				items = filter(None, items)
				#print items,len(items)
				equip = ''
				for i in range(len(items)):
					k = [] 
					if len(items[i])>=11 and len(items[i])<=16:
						#office = items[i]
						for j in range(i+1, len(items)):
							if any(char.isdigit() for char in items[j]):
								k.append(j)
								#print k
								equip = items[k[0]-1]
								break
							else:
								equip = equip + ' '+ items[j]
					elif len(items[i])>17:
						#office = items[i][0:17]
						equip = items[i][17:]
				equip =  equip.strip()





			outFile.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
				sys,tktNm,crePriority,creWrkList,actWrkList,rpt_chr,createDate,crTime,resolvDate,rsTime,
				entity,equip,firstCond,lastCond,sID,ne_Lang,channel,source,lastFClrUser,threshType,wfa_TR,
				msg1,msg2,note,resCode,resText,resTextAdd))
    outFile.close()


def parse_target_2(inPath,outPath):
	outFile = open(outPath,'w')
	outFile.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % ('sys','tktNm','crePriority','creWrkList','actWrkList','rpt_chr','createDate','crTime','resolvDate','rsTime',
				'entity','equip','firstCond','lastCond','sID','ne_Lang','channel','source','lastFClrUser','threshType','wfa_TR',
				'msg1','msg2','note','resCode','resText','resTextAdd'))


	name_dict = {'senders bsy':'cpe','rcan failure':'cpe','serv loss':'cpe','proc':'cpe','dtf':'ds',
	'function not mounted':'mp','pgm mismatch':'mp','cogn':'annc','iptrusta':'audit','regen scheduled':'backup',
	'bkup info':'backup','mts regen':'backup','eqpt':'bldg','blk':'blkd','fault astn':'cdu_flt','ne direct':'channel',
	'door':'ewsd','hf archive':'file','filesys':'file','sg oper':'file','snococ failures':'front end','sn fail':'front end',
	'ost una':'front end','failure witho':'front end','man svewsd':'front end','ost una':'front end',
	'group switch fault':'gs fault','man sv':'manual','metallic test errors':'mlt','software errors':'sftware',
	'sp unit flt':'sp fault','lkset':'ss7','tech support':'tech supt','dpc':'trunking'}

	counter = 0
	counter_line = 0
	equipList = ['lsc','alit','ama','clk','cp','senders bsy','rcan failure','serv loss','congestion','isolation',
	'no paths','cpu','dli','dsf','lgc','fp','ltc','mcc','mdc','mic','mp','power','pwr','rng','rpl','rsu','slc',
	'ss7','tpp','tst','aic','ali','cogn','iptrusta','regen scheduled','backup','fault code','bldg','blk','ccg',
	'fault astn','channel','ne direct','cp flt','digital path','dipflt','diu','dlu','dly stat','dst sw',
	'em fault','emg','ewsd','door','ext alm','hf archive','filesys','sg oper','man svewsd','ost una',
	'group switch fault','isolation','lost','lss','ltg','man busy','man sv','mdd','metallic test errors','mod',
	'nstart','omt','overeng','parse''power','route','rsu','software errors','slct','sp unit flt','lkset','swinv',
	'system operator','tech support','test assist','dpc','x25link']


	with open(inPath,'Ur') as inFile:




		for line in inFile:
			#equip = ''
			counter_line += 1
			if line.startswith('Sys'):
				continue
			else:				
				sys,tktNm,crePriority,creWrkList,actWrkList,rpt_chr,createDate,crTime,resolvDate,rsTime,entity,firstCond,lastCond,sID,ne_Lang,channel,source,lastFClrUser,threshType,wfa_TR,msg1,msg2, note,resCode,resText,resTextAdd = line.strip().split(',')
				entity = re.sub("[^a-zA-Z]"," ", entity)
				' '.join(entity.split())
				print(entity)
				for key in name_dict:
					if key in entity:
						entity = re.sub(key,name_dict[key],entity)
				print(entity)
				
				for substr in equipList:
					if substr in entity:
						equip = substr
						print(equip)
					else:
						equip = 'unknown'


			outFile.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
				sys,tktNm,crePriority,creWrkList,actWrkList,rpt_chr,createDate,crTime,resolvDate,rsTime,
				entity,equip,firstCond,lastCond,sID,ne_Lang,channel,source,lastFClrUser,threshType,wfa_TR,
				msg1,msg2,note,resCode,resText,resTextAdd))

	outFile.close()
"""

def msg_vec_generate(inPath,vec_method="Count",min_frequency=10):
	stemmer = LancasterStemmer()
	msg_list = []
	counter = 0
	with open("ticket_Feb2015_equip.csv",'Ur') as inFile:
		for line in inFile:
			if line.startswith('sys'):
				continue
			sys,tktNm,crePriority,creWrkList,actWrkList,rpt_chr,createDate,crTime,resolvDate,rsTime,entity,entity_remove,entity_clean,equip,firstCond,lastCond,sID,ne_Lang,channel,source,lastFClrUser,threshType,wfa_TR,msg1,msg2,note,resCode,resText,resTextAdd,day = line.strip().split(',')
			
			msg = ''

			msg1 = re.sub("[^a-zA-Z]"," ", msg1)
			msg1 = msg1.strip().lower()
			msg1 = ' '.join(msg1.split())

			msgTokens = nltk.word_tokenize(msg1)
			stopset = set(stopwords.words('english'))
			for token in msgTokens:
				if len(token) > 2 and token not in stopset and token != 'msg' and token != equip:
					token_stem = stemmer.stem(token)
					msg = msg+" "+token_stem
					msg = msg.strip()
			msg_list.append(msg)

		if vec_method == "Count":
			vectorizer = CountVectorizer(analyzer = "word",tokenizer = None,preprocessor = None, stop_words = None,max_features = 5000,min_df = min_frequency,ngram_range = (1,3)) 
		elif vec_method == "Tfidf":
			vectorizer = TfidfVectorizer(analyzer = "word",tokenizer = None,preprocessor = None, stop_words = None,max_features = 5000,min_df = min_frequency) 
		else:
			print("vec_method")
		msg_vec_features = vectorizer.fit_transform(msg_list)
		msg_vec_features = msg_vec_features.toarray()
		print("msg_vec_feature_shape",msg_vec_features.shape)
		vocab = vectorizer.get_feature_names()
		print('vocabulary starts here ...',vocab)
		print(msg_vec_features,type(msg_vec_features),msg_vec_features.shape)
		return msg_vec_features

		
def model_selection(inPath,msg_vec_array,outPath):
	outFile =  open(outPath,'w')
	outFile.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % 
		('sys','tktNm','crePriority','creWrkList','actWrkList','rpt_chr','createDate','crTime','resolvDate','rsTime',
		'entity','equip','firstCond','lastCond','sID','ne_Lang','channel','source',
		'lastFClrUser','threshType','wfa_TR','msg1','msg2','note','resCode','resText','resTextAdd'))
	print(msg_vec_array,type(msg_vec_array),msg_vec_array.size)
	print(msg_vec_array[:,0],type(msg_vec_array[:,0]),msg_vec_array[:,0].size)


	equip_list = []
	code_list = []
	cond_list = []
	day_list = []
	creWrkList_list = []
	actWrkList_list = []
	crePriority_list = []
	sys_list = []
	with open(inPath,'Ur') as inFile:
		for line in inFile:
			if line.startswith('sys'):
				continue
			sys,tktNm,crePriority,creWrkList,actWrkList,rpt_chr,createDate,crTime,resolvDate,rsTime,entity,entity_remove,entity_clean,equip,firstCond,lastCond,sID,ne_Lang,channel,source,lastFClrUser,threshType,wfa_TR,msg1,msg2,note,resCode,resText,resTextAdd,day = line.strip().split(',')
			
			equip = re.sub("[^a-zA-Z]"," ", equip)
			equip = equip.strip().lower()
			equip_list.append(equip)
			#print equip_list

			resCode = re.sub("[^a-zA-Z]"," ", resCode)
			resCode = resCode.strip().lower()
			if resCode != 'almclr' and resCode != 'rts' and resCode !='techsup':
				resCode = "other"
			#if resCode != 'almclr'
			#	resCode = "other"
			outFile.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % 
				(sys,tktNm,crePriority,creWrkList,actWrkList,rpt_chr,createDate,crTime,resolvDate,rsTime,
					entity,equip,firstCond,lastCond,sID,ne_Lang,channel,source,
					lastFClrUser,threshType,wfa_TR,msg1,msg2,note,resCode,resText,resTextAdd,day))


			code_list.append(resCode.strip())

			firstCond = re.sub("[^a-zA-Z]"," ", firstCond)
			firstCond = firstCond.strip().lower()
			cond = ''
			firstCond_list = firstCond.strip().split(" ")
			for i in firstCond_list:
				if len(i)>2:
					cond = cond + ' ' + i
			cond_list.append(cond.strip())

			day = re.sub("[^a-zA-Z]"," ", day)
			day = day.strip().lower()
			day_list.append(day)

			creWrkList = re.sub("[^a-zA-Z]"," ", creWrkList)
			creWrkList = creWrkList.strip().lower()
			creWrkList_list.append(creWrkList)

			actWrkList = re.sub("[^a-zA-Z]"," ", actWrkList)
			actWrkList = actWrkList.strip().lower()
			actWrkList_list.append(actWrkList)

			crePriority = crePriority.strip().lower()
			crePriority_list.append(crePriority)

			sys = re.sub("[^a-zA-Z]"," ", sys)
			sys = sys.strip().lower()
			sys_list.append(sys)


		equip_unique = list(set(equip_list))
		cond_unique = list(set(cond_list))
		resCode_unique = list(set(code_list))	
		creWrkList_unique = list(set(creWrkList_list))
		actWrkList_unique = list(set(actWrkList_list))
		sys_unique = list(set(sys_list))
		crePriority_unique = list(set(crePriority_list))
		code_dict = dict([(name,i) for i,name in enumerate(resCode_unique)])
		print(code_dict)

		equip_array = np.array([equip_list])

		cond_array = np.array([cond_list])
		day_array = np.array([day_list])
		creWrkList_array = np.array([creWrkList_list])
		actWrkList_array = np.array([actWrkList_list])
		crePriority_array = np.array([crePriority_list])
		sys_array = np.array([sys_list])
		print("size check",cond_array.shape,creWrkList_array.shape,actWrkList_array.shape)
		features = np.concatenate((equip_array,cond_array,day_array,creWrkList_array,actWrkList_array,crePriority_array,sys_array))
		
		features_df = pd.DataFrame(features)
		features_df = pd.DataFrame.transpose(features_df)

		vec = DictVectorizer()
		features_vec = vec.fit_transform(features_df.to_dict('records')).toarray()


		#print(features,features.size,features[0].size, features.ndim)
		features_vec_df = pd.DataFrame(features_vec)
		#print(features_df,features_df.size)
	

		#print(msg_vec_array,msg_vec_array.size,msg_vec_array[0].size, msg_vec_array.ndim)
		msg_vec_df = pd.DataFrame(msg_vec_array)

		#print(msg_vec_df,msg_vec_df.size)
		features_all = pd.concat([features_vec_df,msg_vec_df],axis=1)
		
	
		#print("features_all",features_all,features_all.size)

		vec = DictVectorizer()
		features_all_array  = vec.fit_transform(features_all.to_dict('records')).toarray()

		code = [code_dict[x] for x in code_list]
		#print("code_list",code_list)
		#print("code",code)
		code_df = pd.DataFrame(code)
		data = pd.concat([features_all,code_df],axis=1)
		data2 = pd.concat([features_df,msg_vec_df,code_df],axis=1)
		data.to_csv(path_or_buf="text_binary.csv", sep=',')
		data2.to_csv(path_or_buf="text_token.csv", sep=',')
		## Start training



		print("Training the Naive Bayes...")
		
		x_train, x_test, y_train, y_test = train_test_split(features_all_array, code, test_size=0.33, random_state=42)
		#print("x_train",x_train,"y_train",y_train,"x_test",x_test,"y_test",y_test)

		nb = MultinomialNB(alpha=1,fit_prior=True).fit(x_train, y_train)
		nb_pred = nb.predict(x_test)
		print(metrics.classification_report(y_test, nb_pred,target_names=resCode_unique))
		print(confusion_matrix(y_test, nb_pred))
		#print((nb_pred(x_test) == y_test).mean())
		"""
		print("Training the Random Forest...")

		n_trees = 500
		criteria = ['gini', 'entropy']
		max_feature_params = ["sqrt", "log2"]
		max_depth_params = [10,15,20,25,30,40]
		class_weight_params = ["auto","subsample",None]
		parameter_space = it.product(criteria,max_feature_params, max_depth_params,class_weight_params)

		accuracies = {}
		verbose = 1
		n_jobs = 1
		for criterium, max_feature_param, max_depth_param,class_weight_param in parameter_space:
			rf = RF(n_estimators = n_trees, max_features=max_feature_param,verbose=verbose, 
					n_jobs=n_jobs,max_depth=max_depth_param,class_weight=class_weight_param)
			#clf_fit = clf.fit(x, y, sample_weight = balance_weights(y))
			rf.fit(x_train, y_train)
			rf_pred = rf.predict(x_test)
			
			importances = rf.feature_importances_
			std = np.std([rf.feature_importances_ for tree in rf.estimators_],
             axis=0)
			indices = np.argsort(importances)[::-1]
			print("Feature ranking:")
			for f in range(10):
				print("%d. feature %d (%f)" % (f + 1, indices[f], importances[indices[f]]))
			
			print([(criterium, max_feature_param, max_depth_param,class_weight_param)])
			print(metrics.classification_report(y_test, rf_pred,target_names=resCode_unique))
			print(confusion_matrix(y_test, rf_pred))
			accuracies[(criterium, max_feature_param, max_depth_param,class_weight_param)] = (rf.predict(x_test) == y_test).mean()
		print(accuracies)

		print("training the decision tree....")
		dt = tree.DecisionTreeClassifier()
		dt.fit(x_train,y_train)
		dt_pred = dt.predict(x_test)
		tree.export_graphviz(dt,out_file='tree.dot') 
		print(metrics.classification_report(y_test, dt_pred,target_names=resCode_unique))
		print(confusion_matrix(y_test, dt_pred))
		"""
		print("Training the KNN...")
		
		x_train, x_test, y_train, y_test = train_test_split(features_all_array, code, test_size=0.33, random_state=42)
		#print("x_train",x_train,"y_train",y_train,"x_test",x_test,"y_test",y_test)

		knn = NearestNeighbors(n_neighbors=2, algorithm='ball_tree').fit(x_train,y_train)
		knn_pred = knn.predict(x_test)
		print(metrics.classification_report(y_test, knn_pred,target_names=resCode_unique))
		print(confusion_matrix(y_test, knn_pred))
		#print((nb_pred(x_test) == y_test).mean())




def main():
	#get_equip_equip("ticket_Feb2015.csv",equip_map_entity_remove_sid,"ticket_Feb2015_equip.csv")
	#get_equip_equip("ticket_Jan2015.csv",equip_map_entity_remove_sid,"ticket_Jan2015_equip.csv")
	msg_vec_array = msg_vec_generate("ticket_Feb2015_equip.csv",vec_method="Tfidf",min_frequency=10)
	model_selection('ticket_Feb2015_equip.csv',msg_vec_array,'ticket_Feb2015_equip_comb.csv')




if __name__ == "__main__":
    main()


				








