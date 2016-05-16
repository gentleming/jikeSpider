#coding:utf-8

from pymongo import MongoClient
import csv

def Connect_MongoDB():
	# 建立连接
	client = MongoClient('localhost', 27017)
	# 指定将要进行操作的database和collection
	db = client.jikedb
	collection = db.topic_collection
	# 清空现有数据
	# collection.remove({})
	return collection

def Update_MongoDB(collection):
	for row in collection.find({}):
		subscribers = row['subscribers']
		row['subscribers'] = int(subscribers)
		print row['subscribers']
		collection.save(row)

def Query_MongoDB(collection):
	with open('jike.csv','a') as f:
		f_csv = csv.writer(f)
		for row in collection.find({}, {'title':1, 'subscribers':1} ).sort([('subscribers', -1)]):
			# print '%s %s' %(row['title'], row['subscribers'])
			title = row['title'].encode('utf-8')
			subscribers = row['subscribers']
			print title
			print subscribers
			f_csv.writerow([title, subscribers])

def del_same_MongoDB(collection):
	for row in collection.find({}):
		url = row['url']
		if '//m' in url:
			print url
			collection.remove({'url':url})

def main():
	collection = Connect_MongoDB()
	# Update_MongoDB(collection)
	# del_same_MongoDB(collection)
	Query_MongoDB(collection)

if __name__ == '__main__':
	main()


