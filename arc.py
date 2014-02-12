#!/usr/bin/python
#encoding:UTF-8
import Queue
import threading
import time
import warnings
import sys
import urllib2
import urllib
import fileinput
import xml.parsers.expat
from xml.dom.minidom import parse, parseString
from sets import Set
##########importing needed modules for xml marsing
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import parse
#from collections import Counter
import MySQLdb
import datetime


now = datetime.datetime.now()




conn = MySQLdb.connect (host = "localhost",
                        user = "root",
                        passwd = "",
                        db = "arcdb"
                    )




cursor = conn.cursor ()


#cnt = Counter()
#cnt2 = Counter()

titleList = []
categoryList = []
linkList = []
descriptionList = []
pubdateList = []

#print "success"


queue = Queue.Queue()
out_queue = Queue.Queue()



    
class ThreadUrl(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, queue, out_queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue

    def run(self):
        while True:
            #grabs host from queue
            host = self.queue.get()
            try:
            #grabs urls of hosts and then grabs chunk of webpage
                url = urllib2.urlopen(host)
                tree = ET.parse(url)
                chunk = tree.getroot()
            except:
               print chunk 
            
            
            #place chunk into out queue
            self.out_queue.put(chunk)

            #signals to queue job is done
            self.queue.task_done()

class DatamineThread(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, out_queue):
        threading.Thread.__init__(self)
        self.out_queue = out_queue

    def run(self):
        while True:
            #grabs host from queue
            chunk = self.out_queue.get()
            
            #print chunk
            
            for  element in chunk.getiterator():
                
                if element.tag == "item":
                    for subelement in element:
                        #print subelement.tag
                        if subelement.tag == "title":
                            
                            #print "#####################NEW TITLE@@@@@@@@@@@@@"
                            #print subelement.text
                            titleList.append(subelement.text)
                            
                        if subelement.tag == "category":
                            
                            #print "#####################NEW TITLE@@@@@@@@@@@@@"
                            #print subelement.text
                            categoryList.append(subelement.text)
                            
                            
                        if subelement.tag == "link":
#                            print "_______________NEW LINK------------------"
#                            print subelement.text
                            linkList.append(subelement.text)
                            
                            
                        if subelement.tag == "description":
                            #print "_______________NEW DESCRIPTION------------------"
                            #print subelement.text
                            descriptionList.append(subelement.text)
                            
                        if subelement.tag == "pubDate":
#                            print "_______________NEW PUBDATE------------------"
#                            print subelement.text
                            pubdateList.append(subelement.text)
        
            

            #signals to queue job is done
            self.out_queue.task_done()



    
start = time.time()
def main():

    #spawn a pool of threads, and pass them queue instance
    for i in range(5):
        t = ThreadUrl(queue, out_queue)
        t.setDaemon(True)
        t.start()

    #populate queue with data
    for line in fileinput.input(['phoenix.txt']):
        queue.put(line)
        #print line

    for i in range(5):
        dt = DatamineThread(out_queue)
        dt.setDaemon(True)
        dt.start()


    #wait on the queue until everything has been processed
    queue.join()
    out_queue.join()

main()
print "Elapsed Time: %s" % (time.time() - start)





singleList = []
verbList = []
nounList = []

for words in titleList:
    try:
        single = words.split()
        singleList.extend(single)
    except:
        pass


     
#for line in fileinput.input(['verbs.txt']):
#    fixed = line.strip("\n")
#    fixed2 = fixed.strip(" ")
#    verbList.append(fixed2)
    
#for line in fileinput.input(['nouns.txt']):
#    fixed = line.strip("\n")
#    fixed2 = fixed.strip(" ")
#    nounList.append(fixed2)

#print verbList
#print nounList

verbSingles = []
nounSingles = []
    

titleWords = Set(singleList)
verbs = Set(verbList)
nouns = Set(nounList)
narrowVerbs = list(titleWords.intersection(verbs))
narrowNouns = list(titleWords.intersection(nouns))

#print narrowVerbs
#print narrowNouns
verbCount = []
nounCount = []
#print "Title Words: ", titleWords
#print "Verbs      :", verbs

#print "Appearing Verbs: ", narrowVerbs

for item in singleList:
    if item in narrowVerbs:
        #print item
        verbSingles.append(item)

    if item in narrowNouns:
        nounSingles.append(item)
        
#for item in verbSingles:
#    cnt[item] += 1
    

#for item in nounSingles:
#    cnt2[item] += 1
    
#print cnt
tops = []
tops1 = []
nums = []
nums1 = []
#print 'Most common VERBS:'
#for word, count in cnt.most_common(30):
#    print '%s: %7d' % (word, count)
#    tops.append(word)
#   nums.append(count)  
   
#print 'Most common NOUNS:'
 
#for word, count in cnt2.most_common(30):
#    print '%s: %7d' % (word, count)
#    tops1.append(word)
#    nums1.append(count)    
  

#print tops
#print tops1        
#print nums
#print nums1

#print titleList    
titles = open('titles.txt', 'w')
titles2 = open('titles2.txt', 'w')

for item in titleList:
    #print item    
    try:
        titles.write(item.lstrip())
        titles.writelines("\n")
    except:
        pass


tits = []

for line in fileinput.input(['titles.txt']):
    #print line
    titles2.write(line.lstrip() + "\n")
    
for line in fileinput.input(['titles2.txt']):
    #print line
    tits.append(line)
    
#for item in tits:
#	print item    

#print len(titleList)

timeList = []
fetchtime = now.strftime("%Y-%m-%d %H:%M")

for item in tops1:
    timeList.append(fetchtime)


#print timeList


#cursor.execute ("DROP TABLE IF EXISTS verbs")
#cursor.execute ("""
#            CREATE TABLE verbs
#       (
#         item_id INT NOT NULL AUTO_INCREMENT,
#        word         varchar(15),
#        count        varchar(15),
#        pull_order    varchar(15),
#        fetchtime    varchar(25),
#       primary key    (item_id)
#        
#                 ) 
#  """)




#cursor.execute ("DROP TABLE IF EXISTS nouns")
#cursor.execute ("""
#            CREATE TABLE nouns
#       (
#         item_id INT NOT NULL AUTO_INCREMENT,
#       word         varchar(15),
#        count        varchar(15),
#        pull_order    varchar(15),
#        fetchtime    varchar(25),
#       primary key    (item_id)
#        
#                 )
#           
#  """)


#cursor.executemany("""
#    
#                insert ignore into verbs (word, count, fetchtime) values (%s, %s, %s)
#    
#                   """,zip(tops, nums, timeList))
#
#
#
#
#cursor.executemany("""
#    
#                insert ignore into nouns (word, count, fetchtime) values (%s, %s, %s)
#    
#                   """,zip(tops1, nums1, timeList))
#
#
#
#

#print tits


#cursor.execute ("DROP TABLE IF EXISTS articles")

#cursor.execute ("""
#            CREATE TABLE articles
#       (
#         article_id INT NOT NULL AUTO_INCREMENT,
#        titles         varchar(250),
#        category        varchar(150),
#        link            varchar(350),
#        description    TEXT,
#        pubDate        varchar(150),
#        pull_id        varchar(200),
#	UNIQUE		(titles),        
#       primary key    (article_id)
#        
#                 
#                 )
#           
#  """)


cursor.executemany("""


insert ignore into articles (titles, link, pubDate) values (%s, %s, %s)


""", zip(tits, linkList, pubdateList))



conn.commit()

print "Number of rows inserted: %d" % cursor.rowcount
