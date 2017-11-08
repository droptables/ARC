import os, time

while True:
	try:
		os.system("go run harvest-articles.go")
	except:
		print 'failed, sleeping...'
		time.sleep(300)
		pass
	print 'normal sleeping'
	time.sleep(300)