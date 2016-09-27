import urllib.parse, urllib.error, os, re, sys, getopt, pypyodbc
from urllib.request import urlopen
from datetime import datetime, timedelta
from bs4 import BeautifulSoup, SoupStrainer

__author__ = 'Spencer Bingol'


def generate_daterange(start_date, end_date):
	dt = start_date
	delta = timedelta(days=1)
	dates = []

	while dt <= end_date:
		dates.append("year_" + str(dt.year) + "/" + "month_" + ("0" + str(dt.month))[-2:] + "/" + "day_" + ("0" + str(dt.day))[-2:] + "/")
		dt += delta

	return dates

def import_game_to_SQL(gid, dir_loc):
	try:
		connection = pypyodbc.connect('Driver={SQL Server};Server=localhost\SQLEXPRESS;Database=PitchFX;Trusted_Connection=True')
		cursor = connection.cursor()
		
		storedProcedure = "EXECUTE PitchFX.dbo.InsertGame_FromXML @gid = ?, @file_dir = ?"
		values = [gid, dir_loc]
		
		#print(storedProcedure)
		#print(gid)
		#print(dir_loc)

		cursor.execute(storedProcedure, values)
		connection.commit()
		connection.close()
		print("IMPORTED TO SQL: {}".format(gid))
	except Exception as e:
		print("Failed to import game {} from '{}': {}".format(gid, dir_loc, e))
		pass

def download_file(dir_url, dir_loc, file_name, progress_bar=True):
	file_url = dir_url + file_name
	file_loc = dir_loc + file_name

	f = open(file_loc, 'wb')

	try:
		u = urlopen(file_url)
		#meta = u.info()

		file_size = int(u.headers['Content-Length'])
		#if progress_bar: 
		#	print("Downloading: {} Bytes: {}".format(file_name, file_size))
		file_size_dl = 0
		block_sz = 8192
	
		while True:
			buffer = u.read(block_sz)
			if not buffer:
				break

			file_size_dl += len(buffer)
			f.write(buffer)
			
			#if progress_bar:
			#	status = r"%10d [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
			#	status = status + chr(8)*(len(status)+1)
			#	print(status),

		f.close()
	except Exception as e:
		f.close()
		print("Error trying to download [{}]: {}".format(file_url, e))

def download_dir(dir_url, dir_loc, dir_name, file_name_pattern = "[0-9][0-9][0-9][0-9][0-9][0-9].xml"):
	dir_url = dir_url + dir_name
	dir_loc = dir_loc + dir_name

	#print("Downloading: {} ".format(dir_name))
	if not os.path.exists(dir_loc):
		os.makedirs(dir_loc)

	url_path = urlopen(dir_url)
	string = url_path.read().decode('utf-8')

	pattern = re.compile(file_name_pattern)
	file_list = pattern.findall(string)

	for file_name in file_list: 
		download_file(dir_url, dir_loc, file_name, False)

	url_path.close()


def download_game_files(dir_url):
	gid = dir_url.split('/')[-2]
	dir_loc = "data/" + dir_url.split('/')[-5] + "/" + dir_url.split('/')[-4] + "/" + dir_url.split('/')[-3] + "/" + gid + "/"

	try:
		plays = urlopen(dir_url+"plays.xml")
	except urllib.error.HTTPError as e:
		pass
	else:
		if not os.path.exists(dir_loc):
			#print("DOWNLOADING GAME: {}".format(dir_url))
			os.makedirs(dir_loc)
			os.makedirs(dir_loc+"inning/")

			download_file(dir_url, dir_loc, "linescore.xml")
			download_file(dir_url, dir_loc, "plays.xml")
			download_file(dir_url, dir_loc, "players.xml")
			download_file(dir_url, dir_loc, "inning/inning_all.xml")
			download_file(dir_url, dir_loc, "inning/inning_hit.xml")
			print("DOWNLOADED GAME: {}".format(dir_url))
			return dir_loc


def download_date_games(dir_url):
	try: 
		response = urlopen(dir_url)
	except:
		print("Failed while attempting to open the location [{}]".format(dir_url))
	
	links = SoupStrainer('a')
	soup = BeautifulSoup(response.read(), "html.parser", parse_only=links)

	for link in soup:
		if link['href'].startswith('gid_'):
			gid = link['href']
			dir_loc = download_game_files(dir_url + gid)
			if dir_loc is not None:
				#print((os.getcwd()+'\\'+dir_loc).replace('/', '\\'))
				import_game_to_SQL(gid.replace('/', ''), (os.getcwd()+'\\'+dir_loc).replace('/', '\\'))

def main(argv):
	gameday_url = "http://gd2.mlb.com/components/game/mlb/"
	usage = 'Usage: downloader.py -s <yyyy-mm-dd> -e <yyyy-mm-dd> -i [0/1]'
	
	try:
		opts, args = getopt.getopt(argv, "s:e:")
	except:
		print(usage)
		print("   -s: yyyy-mm-dd formatted date, the START DATE of the range to download.")
		print("   -e: yyyy-mm-dd formatted date, the END DATE of the range to download.")
		sys.exit(2)

	start_date = 0
	end_date = 0
	for opt, arg in opts:
		if opt == '-s':
			try:
				start_date = datetime.strptime(arg, '%Y-%m-%d').date()
			except:
				print("Invalid start date. {}".format(usage))
				sys.exit(3)
		elif opt == '-e':
			try:
				end_date = datetime.strptime(arg, '%Y-%m-%d').date()
			except:
				print("Invalid end date. {}".format(usage))
				sys.exit(3)

	if start_date == 0 or end_date == 0:
		print("both bounds of date range, and import bit, required")
		print(usage)
		sys.exit(3)

	print("Start Date: {}".format(start_date.strftime("%B %d, %Y")))
	print("End Date: {}".format(end_date.strftime("%B %d, %Y")))

	dates = generate_daterange(start_date, end_date)
	
	for date_url in dates:
		download_date_games(gameday_url + date_url)

	sys.exit()

# Begin the main function when run
if __name__ == "__main__":
	main(sys.argv[1:])