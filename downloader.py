#!/usr/bin/python3

import urllib.parse, urllib.error, os, re, sys, getopt, queue, pypyodbc
from urllib.request import urlopen
from datetime import datetime, timedelta
from bs4 import BeautifulSoup, SoupStrainer
from multiprocessing.dummy import Pool
from HTTP_Manager import HTTP_Manager
from SQL_Manager import SQL_Manager

__author__ = 'Spencer Bingol'
			
def generate_daterange(start_date, end_date, gameday_url):
	""" Derives a list of directories using the MLBAM's known file structure. """
	
	delta = timedelta(days=1)
	dt = start_date

	dates = []
	while dt <= end_date:
		dates.append("{}year_{:04d}/month_{:02d}/day_{:02d}/".format(gameday_url, dt.year, dt.month, dt.day))
		dt += delta

	return dates

def check_game_urls (games, connection_string):
	""" Accepts a list of (game_url, gid) tuples, eliminates those that exist in SQL, and returns the list of remaining URLs. """

	gid_exists = [] # Games in the list that already exist
	auto_commit = True
	with pypyodbc.connect(connection_string, auto_commit) as connection:
		cursor = connection.cursor()
		gids = [g[1] for g in games] 

		query_gids = []
		while len(gids) > 0:		# Loop workaround for limitations in parameterized query (allows maximum of 2100 parameters) 
			if len(gids) >= 2000:	# Query threw error at 2098 for too many parameters, so cut it off at 2000 anyway.
				query_gids = gids[:1999]
				gids = gids[2000:]
			else: 
				query_gids = gids
				gids = []
			query = "SELECT gid FROM game WHERE gid IN ("
			query = query + "?, "*len(query_gids)
			query = query[:-2] + ")"

			cursor.execute(query, query_gids)
			gid_exists = gid_exists + cursor.fetchall() # anything returned here already exists in DB
	return list(set([g[0] for g in games if g[1] not in [e[0] for e in gid_exists] and 'int' not in g[1]])) # return all distinct directory URLs where the corresponding gid not in gid_exists 

def get_games_on_date(directory):
	""" Returns a list of (game directory, gid) tuples from MLBAM's website falling on date. """
	
	games = []
	try: 
		response = urlopen(directory)			
	except: 
		print("Failed while attempting to open the location [{}]".format(dir_url))
	
	links = SoupStrainer('a') # Parse all links in the HTML
	soup = BeautifulSoup(response.read(), "html.parser", parse_only=links)
	
	for link in soup:
		if link['href'].startswith('gid_'): # Look for any link indicating a game id
			games.append([directory + link['href'], link['href'].replace('/', '')])

	return games

def get_game_urls(daterange, connection_string, HTTP_pool_size):
	""" Takes the list of directories based on date range, finds all games on those dates, and eliminates those already in the DB. """
	
	game_urls = []
	HTTP_Pool = Pool(HTTP_pool_size)							# Use threads to make more effecient
	date_lists = HTTP_Pool.map(get_games_on_date, daterange)	# Returns a list of URLs each time

	for dt in date_lists:
		game_urls += dt

	HTTP_Pool.close()
	HTTP_Pool.join()

	print("Number of unchecked games is {}".format(len(game_urls)))
	return check_game_urls(game_urls, connection_string)

def main(argv):
	"""Validates and processes the command line arguments, determines a list of urls to request, and begins the programs threads."""
	gameday_url = "http://gd2.mlb.com/components/game/mlb/"
	connection_string = "Driver={SQL Server};Server=localhost\SQLEXPRESS;Database=PitchFX;Trusted_Connection=True"
	usage = """Usage 1: downloader.py -s <yyyy-mm-dd> -e <yyyy-mm-dd>
Usage 2: downloader.py -y"""
	
	HTTP_pool_size  = 5 # Number of threads to run to handle HTTP Requests
	SQL_pool_size = 10	# Number of threads to run to handle the SQL Import
	
	try:
		opts, args = getopt.getopt(argv, "s:e:y")
	except:
		print(usage)
		print("   -s: yyyy-mm-dd formatted date, the START DATE of the range to download.")
		print("   -e: yyyy-mm-dd formatted date, the END DATE of the range to download.")
		print("   -y: A separate, standalone command that indicates to pull all of YESTERDAY's games.")
		sys.exit(2)

	start_date = 0
	end_date = 0
	for opt, arg in opts:
		if opt == '-y': # If parameter is YESTERDAY, calculate a date range of yesterday
			start_date = datetime.now() - timedelta(days=1)
			end_date = datetime.now() - timedelta(days=1)
			break
		elif opt == '-s':
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
		print("both bounds of date range required")
		print(usage)
		sys.exit(3)

	print("Start Date: {}".format(start_date.strftime("%B %d, %Y")))
	print("End Date: {}".format(end_date.strftime("%B %d, %Y")))

	dates = generate_daterange(start_date, end_date, gameday_url) 	# Create a list of MLBAM Website date directories to search
	games = get_game_urls(dates, connection_string, HTTP_pool_size) # Create a list of individual game directories to process
	print("GAMES TO IMPORT: {}".format(len(games)))

	q = queue.Queue()	# This object is filled by HTTP_thread, and emptied by SQL_thread
	SQL_thread = SQL_Manager(q, SQL_pool_size, connection_string)
	HTTP_thread = HTTP_Manager(q, games, HTTP_pool_size, SQL_pool_size)

	SQL_thread.start()
	HTTP_thread.start()

	sys.exit() # Exit once starting threads

# Enter the main function on run
if __name__ == "__main__":
	main(sys.argv[1:])