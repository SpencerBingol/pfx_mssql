#!/usr/bin/python3

import urllib.parse, urllib.error, os, re, sys, getopt, queue, pypyodbc
from urllib.request import urlopen
from datetime import datetime, timedelta
from bs4 import BeautifulSoup, SoupStrainer
from HTTP_Manager import HTTP_Manager
from SQL_Manager import SQL_Manager

__author__ = 'Spencer Bingol'
			
def generate_daterange(start_date, end_date, gameday_url):
	dt = start_date
	delta = timedelta(days=1)
	dates = []

	while dt <= end_date:
		dates.append(gameday_url + "year_" + str(dt.year) + "/" + "month_" + ("0" + str(dt.month))[-2:] + "/" + "day_" + ("0" + str(dt.day))[-2:] + "/")
		dt += delta

	return dates

def get_game_urls(daterange, connection_string):
	games = []
	
	for directory in daterange:
		try: 
			response = urlopen(directory)			
		except: 
			print("Failed while attempting to open the location [{}]".format(dir_url))

		links = SoupStrainer('a')
		soup = BeautifulSoup(response.read(), "html.parser", parse_only=links)

		for link in soup:
			if link['href'].startswith('gid_'):
				games.append([directory + link['href'], link['href'].replace('/', '')])

	gid_exists = []
	auto_commit = True
	with pypyodbc.connect(connection_string, auto_commit) as connection:
		cursor = connection.cursor()
		gids = [g[1] for g in games]

		query_gids = []
		while len(gids) > 0:
			if len(gids) > 2100:
				query_gids = gids[:2099]
				gids = gids[2100:]
			else: 
				query_gids = gids
				gids = []
			query = "SELECT gid FROM game WHERE gid IN ("
			query = query + "?, "*len(query_gids)
			query = query[:-2] + ")"

			cursor.execute(query, query_gids)
			gid_exists = gid_exists + cursor.fetchall()
	return [g[0] for g in games if g[1] not in [e[0] for e in gid_exists] and 'int' not in g[1]]

def main(argv):
	gameday_url = "http://gd2.mlb.com/components/game/mlb/"
	usage = """Usage 1: downloader.py -s <yyyy-mm-dd> -e <yyyy-mm-dd>
Usage 2: downloader.py -y"""
	connection_string = 'Driver={SQL Server};Server=localhost\SQLEXPRESS;Database=PitchFX;Trusted_Connection=True'
	SQL_pool_size = 10
	HTTP_pool_size  = 5
	
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
		if opt == '-y':
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

	dates = generate_daterange(start_date, end_date, gameday_url)
	games = get_game_urls(dates, connection_string)
	print("GAMES TO IMPORT: {}".format(len(games)))


	q = queue.Queue()
	SQL_thread = SQL_Manager(q, SQL_pool_size, connection_string)
	HTTP_thread = HTTP_Manager(q, games, HTTP_pool_size, SQL_pool_size)

	SQL_thread.start()
	HTTP_thread.start()

	sys.exit()

# Begin the main function when run
if __name__ == "__main__":
	main(sys.argv[1:])