#!/usr/bin/python3

import threading, queue, itertools, time, urllib
from multiprocessing.dummy import Pool
from urllib.request import urlopen
from urllib.error import HTTPError

__author__ = 'Spencer Bingol'

class HTTP_Manager(threading.Thread):
	""" This class manages the thread pool that collects game XML using HTTP Requests and inserts them into the Queue. """

	def __init__(self, q, games, HTTP_pool_size, SQL_pool_size):
		threading.Thread.__init__(self)
		self._games = games
		self._q = q
		self._HTTP_pool_size = HTTP_pool_size
		self._SQL_pool_size = SQL_pool_size  # Necessary to add the correct number of escape keywords to the queue.

	def run(self):
		start_time = time.time()	# Simple timer - Start time
		HTTP_pool = Pool(self._HTTP_pool_size)
		results = HTTP_pool.starmap(self.get_game_files, zip(self._games, itertools.repeat(self._q)))	# Create the pool, distribute game URLs to threads

		HTTP_pool.close()
		HTTP_pool.join()

		for _ in range(self._SQL_pool_size):	# Add an escape keyword for each SQL thread
			self._q.put('quit')

		end_time = time.time()		# Simple timer - End time
		print("HTTP Manager Elapsed Time: {}".format(end_time - start_time))	# Print elapsed time

	def download_file(self, url):
		""" Given the URL of an XML file, attempt to download using a buffer and return content. """
		try:
			text = ""
			with urlopen(url) as u:
				file_size = int(u.headers['Content-Length'])
				file_size_dl = 0
				block_sz = 8192
		
				while True:
					buffer = u.read(block_sz)
					if not buffer:
						break

					file_size_dl += len(buffer)
					text = text + buffer.decode('utf-8')

			return text
		# HTTPError almost always thrown on a suspended, postponed, or cancelled game, so not often something to worry about.
		except urllib.error.HTTPError as e:
			report = "HTTP Error on page [{}]: {}".format(url, e)
			print(report)
			with open("errorlog.txt", "a") as logFile:
				logFile.write(report + '\n')	# Write to the error log file
			return None
		except Exception as e:
			report = "Error trying to download [{}]: {}".format(url, e)
			print(report)
			with open("errorlog.txt", "a") as logFile:
				logFile.write(report + '\n')	# Write to the error log file
			return None

	def get_game_files(self, game, q):
		""" Given the URL of an individual game's web directory, download & process its XML, and insert it into the Queue. """

		gid = game.split('/')[-2]	# Parse gid from URL
		print("DOWNLOADING GAME: {}".format(game)) # All three files below must download to continue.

		linescore = self.download_file(game + "linescore.xml")	
		if linescore is None:	
			return
		players = self.download_file(game + "players.xml")
		if players is None:
			return
		inning_all = self.download_file(game + "inning/inning_all.xml")
		if inning_all is None:
			return
		
		try:
			# Only thing needed from the linescore file is the <game> element
			# <game> element in inning_all is unnecessary for these purposes, so just swap them.
			linescore = linescore.split('>')[2]	# linescore file has a copyright comment, so <game> is [2]. 
			inning_split = inning_all.split('>')
			inning_split[1] = linescore	# inning_all does not have a copyright comment, so <game> is [1].
			inning_all = '>'.join(inning_split)
			q.put([gid, players, inning_all]) # put (gid, players.xml, [modified] inning_all.xml) tuple into queue for SQL Threads.
		except Exception as e:
			report = "Error trying to prepare the XML [{}]: {}".format(gid, e)
			print(report)
			with open("errorlog.txt", "a") as logFile:
				logFile.write(report + '\n')	# Write to the error log file