#!/usr/bin/python3

import threading, queue, itertools, time
from multiprocessing.dummy import Pool
from urllib.request import urlopen
from urllib.error import HTTPError

__author__ = 'Spencer Bingol'

class HTTP_Manager(threading.Thread):
	def __init__(self, q, games, HTTP_pool_size, SQL_pool_size):
		threading.Thread.__init__(self)
		self._games = games
		self._q = q
		self._HTTP_pool_size = HTTP_pool_size
		self._SQL_pool_size = SQL_pool_size

	def run(self):
		start_time = time.time()
		HTTP_pool = Pool(self._HTTP_pool_size)
		results = HTTP_pool.starmap(self.get_game_files, zip(self._games, itertools.repeat(self._q)))

		HTTP_pool.close()
		HTTP_pool.join()

		for _ in range(self._SQL_pool_size):
			self._q.put('quit')

		end_time = time.time()
		print("HTTP Manager Elapsed Time: {}".format(end_time - start_time))

	def download_file(self, url):
		try:
			u = urlopen(url)
			text = ""
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
		except urllib.error.HTTPError as e:
			report = "HTTP Error on page [{}]: {}".format(url, e)
			print(report)
			with open("errorlog.txt", "a") as logFile:
				logFile.write(report + '\n')
			return None
		except Exception as e:
			report = "Error trying to download [{}]: {}".format(url, e)
			print(report)
			with open("errorlog.txt", "a") as logFile:
				logFile.write(report + '\n')
			return None

	def get_game_files(self, game, q):
		gid = game.split('/')[-2]
		#try:
		#	plays = urlopen(game+"linescore.xml")
		#	pl = urlopen(game+"players.xml")
		#	inn = urlopen(game+"inning/inning_all.xml")
		#except urllib.error.HTTPError as e:
		#	pass
		#else:
		print("DOWNLOADING GAME: {}".format(game))
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
			linescore = linescore.split('>')[2]
			inning_split = inning_all.split('>')
			inning_split[1] = linescore
			inning_all = '>'.join(inning_split)
			q.put([gid, players, inning_all])
		except Exception as e:
			report = "Error trying to prepare the XML [{}]: {}".format(gid, e)
			print(report)
			with open("errorlog.txt", "a") as logFile:
				logFile.write(report + '\n')