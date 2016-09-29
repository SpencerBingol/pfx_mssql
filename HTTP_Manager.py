#!/usr/bin/python3

import threading, queue, os, itertools
from multiprocessing.dummy import Pool
from urllib.request import urlopen

__author__ = 'Spencer Bingol'

class HTTP_Manager(threading.Thread):
	def __init__(self, q, games, HTTP_pool_size, SQL_pool_size):
		threading.Thread.__init__(self)
		self._games = games
		self._q = q
		self._HTTP_pool_size = HTTP_pool_size
		self._SQL_pool_size = SQL_pool_size

	def run(self):
		HTTP_pool = Pool(self._HTTP_pool_size)
		results = HTTP_pool.starmap(self.get_game_files, zip(self._games, itertools.repeat(self._q)))

		HTTP_pool.close()
		HTTP_pool.join()

		for _ in range(self._SQL_pool_size):
			self._q.put('quit')

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
		except Exception as e:
			print("Error trying to download [{}]: {}".format(url, e))

	def get_game_files(self, game, q):
		gid = game.split('/')[-2]
		dir_loc = "data/" + game.split('/')[-5] + "/" + game.split('/')[-4] + "/" + game.split('/')[-3] + "/" + gid + "/"

		try:
			plays = urlopen(game+"plays.xml")
		except urllib.error.HTTPError as e:
			pass
		else:
			if not os.path.exists(dir_loc):
				os.makedirs(dir_loc)

				linescore = self.download_file(game + "linescore.xml")
				players = self.download_file(game + "players.xml")
				inning_all = self.download_file(game + "inning/inning_all.xml")

				linescore = linescore.split('>')[2]
				inning_split = inning_all.split('>')
				inning_split[1] = linescore
				inning_all = '>'.join(inning_split)

				playersLoc = dir_loc + 'players.xml'
				pitchesLoc = dir_loc + 'pitches.xml'

				with open(playersLoc, "w") as playersFile:
					playersFile.write(players)

				with open(pitchesLoc, "w") as pitchesFile:
					pitchesFile.write(inning_all)

				print("DOWNLOADED GAME: {}".format(game))
				q.put([gid, (os.getcwd()+'\\'+playersLoc).replace('/', '\\'), (os.getcwd()+'\\'+pitchesLoc).replace('/', '\\')])
	
