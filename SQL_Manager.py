#!/usr/bin/python3

import threading, pypyodbc, queue

__author__ = 'Spencer Bingol'

class Game_Importer(threading.Thread):
	def __init__(self, q):
		threading.Thread.__init__(self)
		self._q = q

	def run(self):
		while True:
			game_data = self._q.get()
			if isinstance(game_data, str) and game_data == 'quit':
				break
			if game_data is not None:
				self.import_game(game_data[0], game_data[1], game_data[2])

	def import_game(self, gid, player_data, game_data):
		try:
			auto_commit = True
			cnx = 'Driver={SQL Server};Server=localhost\SQLEXPRESS;Database=PitchFX;Trusted_Connection=True'
			with pypyodbc.connect(cnx, auto_commit) as connection:
				cursor = connection.cursor()
			
				storedProcedure = "EXECUTE PitchFX.dbo.sp_ImportPlayers @xml = ?"
				values = [player_data]
				cursor.execute(storedProcedure, values)

				storedProcedure = "EXECUTE PitchFX.dbo.sp_ImportGame @gid = ?, @xml = ?"
				values = [gid, game_data]
				cursor.execute(storedProcedure, values)

				storedProcedure = "EXECUTE PitchFX.dbo.sp_ImportUmpires @gid = ?, @xml = ?"
				values = [gid, player_data]
				cursor.execute(storedProcedure, values)

				connection.commit()
			
			print("IMPORTED TO SQL: {}".format(gid))
		except Exception as e:
			report = "Failed to import game {}: {}".format(gid, e)
			with open("errors.log", "a") as logFile:
				logFile.write(report + '\n')
			print(report)
			pass

class SQL_Manager(threading.Thread):
	def __init__(self, q, SQL_pool_size):
		threading.Thread.__init__(self)
		self._q = q
		self._SQL_pool_size = SQL_pool_size

	def run(self):
		SQL_pool = []
		for _ in range(self._SQL_pool_size):
			SQL_thread = Game_Importer(self._q)
			SQL_thread.start()
			SQL_pool.append(SQL_thread)

		for SQL_thread in SQL_pool:
			SQL_thread.join()