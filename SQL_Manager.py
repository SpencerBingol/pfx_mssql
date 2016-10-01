#!/usr/bin/python3

import threading, pypyodbc, queue
from xml.etree import ElementTree

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

				cursor.execute("SELECT * FROM game WHERE gid = ?", [gid])
				if len(cursor.execute("SELECT * FROM game WHERE gid = ?", [gid]).fetchall()) == 0:
					players = ElementTree.ElementTree(ElementTree.fromstring(player_data)).getroot()
					game = ElementTree.ElementTree(ElementTree.fromstring(game_data)).getroot()

					self.players_to_SQL(cursor, gid, players) # Insert new players into DB
					#self.teams_to_SQL(cursor, gid, game) # Insert new teams into DB

					# Import game data
					storedProcedure = "EXECUTE PitchFX.dbo.sp_ImportGame @gid = ?, @xml = ?"
					values = [gid, game_data]
					cursor.execute(storedProcedure, values)

					self.umpires_to_SQL(cursor, gid, players) # Insert new umpires into DB, update game record with umpires

					connection.close()
					print("IMPORTED TO SQL: {}".format(gid))
		except Exception as e:
			report = "Failed to import game {}: {}".format(gid, e)
			with open("errors.log", "a") as logFile:
				logFile.write(report + '\n')
			print(report)
			pass

	def teams_to_SQL (self, cursor, gid, games):
		# Import either team if it doesn't already exist
		team_values = []
		teams_with = """WITH teams
AS (
"""
		for game in games.findall('./game'):
			teams_with = teams_with + """	SELECT ? AS id, ? AS abbreviation, ? AS file_code, ? AS city, ? AS name
	UNION
	SELECT ? AS id, ? AS abbreviation, ? AS file_code, ? AS city, ? AS name
	UNION
"""
			team_values.append(game.attrib['away_team_id'])
			team_values.append(game.attrib['away_name_abbrev'])
			team_values.append(game.attrib['away_file_code'])
			team_values.append(game.attrib['away_team_city'])
			team_values.append(game.attrib['away_team_name'])
			team_values.append(game.attrib['home_team_id'])
			team_values.append(game.attrib['home_name_abbrev'])
			team_values.append(game.attrib['home_file_code'])
			team_values.append(game.attrib['home_team_city'])
			team_values.append(game.attrib['home_team_name'])
		teams_with = teams_with[:-8] + """
)
"""

		team_insert = teams_with + """INSERT INTO team (id, abbreviation, file_code, city, name)
"""
		team_insert = team_insert + """SELECT t.id, t.abbreviation, t.file_code, t.city, t.name
FROM teams t 
LEFT OUTER JOIN team tm ON t.id = tm.id
WHERE tm.id IS NULL;"""
		cursor.execute(team_insert, team_values)

	def players_to_SQL (self, cursor, gid, players):
		# Import any player records that don't already exist
		player_values = []
		players_with = """WITH players 
AS (
"""
		for player in players.findall('./team/player'):
			players_with = players_with + """	SELECT ? AS id, ? AS first_name, ? AS last_name, ? AS bats, ? AS throws
	UNION
"""
			player_values.append(player.attrib['id'])
			player_values.append(player.attrib['first'])
			player_values.append(player.attrib['last'])
			player_values.append(player.attrib['bats'])
			player_values.append(player.attrib['rl'])
		players_with = players_with[:-8] + """
)
"""
		
		player_insert = players_with + """INSERT INTO player (id, first_name, last_name, bats, throws)
"""
		player_insert = player_insert + """SELECT p.id, p.first_name, p.last_name, p.bats, p.throws
FROM players p 
LEFT OUTER JOIN player pl ON p.id = pl.id
WHERE pl.id IS NULL;"""
		cursor.execute(player_insert, player_values)

	def umpires_to_SQL (self, cursor, gid, players):
		# Import new umpires
		umpires_values = []
		ump_hp = ''
		ump_1b = ''
		ump_2b = ''
		ump_3b = ''
		umpires_with = """WITH umpires
AS (
"""
		for umpire in players.findall('./umpires/umpire'):
			umpires_with = umpires_with + """	SELECT ? AS id, ? AS first_name, ? AS last_name
UNION
"""
			umpires_values.append(umpire.attrib['id'])
			umpires_values.append(umpire.attrib['first'])
			umpires_values.append(umpire.attrib['last'])

			if umpire.attrib['position'] == 'home':
				ump_hp = umpire.attrib['id']
			if umpire.attrib['position'] == 'first':
				ump_1b = umpire.attrib['id']
			if umpire.attrib['position'] == 'second':
				ump_2b = umpire.attrib['id']
			if umpire.attrib['position'] == 'third': 
				ump_3b = umpire.attrib['id']
		umpires_with = umpires_with[:-8] + """
)
"""	
		umpire_insert = umpires_with + """INSERT INTO umpire (id, first_name, last_name)
"""
		umpire_insert = umpire_insert + """SELECT u.id, u.first_name, u.last_name
FROM umpires u 
LEFT OUTER JOIN umpire ump ON u.id = ump.id
WHERE ump.id IS NULL;"""
		cursor.execute(umpire_insert, umpires_values)

		#Update game record to identify the specific umpires
		umpire_update = """UPDATE game
SET umpire_home = ?, umpire_first = ?, umpire_second = ?, umpire_third = ?
WHERE gid = ?;"""
		umpires_values = []
		umpires_values.append(ump_hp)
		umpires_values.append(ump_1b)
		umpires_values.append(ump_2b)
		umpires_values.append(ump_3b)
		umpires_values.append(gid)

		cursor.execute(umpire_update, umpire_values)

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