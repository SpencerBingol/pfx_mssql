#!/usr/bin/python3

import threading, pypyodbc, queue, time
from xml.etree import ElementTree

__author__ = 'Spencer Bingol'

class SQL_Manager(threading.Thread):
	""" This class manages the thread pool that pulls XML info from the Queue and puts into the DB. """

	def __init__(self, q, SQL_pool_size, connection_string):
		threading.Thread.__init__(self)
		self._q = q
		self._SQL_pool_size = SQL_pool_size
		self._connection_string = connection_string

	def run(self):
		start_time = time.time()	# Simple timer - Start time
		SQL_pool = []
		for _ in range(self._SQL_pool_size):	# Create the requested number of threads
			SQL_thread = Game_Importer(self._q, self._connection_string)
			SQL_thread.start()
			SQL_pool.append(SQL_thread)

		for SQL_thread in SQL_pool:
			SQL_thread.join()	# Join the pool

		end_time = time.time()	# Simple timer - End time
		print("SQL Manager Elapsed Time: {0:.2f} seconds".format(end_time - start_time)) # Print elapsed time

class Game_Importer(threading.Thread):
	### Thread class that receives a section of the queue of game information. ###

	def __init__(self, q, connection_string):
		threading.Thread.__init__(self)
		self._q = q
		self._connection_string = connection_string

	def run(self):
		while True:	# Run forever, until hitting the escape keyword (quit)
			game_data = self._q.get()	# Pull from Queue
			if isinstance(game_data, str) and game_data == 'quit':	# Evaluate for escape keyword
				break
			if game_data is not None:
				self.import_game(self._connection_string, game_data[0], game_data[1], game_data[2])

	def import_game(self, connection_string, gid, player_data, game_data):
		""" Given a connection string and game's XML data, execute the inserts/updates to import the game into SQL. """

		try:
			auto_commit = True
			connection = pypyodbc.connect(connection_string, auto_commit)
			with connection.cursor() as cursor:
				players = ElementTree.ElementTree(ElementTree.fromstring(player_data)).getroot()
				game = ElementTree.ElementTree(ElementTree.fromstring(game_data)).getroot()

				self.players_to_SQL(cursor, gid, players) # Insert new players into DB
				self.teams_to_SQL(cursor, gid, game) # Insert new teams into DB
				self.game_to_SQL(cursor, gid, game) # Insert new game record into DB
				self.atbats_to_SQL(cursor, gid, game) # Insert new atbat records into DB
				self.pitches_to_SQL(cursor, gid, game) # Insert new pitch records into DB
				self.umpires_to_SQL(cursor, gid, players) # Insert new umpires into DB, update game record with umpires

			connection.close()
			print("IMPORTED TO SQL: {}".format(gid))
		except Exception as e:
			report = "Failed to import game {}: {}".format(gid, e)
			with open("errorlog.txt", "a") as logFile:
				logFile.write(report + '\n')	# Write to error log file
			print(report)
			pass

	def players_to_SQL (self, cursor, gid, players):
		""" Import player records that don't already exist, update those that had missing information. """

		values = []	# List of parameters for query
		query_with = "WITH players AS ("	# WITH Clause for query
		for player in players.findall('./team/player'):
			query_with += "SELECT ? AS id, ? AS first_name, ? AS last_name, ? AS bats, ? AS throws UNION "
			values.append(player.attrib['id'])
			values.append(player.attrib['first'])
			values.append(player.attrib['last'])
			if 'bats' in player.attrib:
				values.append(player.attrib['bats'])
			else: 
				values.append(None)
			if 'rl' in player.attrib:
				values.append(player.attrib['rl'])
			else: 
				values.append(None)
		query_with = query_with[:-7] + ") "

		merge = query_with + "MERGE player AS t USING players AS s ON t.id = s.id "
		merge += "WHEN NOT MATCHED BY TARGET THEN INSERT (id, first_name, last_name, bats, throws) VALUES (s.id, s.first_name, s.last_name, s.bats, s.throws) "
		merge += "WHEN MATCHED AND ((t.first_name IS NULL AND s.first_name IS NOT NULL) OR (t.last_name IS NULL AND s.last_name IS NOT NULL) OR (t.bats IS NULL AND s.bats IS NOT NULL) OR (t.throws IS NULL AND s.throws IS NOT NULL)) THEN UPDATE SET t.first_name = s.first_name, t.last_name = s.last_name, t.bats = s.bats, t.throws = s.throws;"

		cursor.execute(merge, values)

	def teams_to_SQL (self, cursor, gid, games):
		""" Import either team if it doesn't already exist. """

		values = []
		query_with = "WITH teams AS ("
		for game in games.findall('.'):
			query_with += "SELECT ? AS id, ? AS abbreviation, ? AS file_code, ? AS city, ? AS name UNION SELECT ? AS id, ? AS abbreviation, ? AS file_code, ? AS city, ? AS name UNION "
			values.append(game.attrib['away_team_id'])
			values.append(game.attrib['away_name_abbrev'])
			values.append(game.attrib['away_file_code'])
			values.append(game.attrib['away_team_city'])
			values.append(game.attrib['away_team_name'])
			values.append(game.attrib['home_team_id'])
			values.append(game.attrib['home_name_abbrev'])
			values.append(game.attrib['home_file_code'])
			values.append(game.attrib['home_team_city'])
			values.append(game.attrib['home_team_name'])
		query_with = query_with[:-7] + ")"

		merge = query_with + "MERGE team AS t USING teams AS s ON t.id = s.id "
		merge += "WHEN NOT MATCHED BY TARGET THEN INSERT (id, abbreviation, file_code, city, name) VALUES (s.id, s.abbreviation, s.file_code, s.city, s.name) "
		merge += "WHEN MATCHED AND ((t.abbreviation IS NULL AND s.abbreviation IS NOT NULL) OR (t.file_code IS NULL AND s.file_code IS NOT NULL) OR (t.city IS NULL AND s.city IS NOT NULL) OR (t.name IS NULL AND s.name IS NOT NULL)) THEN UPDATE SET t.abbreviation = s.abbreviation, t.file_code = s.file_code, t.city = s.city, t.name = s.name;"
		
		cursor.execute(merge, values)

	def umpires_to_SQL (self, cursor, gid, players):
		""" Import umpires that didn't already exist, and update the game record to include them. """
		ump_hp = ''
		ump_1b = ''
		ump_2b = ''
		ump_3b = ''

		values = []
		query_with = "WITH umpires AS ("
		for umpire in players.findall('./umpires/umpire'):
			query_with += "SELECT ? AS id, ? AS first_name, ? AS last_name UNION "

			values.append(umpire.attrib['id'])
			if 'first' in umpire.attrib:
				values.append(umpire.attrib['first'])
			elif 'name' in umpire.attrib:
				values.append(umpire.attrib['name'].split(' ')[0])
			else:
				values.append(None)
			if 'last' in umpire.attrib:
				values.append(umpire.attrib['last'])
			elif 'name' in umpire.attrib:
				values.append(umpire.attrib['name'].split(' ')[1])
			else:
				values.append(None)

			if umpire.attrib['position'] == 'home':
				ump_hp = umpire.attrib['id']
			if umpire.attrib['position'] == 'first':
				ump_1b = umpire.attrib['id']
			if umpire.attrib['position'] == 'second':
				ump_2b = umpire.attrib['id']
			if umpire.attrib['position'] == 'third': 
				ump_3b = umpire.attrib['id']
		query_with = query_with[:-7] + ")"

		merge = query_with + "MERGE umpire AS t USING umpires AS s ON t.id = s.id "
		merge += "WHEN NOT MATCHED BY TARGET THEN INSERT (id, first_name, last_name) VALUES (s.id, s.first_name, s.last_name) "
		merge += "WHEN MATCHED AND ((t.first_name IS NULL AND s.first_name IS NOT NULL) OR (t.last_name IS NULL AND s.last_name IS NOT NULL)) THEN UPDATE SET t.first_name = s.first_name, t.last_name = s.last_name;"

		cursor.execute(merge, values)

		#Update game record to identify the specific umpires
		update = "UPDATE game SET umpire_home = ?, umpire_first = ?, umpire_second = ?, umpire_third = ? WHERE gid = ?;"
		values = []
		values.append(ump_hp)
		values.append(ump_1b)
		values.append(ump_2b)
		values.append(ump_3b)
		values.append(gid)

		cursor.execute(update, values)

	def game_to_SQL(self, cursor, gid, games):
		""" Import the game record from the parsed XML. """
		
		values = []
		query_with = "WITH games AS ("
		for game in games.findall('.'):
			query_with += "SELECT ? AS gid, ? AS time_date, ? AS game_pk, ? AS venue, ? AS venue_id, ? AS away_team_id, ? AS home_team_id, ? AS home_time, ? AS home_time_zone, ? AS game_nbr, ? AS home_team_runs, ? AS away_team_runs, ? AS is_perfect_game, ? AS is_no_hitter, ? AS [des] UNION "
			values.append(gid)
			if 'time_date' in game.attrib:
				values.append(game.attrib['time_date'])
			else:
				try:
					game.attrib['time'].strptime(input, '%H:%M')
					values.append(game.attrib['id'][:10] + " " + game.attrib['time'])
				except:
					values.append(game.attrib['id'][:10])
					pass
			values.append(game.attrib['game_pk'])
			values.append(game.attrib['venue'])
			values.append(game.attrib['venue_id'])
			values.append(game.attrib['away_team_id'])
			values.append(game.attrib['home_team_id'])
			values.append(game.attrib['home_time'])
			values.append(game.attrib['home_time_zone'])
			if 'game_nbr' in game.attrib:
				values.append(game.attrib['game_nbr'])
			else: 
				values.append(gid[-1])
			if 'home_team_runs' in game.attrib:
				values.append(game.attrib['home_team_runs'])
			else:
				values.append(None)
			if 'away_team_runs' in game.attrib:
				values.append(game.attrib['away_team_runs'])
			else:
				values.append(None)
			if 'is_perfect_game' in game.attrib:
				values.append(game.attrib['is_perfect_game'])
			else:
				values.append(None)
			if 'is_no_hitter' in game.attrib:
				values.append(game.attrib['is_no_hitter'])
			else:
				values.append(None)
			if 'description' not in game.attrib: 
				values.append(None)
			else:
				values.append(game.attrib['description'])
		query_with = query_with[:-7] + ")"

		insert = query_with + "INSERT INTO game (gid, game_date, game_pk, venue, venue_id, away_team_id, home_team_id, home_time, home_time_zone, game_nbr, home_team_runs, away_team_runs, is_perfect_game, is_no_hitter, [description]) "
		insert += "SELECT g.gid, g.time_date, g.game_pk, g.venue, g.venue_id, g.away_team_id, g.home_team_id, g.home_time, g.home_time_zone, g.game_nbr, g.home_team_runs,  g.away_team_runs, g.is_perfect_game, g.is_no_hitter, g.des FROM games g;"
		
		cursor.execute(insert, values)

	def atbats_to_SQL(self, cursor, gid, games):
		""" Import the atbats within a game from the parsed XML. """
		
		atbats = []
		for inning in games.findall('./inning'):
			inn_num = inning.attrib['num']
			for half in ['top', 'bottom']:
				for atbat in inning.findall('./' + half + '/atbat'):
					half_inn = 0
					if (half == "bottom"):
						half_inn = 1

					current_atbat = []
					current_atbat.append(gid)
					current_atbat.append(inn_num)
					current_atbat.append(half_inn)
					current_atbat.append(atbat.attrib['num'])
					current_atbat.append(atbat.attrib['b'])
					current_atbat.append(atbat.attrib['s'])
					current_atbat.append(atbat.attrib['o'])
					current_atbat.append(atbat.attrib['start_tfs_zulu'])
					current_atbat.append(atbat.attrib['batter'])
					current_atbat.append(atbat.attrib['stand'])
					current_atbat.append(atbat.attrib['b_height'])
					current_atbat.append(atbat.attrib['pitcher'])
					current_atbat.append(atbat.attrib['p_throws'])
					current_atbat.append(atbat.attrib['des'])
					current_atbat.append(atbat.attrib['des_es'])
					if 'event_num' in atbat.attrib:
						current_atbat.append(atbat.attrib['event_num'])
					else: 
						current_atbat.append(None)
					if 'event' in atbat.attrib:
						current_atbat.append(atbat.attrib['event'])
					else: 
						current_atbat.append(None)
					if 'event_es' in atbat.attrib:
						current_atbat.append(atbat.attrib['event_es'])
					else: 
						current_atbat.append(None)
					if 'home_team_runs' in atbat.attrib:
						current_atbat.append(atbat.attrib['home_team_runs'])
					else: 
						current_atbat.append(None)
					if 'away_team_runs' in atbat.attrib:
						current_atbat.append(atbat.attrib['away_team_runs'])
					else: 
						current_atbat.append(None)
					atbats.append(current_atbat)

		insert = "INSERT INTO atbat (game_id, inning, top_bot, num, b, s, o, start_tfs_zulu, batter, stand, b_height, pitcher, p_throws, [des], des_es, event_num, [event], event_es, home_team_runs, away_team_runs) "		
		insert += "SELECT a.game_id, a.inning, a.top_bot, a.num, a.b, a.s, a.o, a.start_tfs_zulu, a.batter, a.stand, a.b_height, a.pitcher, a.p_throws, a.[des], a.des_es, a.event_num, a.[event], a.event_es, a.home_team_runs, a.away_team_runs FROM atbats a;"
		while len(atbats) > 0:
			query_with = "WITH atbats AS ("
			if len(atbats) >= 100:
				query_atbats = atbats[:99]
				atbats = atbats[100:]
			else: 
				query_atbats = atbats
				atbats = []

			values = []
			for ab in query_atbats:
				values += ab
				query_with += "SELECT ? AS game_id, ? AS inning, ? AS top_bot, ? AS num, ? AS b, ? AS s, ? AS o, ? AS start_tfs_zulu, ? AS batter, ? AS stand, ? AS b_height, ? AS pitcher, ? AS p_throws, ? AS des, ? AS des_es, ? AS event_num, ? AS event, ? AS event_es, ? AS home_team_runs, ? AS away_team_runs UNION ALL "
			query_with = query_with[:-11] + ")"

			cursor.execute(query_with + insert, values)
	
	def pitches_to_SQL(self, cursor, gid, games):
		""" Import the individual pitches from the parsed XML. """

		pitches = []
		for atbat in games.findall('.//atbat'):
			atbat_num = atbat.attrib['num']

			for pitch in atbat.findall('pitch'):

				current_pitch = []
				current_pitch.append(atbat_num)
				current_pitch.append(pitch.attrib['des'])
				current_pitch.append(pitch.attrib['des_es'])
				current_pitch.append(pitch.attrib['id'])
				current_pitch.append(pitch.attrib['type'])
				current_pitch.append(pitch.attrib['tfs'])
				current_pitch.append(pitch.attrib['tfs_zulu'])
				current_pitch.append(pitch.attrib['x'])
				current_pitch.append(pitch.attrib['y'])
				if 'event_num' in pitch.attrib:
					current_pitch.append(pitch.attrib['event_num'])
				else:
					current_pitch.append(None)
				if 'sv_id' in pitch.attrib:
					current_pitch.append(pitch.attrib['sv_id'])
				else:
					current_pitch.append(None)
				if 'play_guid' in pitch.attrib:
					current_pitch.append(pitch.attrib['play_guid'])
				else: 
					current_pitch.append(None)
				if 'start_speed' in pitch.attrib:
					current_pitch.append(pitch.attrib['start_speed'])
				else :
					current_pitch.append(None)
				if 'end_speed' in pitch.attrib:
					current_pitch.append(pitch.attrib['end_speed'])
				else:
					current_pitch.append(None)
				if 'sz_top' in pitch.attrib:
					current_pitch.append(pitch.attrib['sz_top'])
				else:
					current_pitch.append(None)
				if 'sz_bot' in pitch.attrib:
					current_pitch.append(pitch.attrib['sz_bot'])
				else: 
					current_pitch.append(None)
				if 'pfx_x' in pitch.attrib:
					current_pitch.append(pitch.attrib['pfx_x'])
				else:
					current_pitch.append(None)
				if 'pfx_z' in pitch.attrib:
					current_pitch.append(pitch.attrib['pfx_z'])
				else:
					current_pitch.append(None)
				if 'px' in pitch.attrib:
					current_pitch.append(pitch.attrib['px'])
				else:
					current_pitch.append(None)
				if 'pz' in pitch.attrib:
					current_pitch.append(pitch.attrib['pz'])
				else: 
					current_pitch.append(None)
				if 'x0' in pitch.attrib:
					current_pitch.append(pitch.attrib['x0'])
				else:
					current_pitch.append(None)
				if 'y0' in pitch.attrib:
					current_pitch.append(pitch.attrib['y0'])
				else: 
					current_pitch.append(None)
				if 'z0' in pitch.attrib:
					current_pitch.append(pitch.attrib['z0'])
				else: 
					current_pitch.append(None)
				if 'vx0' in pitch.attrib:
					current_pitch.append(pitch.attrib['vx0'])
				else:
					current_pitch.append(None)
				if 'vy0' in pitch.attrib:
					current_pitch.append(pitch.attrib['vy0'])
				else: 
					current_pitch.append(None)
				if 'vz0' in pitch.attrib:
					current_pitch.append(pitch.attrib['vz0'])
				else: 
					current_pitch.append(None)
				if 'ax' in pitch.attrib:
					current_pitch.append(pitch.attrib['ax'])
				else:
					current_pitch.append(None)
				if 'ay' in pitch.attrib:
					current_pitch.append(pitch.attrib['ay'])
				else: 
					current_pitch.append(None)
				if 'az' in pitch.attrib:
					current_pitch.append(pitch.attrib['az'])
				else: 
					current_pitch.append(None)
				if 'break_y' in pitch.attrib:
					current_pitch.append(pitch.attrib['break_y'])
				else:
					current_pitch.append(None)
				if 'break_angle' in pitch.attrib:
					current_pitch.append(pitch.attrib['break_angle'])
				else: 
					current_pitch.append(None)
				if 'break_length' in pitch.attrib:
					current_pitch.append(pitch.attrib['break_length'])
				else: 
					current_pitch.append(None)
				if 'pitch_type' in pitch.attrib:
					current_pitch.append(pitch.attrib['pitch_type'])
				else:
					current_pitch.append(None)
				if 'type_confidence' in pitch.attrib:
					current_pitch.append(pitch.attrib['type_confidence'])
				else: 
					current_pitch.append(None)
				if 'zone' in pitch.attrib:
					current_pitch.append(pitch.attrib['zone'])
				else: 
					current_pitch.append(None)
				if 'nasty' in pitch.attrib:
					current_pitch.append(pitch.attrib['nasty'])
				else: 
					current_pitch.append(None)
				if 'spin_dir' in pitch.attrib:
					current_pitch.append(pitch.attrib['spin_dir'])
				else:
					current_pitch.append(None)
				if 'spin_rate' in pitch.attrib:
					current_pitch.append(pitch.attrib['spin_rate'])
				else:
					current_pitch.append(None)
				pitches.append(current_pitch)

		insert = "INSERT INTO pitch ([atbat_id],[des],[des_es],[pid],[type],[tfs],[tfs_zulu],[x],[y],[event_num],[sv_id],[play_guid],[start_speed],[end_speed],[sz_top],[sz_bottom],[pfx_x],[pfx_z],[px],[pz],[x0],[y0],[z0],[vx0],[vy0],[vz0],[ax],[ay],[az],[break_y],[break_angle],[break_length],[pitch_type],[type_confidence],[zone],[nasty],[spin_dir],[spin_rate])"		
		insert += "SELECT a.id,p.[des],p.des_es,p.pid,p.[type],p.tfs,p.tfs_zulu,p.x,p.y,p.event_num,p.sv_id,p.play_guid,p.start_speed,p.end_speed,p.sz_top,p.sz_bot,p.pfx_x,p.pfx_z,p.px,p.pz,p.x0,p.y0,p.z0,p.vx0,p.vy0,p.vz0,p.ax,p.ay,p.az,p.break_y,p.break_angle,p.break_length,p.pitch_type,p.type_confidence,p.zone,p.nasty,p.spin_dir,p.spin_rate FROM pitches p INNER JOIN atbat a ON p.atbat_num = a.num WHERE a.game_id = ?;"
		while len(pitches) > 0:
			query_with = "WITH pitches AS ("
			if len(pitches) >= 55:
				query_pitches = pitches[:54]
				pitches = pitches[55:]
			else: 
				query_pitches = pitches
				pitches = []

			values = []
			for p in query_pitches:
				values += p
				query_with += "SELECT ? AS atbat_num, ? AS des, ? AS des_es, ? AS pid, ? AS type, ? AS tfs, ? AS tfs_zulu, ? AS x, ? AS y, ? AS event_num, ? AS sv_id, ? AS play_guid, ? AS start_speed, ? AS end_speed, ? AS sz_top, ? AS sz_bot, ? AS pfx_x, ? AS pfx_z, ? AS px, ? AS pz, ? AS x0, ? AS y0, ? AS z0, ? AS vx0, ? AS vy0, ? AS vz0, ? AS ax, ? AS ay, ? AS az, ? AS break_y, ? AS break_angle, ? AS break_length, ? AS pitch_type, ? AS type_confidence, ? AS zone, ? AS nasty, ? AS spin_dir, ? AS spin_rate UNION ALL "
			query_with = query_with[:-11] + ")"
			values.append(gid)

			cursor.execute(query_with + insert, values)