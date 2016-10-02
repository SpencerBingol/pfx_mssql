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
					self.teams_to_SQL(cursor, gid, game) # Insert new teams into DB
					self.game_to_SQL(cursor, gid, game) # Insert new game record into DB
					self.atbats_to_SQL(cursor, gid, game) # Insert new atbat records into DB
					self.pitches_to_SQL(cursor, gid, game) # Insert new pitch records into DB
					self.umpires_to_SQL(cursor, gid, players) # Insert new umpires into DB, update game record with umpires

					#connection.close()
					print("IMPORTED TO SQL: {}".format(gid))
		except Exception as e:
			report = "Failed to import game {}: {}".format(gid, e)
			with open("errors.log", "a") as logFile:
				logFile.write(report + '\n')
			print(report)
			pass
	
	def pitches_to_SQL(self, cursor, gid, games):
		# Import the individual pitches from the parsed XML
		pitch_values = []
		pitch_values.append([])

		pitch_withs = []
		pitch_withs.append("")

		pitch_withs[len(pitch_withs)-1] = """WITH pitches
AS (
"""
		current_parameter_count = 0
		for atbat in games.findall('.//atbat'):
			atbat_num = atbat.attrib['num']

			for pitch in atbat.findall('pitch'):
				if current_parameter_count + 39 >= 2100:
					pitch_withs[len(pitch_withs)-1] = pitch_withs[len(pitch_withs)-1][:-12] + """
)
"""
					pitch_withs.append("""WITH pitches
AS (
""")
					pitch_values[len(pitch_values)-1].append(gid)
					pitch_values.append([])
					current_parameter_count = 0
				
				pitch_withs[len(pitch_withs)-1] = pitch_withs[len(pitch_withs)-1] + """	SELECT ? AS atbat_num, ? AS des, ? AS des_es, ? AS pid, ? AS type, ? AS tfs, ? AS tfs_zulu, ? AS x, ? AS y, ? AS event_num, ? AS sv_id, ? AS play_guid, ? AS start_speed, ? AS end_speed, ? AS sz_top, ? AS sz_bot, ? AS pfx_x, ? AS pfx_z, ? AS px, ? AS pz, ? AS x0, ? AS y0, ? AS z0, ? AS vx0, ? AS vy0, ? AS vz0, ? AS ax, ? AS ay, ? AS az, ? AS break_y, ? AS break_angle, ? AS break_length, ? AS pitch_type, ? AS type_confidence, ? AS zone, ? AS nasty, ? AS spin_dir, ? AS spin_rate
	UNION ALL
"""
				pitch_values[len(pitch_values)-1].append(atbat_num)
				pitch_values[len(pitch_values)-1].append(pitch.attrib['des'])
				pitch_values[len(pitch_values)-1].append(pitch.attrib['des_es'])
				pitch_values[len(pitch_values)-1].append(pitch.attrib['id'])
				pitch_values[len(pitch_values)-1].append(pitch.attrib['type'])
				pitch_values[len(pitch_values)-1].append(pitch.attrib['tfs'])
				pitch_values[len(pitch_values)-1].append(pitch.attrib['tfs_zulu'])
				pitch_values[len(pitch_values)-1].append(pitch.attrib['x'])
				pitch_values[len(pitch_values)-1].append(pitch.attrib['y'])
				pitch_values[len(pitch_values)-1].append(pitch.attrib['event_num'])
				if 'sv_id' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['sv_id'])
				else:
					pitch_values[len(pitch_values)-1].append(None)
				if 'play_guid' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['play_guid'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'start_speed' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['start_speed'])
				else :
					pitch_values[len(pitch_values)-1].append(None)
				if 'end_speed' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['end_speed'])
				else:
					pitch_values[len(pitch_values)-1].append(None)
				pitch_values[len(pitch_values)-1].append(pitch.attrib['sz_top'])
				pitch_values[len(pitch_values)-1].append(pitch.attrib['sz_bot'])
				if 'pfx_x' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['pfx_x'])
				else:
					pitch_values[len(pitch_values)-1].append(None)
				if 'pfx_z' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['pfx_z'])
				else:
					pitch_values[len(pitch_values)-1].append(None)
				if 'px' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['px'])
				else:
					pitch_values[len(pitch_values)-1].append(None)
				if 'pz' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['pz'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'x0' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['x0'])
				else:
					pitch_values[len(pitch_values)-1].append(None)
				if 'y0' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['y0'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'z0' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['z0'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'vx0' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['vx0'])
				else:
					pitch_values[len(pitch_values)-1].append(None)
				if 'vy0' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['vy0'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'vz0' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['vz0'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'ax' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['ax'])
				else:
					pitch_values[len(pitch_values)-1].append(None)
				if 'ay' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['ay'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'az' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['az'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'break_y' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['break_y'])
				else:
					pitch_values[len(pitch_values)-1].append(None)
				if 'break_angle' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['break_angle'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'break_length' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['break_length'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'pitch_type' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['pitch_type'])
				else:
					pitch_values[len(pitch_values)-1].append(None)
				if 'type_confidence' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['type_confidence'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'zone' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['zone'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'nasty' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['nasty'])
				else: 
					pitch_values[len(pitch_values)-1].append(None)
				if 'spin_dir' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['spin_dir'])
				else:
					pitch_values[len(pitch_values)-1].append(None)
				if 'spin_rate' in pitch.attrib:
					pitch_values[len(pitch_values)-1].append(pitch.attrib['spin_rate'])
				else:
					pitch_values[len(pitch_values)-1].append(None)
				current_parameter_count = current_parameter_count + 38
		pitch_withs[len(pitch_withs)-1] = pitch_withs[len(pitch_withs)-1][:-12] + """
)
"""
		pitch_values[len(pitch_values)-1].append(gid)
		pitch_insert = """INSERT INTO pitch ([atbat_id],[des],[des_es],[pid],[type],[tfs],[tfs_zulu],[x],[y],[event_num],[sv_id],[play_guid],[start_speed],[end_speed],[sz_top],[sz_bottom],[pfx_x],[pfx_z],[px],[pz],[x0],[y0],[z0],[vx0],[vy0],[vz0],[ax],[ay],[az],[break_y],[break_angle],[break_length],[pitch_type],[type_confidence],[zone],[nasty],[spin_dir],[spin_rate])
"""		
		pitch_insert = pitch_insert + """SELECT a.id,p.[des],p.des_es,p.pid,p.[type],p.tfs,p.tfs_zulu,p.x,p.y,p.event_num,p.sv_id,p.play_guid,p.start_speed,p.end_speed,p.sz_top,p.sz_bot,p.pfx_x,p.pfx_z,p.px,p.pz,p.x0,p.y0,p.z0,p.vx0,p.vy0,p.vz0,p.ax,p.ay,p.az,p.break_y,p.break_angle,p.break_length,p.pitch_type,p.type_confidence,p.zone,p.nasty,p.spin_dir,p.spin_rate
FROM pitches p
INNER JOIN atbat a ON p.atbat_num = a.num
WHERE a.game_id = ?;"""

		for with_clause, values in zip(pitch_withs, pitch_values):
			if len(values) < 2100:
				cursor.execute(with_clause + pitch_insert, values)
			else:
				print("Somehow, there are too many parameters in this index.")

	def atbats_to_SQL(self, cursor, gid, games):
		# Import the atbats within a game from the parsed XML
		atbat_withs = []
		atbat_withs.append("""WITH atbats
AS (
""")
		atbat_values = []
		
		atbat_values.append([])
		current_parameter_count = 0
		for inning in games.findall('./inning'):
			inn_num = inning.attrib['num']
			for half in ['top', 'bottom']:
				for atbat in inning.findall('./' + half + '/atbat'):
					if current_parameter_count + 20 >= 2100:
						atbat_withs[len(atbat_withs)-1] = atbat_withs[len(atbat_withs)-1][:-12] + """
	)
	"""
						atbat_withs.append("""WITH atbats
	AS (
	""")
						atbat_values.append([])
						current_parameter_count = 0
					atbat_withs[len(atbat_withs)-1] = atbat_withs[len(atbat_withs)-1] + """	SELECT ? AS game_id, ? AS inning, ? AS top_bot, ? AS num, ? AS b, ? aS s, ? AS o, ? AS start_tfs_zulu, ? AS batter, ? AS stand, ? AS b_height, ? AS pitcher, ? AS p_throws, ? AS des, ? AS des_es, ? AS event_num, ? AS event, ? AS event_es, ? AS home_team_runs, ? AS away_team_runs
	UNION ALL
"""
					half_inn = 0
					if (half == "bottom"):
						half_inn = 1

					atbat_values[len(atbat_values)-1].append(gid)
					atbat_values[len(atbat_values)-1].append(inn_num)
					atbat_values[len(atbat_values)-1].append(half_inn)
					atbat_values[len(atbat_values)-1].append(atbat.attrib['num'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['b'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['s'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['o'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['start_tfs_zulu'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['batter'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['stand'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['b_height'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['pitcher'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['p_throws'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['des'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['des_es'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['event_num'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['event'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['event_es'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['home_team_runs'])
					atbat_values[len(atbat_values)-1].append(atbat.attrib['away_team_runs'])
					current_parameter_count = current_parameter_count + 20
		atbat_withs[len(atbat_withs)-1] = atbat_withs[len(atbat_withs)-1][:-12] + """
)
"""
		atbat_insert = """INSERT INTO atbat  (game_id, inning, top_bot, num, b, s, o, start_tfs_zulu, batter, stand, b_height, pitcher, p_throws, [des], des_es, event_num, [event], event_es, home_team_runs, away_team_runs)
"""		
		atbat_insert = atbat_insert + """SELECT a.game_id, a.inning, a.top_bot, a.num, a.b, a.s, a.o, a.start_tfs_zulu, a.batter, a.stand, a.b_height, a.pitcher, a.p_throws, a.[des], a.des_es, a.event_num, a.[event], a.event_es, a.home_team_runs, a.away_team_runs
FROM atbats a;"""

		for with_clause, values in zip(atbat_withs, atbat_values):
			cursor.execute(with_clause + atbat_insert, values)

	def game_to_SQL(self, cursor, gid, games):
		#Import the game record from the parsed XML
		game_values = []
		game_with = """WITH games
AS (
"""
		for game in games.findall('.'):
			game_with = game_with + """	SELECT ? AS gid, ? AS time_date, ? AS game_pk, ? AS venue, ? AS venue_id, ? AS away_team_id, ? AS home_team_id, ? AS home_time, ? AS home_time_zone, ? AS game_nbr, ? AS home_team_runs, ? AS away_team_runs, ? AS is_perfect_game, ? AS is_no_hitter, ? AS des
	UNION
"""
			game_values.append(gid)
			game_values.append(game.attrib['time_date'])
			game_values.append(game.attrib['game_pk'])
			game_values.append(game.attrib['venue'])
			game_values.append(game.attrib['venue_id'])
			game_values.append(game.attrib['away_team_id'])
			game_values.append(game.attrib['home_team_id'])
			game_values.append(game.attrib['home_time'])
			game_values.append(game.attrib['home_time_zone'])
			game_values.append(game.attrib['game_nbr'])
			game_values.append(game.attrib['home_team_runs'])
			game_values.append(game.attrib['away_team_runs'])
			game_values.append(game.attrib['is_perfect_game'])
			game_values.append(game.attrib['is_no_hitter'])
			if 'description' not in game.attrib: 
				game_values.append(None)
			else:
				game_values.append(game.attrib['description'])
		game_with = game_with[:-8] + """
)
"""
		game_insert = game_with + """INSERT INTO game (gid, game_date, game_pk, venue, venue_id, away_team_id, home_team_id, home_time, home_time_zone, game_nbr, home_team_runs, away_team_runs, is_perfect_game, is_no_hitter, [description] )
"""
		game_insert = game_insert + """SELECT g.gid, g.time_date, g.game_pk, g.venue, g.venue_id, g.away_team_id, g.home_team_id, g.home_time, g.home_time_zone, g.game_nbr, g.home_team_runs,  g.away_team_runs, g.is_perfect_game, g.is_no_hitter, g.des
FROM games g;"""
		cursor.execute(game_insert, game_values)

	def teams_to_SQL (self, cursor, gid, games):
		# Import either team if it doesn't already exist
		team_values = []
		teams_with = """WITH teams
AS (
"""
		for game in games.findall('.'):
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

		cursor.execute(umpire_update, umpires_values)

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