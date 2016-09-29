USE [PitchFX]
GO

/****** Object:  StoredProcedure [dbo].[sp_ImportGame]    Script Date: 9/29/2016 6:31:23 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_ImportGame]
(
	@gid VARCHAR(255),
	@fileLoc VARCHAR(255)
)
AS
SET XACT_ABORT ON;
BEGIN TRANSACTION
	IF NOT EXISTS (SELECT * FROM game WHERE gid = @gid)
	BEGIN
		DECLARE @xml XML, @QryStr NVARCHAR(MAX);

		SET @QryStr = '
		SELECT @XML_out = BulkColumn
		FROM OPENROWSET(
			BULK '''+@fileLoc+'''
			, SINGLE_CLOB
		) AS x;';
		EXECUTE sp_executesql @QryStr, N'@XML_out XML OUTPUT', @XML_out=@xml OUTPUT;

		-- Insert team entries where it doesn't exist
		INSERT INTO team (id, abbreviation, file_code, city, name)
		SELECT x.id, x.abbreviation, x.file_code, x.city, x.name
		FROM (
			SELECT 
				xt.xc.value('@home_team_id', 'INT') AS id
				,xt.xc.value('@home_name_abbrev', 'VARCHAR(3)') AS abbreviation
				,xt.xc.value('@home_file_code', 'VARCHAR(3)') AS file_code
				,xt.xc.value('@home_team_city', 'VARCHAR(255)') AS city
				,xt.xc.value('@home_team_name', 'VARCHAR(255)') AS name
			FROM @xml.nodes('/game') AS xt(xc)
			UNION
			SELECT 
				xt.xc.value('@away_team_id', 'INT') AS id
				,xt.xc.value('@away_name_abbrev', 'VARCHAR(3)') AS abbreviation
				,xt.xc.value('@away_file_code', 'VARCHAR(3)') AS file_code
				,xt.xc.value('@away_team_city', 'VARCHAR(255)') AS city
				,xt.xc.value('@away_team_name', 'VARCHAR(255)') AS name
			FROM @xml.nodes('/game') AS xt(xc)
		) x
		LEFT OUTER JOIN team t ON t.id = x.id
		WHERE t.id IS NULL;

		INSERT INTO game (gid, game_date, game_pk, venue, venue_id, away_team_id, home_team_id, home_time_zone, game_nbr, home_team_runs, away_team_runs, is_perfect_game, is_no_hitter)
		SELECT 
			@gid
			,xt.xc.value('@time_date', 'DATETIME')
			,xt.xc.value('@game_pk', 'VARCHAR(10)')
			,xt.xc.value('@venue', 'VARCHAR(255)')
			,xt.xc.value('@venue_id', 'INT')
			,xt.xc.value('@away_team_id', 'INT')
			,xt.xc.value('@home_team_id', 'INT')
			,xt.xc.value('@home_time_zone', 'VARCHAR(5)')
			,xt.xc.value('@game_nbr', 'INT')
			,xt.xc.value('@home_team_runs', 'INT')
			,xt.xc.value('@away_team_runs', 'INT')
			,xt.xc.value('@is_perfect_game', 'VARCHAR(1)')
			,xt.xc.value('@is_no_hitter', 'VARCHAR(1)')
		FROM @xml.nodes('/game') AS xt(xc);

		INSERT INTO atbat (game_id, inning, num, b, s, o, start_tfs_zulu, batter, stand, b_height, pitcher, p_throws, [des], des_es, event_num, [event], event_es, home_team_runs, away_team_runs)
		SELECT @gid AS game_id
			,xt.xc.value('(../../@num)[1]', 'INT') AS inning
			,xt.xc.value('@num','INT') AS num
			,xt.xc.value('@b', 'INT') AS b
			,xt.xc.value('@s', 'INT') AS s
			,xt.xc.value('@o', 'INT') AS o
			,xt.xc.value('@start_tfs_zulu', 'DATETIME') AS start_tfs_zulu
			,xt.xc.value('@batter', 'INT') AS batter
			,xt.xc.value('@stand', 'VARCHAR(1)') AS stand
			,xt.xc.value('@b_height','VARCHAR(5)') AS b_stand
			,xt.xc.value('@pitcher', 'INT') AS pitcher
			,xt.xc.value('@p_throws', 'VARCHAR(1)') AS p_throws
			,xt.xc.value('@des', 'VARCHAR(255)') AS [des]
			,xt.xc.value('@des_es', 'VARCHAR(255)') AS des_es
			,xt.xc.value('@event_num', 'INT') AS event_num
			,xt.xc.value('@event', 'VARCHAR(255)') AS [event]
			,xt.xc.value('@event_es', 'VARCHAR(255)') AS event_es
			,xt.xc.value('@home_team_runs','INT') AS home_team_runs
			,xt.xc.value('@away_team_runs', 'INT') AS away_team_runs
		FROM @xml.nodes('/game/inning//atbat') AS xt(xc);

			INSERT INTO [dbo].[pitch]
					   ([atbat_id]
					   ,[des]
					   ,[des_es]
					   ,[pid]
					   ,[type]
					   ,[tfs]
					   ,[tfs_zulu]
					   ,[x]
					   ,[y]
					   ,[event_num]
					   ,[sv_id]
					   ,[play_guid]
					   ,[start_speed]
					   ,[end_speed]
					   ,[sz_top]
					   ,[sz_bottom]
					   ,[pfx_x]
					   ,[pfx_z]
					   ,[px]
					   ,[pz]
					   ,[x0]
					   ,[y0]
					   ,[z0]
					   ,[vx0]
					   ,[vy0]
					   ,[vz0]
					   ,[ax]
					   ,[ay]
					   ,[az]
					   ,[break_y]
					   ,[break_angle]
					   ,[break_length]
					   ,[pitch_type]
					   ,[type_confidence]
					   ,[zone]
					   ,[nasty]
					   ,[spin_dir]
					   ,[spin_rate]
					   )
			SELECT a.id AS atbat_id
				,p.[des]
				,p.des_es
				,p.pid
				,p.[type]
				,p.tfs
				,p.tfs_zulu
				,p.x
				,p.y
				,p.event_num
				,p.sv_id
				,p.play_guid
				,p.start_speed
				,p.end_speed
				,p.sz_top
				,p.sz_bottom
				,p.pfx_x
				,p.pfx_z
				,p.px
				,p.pz
				,p.x0
				,p.y0
				,p.z0
				,p.vx0
				,p.vy0
				,p.vz0
				,p.ax
				,p.ay
				,p.az
				,p.break_y
				,p.break_angle
				,p.break_length
				,p.pitch_type
				,p.type_confidence
				,p.zone
				,p.nasty
				,p.spin_dir
				,p.spin_rate
			FROM (
				SELECT xt.xc.value('../@num', 'INT') AS atbat_num
					,xt.xc.value('@des', 'VARCHAR(255)') AS [des]
					,xt.xc.value('@des_es', 'VARCHAR(255)') AS des_es
					,xt.xc.value('@id', 'INT') AS pid
					,xt.xc.value('@type', 'VARCHAR(1)') AS [type]
					,xt.xc.value('@tfs', 'VARCHAR(10)') AS tfs
					,xt.xc.value('@tfs_zulu', 'DATETIME') AS tfs_zulu
					,xt.xc.value('@x', 'FLOAT') AS x
					,xt.xc.value('@y', 'FLOAT') AS y
					,xt.xc.value('@event_num', 'INT') AS event_num
					,xt.xc.value('@sv_id', 'VARCHAR(50)') AS sv_id
					,xt.xc.value('@play_guid', 'VARCHAR(50)') AS play_guid
					,xt.xc.value('@start_speed', 'FLOAT') AS start_speed
					,xt.xc.value('@end_speed', 'FLOAT') AS end_speed
					,xt.xc.value('@sz_top', 'FLOAT') AS sz_top
					,xt.xc.value('@sz_bot', 'FLOAT') AS sz_bottom
					,xt.xc.value('@pfx_x', 'FLOAT') AS pfx_x
					,xt.xc.value('@pfx_z', 'FLOAT') AS pfx_z
					,xt.xc.value('@px', 'FLOAT') AS px
					,xt.xc.value('@pz', 'FLOAT') AS pz
					,xt.xc.value('@x0', 'FLOAT') AS x0
					,xt.xc.value('@y0', 'FLOAT') AS y0
					,xt.xc.value('@z0', 'FLOAT') AS z0
					,xt.xc.value('@vx0', 'FLOAT') AS vx0
					,xt.xc.value('@vy0', 'FLOAT') AS vy0
					,xt.xc.value('@vz0', 'FLOAT') AS vz0
					,xt.xc.value('@ax', 'FLOAT') AS ax
					,xt.xc.value('@ay', 'FLOAT') AS ay
					,xt.xc.value('@az', 'FLOAT') AS az
					,xt.xc.value('@break_y', 'FLOAT') AS break_y
					,xt.xc.value('@break_angle', 'FLOAT') AS break_angle
					,xt.xc.value('@break_length', 'FLOAT') AS break_length
					,xt.xc.value('@pitch_type', 'VARCHAR(2)') AS pitch_type
					,xt.xc.value('@type_confidence', 'FLOAT') AS type_confidence
					,xt.xc.value('@zone', 'INT') AS zone
					,xt.xc.value('@nasty', 'INT') AS nasty
					,xt.xc.value('@spin_dir', 'FLOAT') AS spin_dir
					,xt.xc.value('@spin_rate', 'FLOAT') AS spin_rate
				FROM @xml.nodes('/game/inning//atbat/pitch') AS xt(xc)
			) p
			INNER JOIN atbat a ON p.atbat_num = a.num
			WHERE a.game_id = @gid;
		END;
COMMIT TRANSACTION;
GO

