USE [master]
GO
/****** Object:  Database [PitchFX]    Script Date: 10/1/2016 8:48:33 PM ******/
CREATE DATABASE [PitchFX]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'PitchFX', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.SQLEXPRESS\MSSQL\DATA\PitchFX.mdf' , SIZE = 466944KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'PitchFX_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.SQLEXPRESS\MSSQL\DATA\PitchFX_log.ldf' , SIZE = 401408KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
GO
ALTER DATABASE [PitchFX] SET COMPATIBILITY_LEVEL = 120
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [PitchFX].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [PitchFX] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [PitchFX] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [PitchFX] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [PitchFX] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [PitchFX] SET ARITHABORT OFF 
GO
ALTER DATABASE [PitchFX] SET AUTO_CLOSE ON 
GO
ALTER DATABASE [PitchFX] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [PitchFX] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [PitchFX] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [PitchFX] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [PitchFX] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [PitchFX] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [PitchFX] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [PitchFX] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [PitchFX] SET  ENABLE_BROKER 
GO
ALTER DATABASE [PitchFX] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [PitchFX] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [PitchFX] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [PitchFX] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [PitchFX] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [PitchFX] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [PitchFX] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [PitchFX] SET RECOVERY SIMPLE 
GO
ALTER DATABASE [PitchFX] SET  MULTI_USER 
GO
ALTER DATABASE [PitchFX] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [PitchFX] SET DB_CHAINING OFF 
GO
ALTER DATABASE [PitchFX] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [PitchFX] SET TARGET_RECOVERY_TIME = 0 SECONDS 
GO
ALTER DATABASE [PitchFX] SET DELAYED_DURABILITY = DISABLED 
GO
ALTER DATABASE [PitchFX] SET QUERY_STORE = OFF
GO
USE [PitchFX]
GO
ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET MAXDOP = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET LEGACY_CARDINALITY_ESTIMATION = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET PARAMETER_SNIFFING = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET QUERY_OPTIMIZER_HOTFIXES = PRIMARY;
GO
USE [PitchFX]
GO
/****** Object:  Table [dbo].[atbat]    Script Date: 10/1/2016 8:48:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[atbat](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[game_id] [varchar](255) NULL,
	[inning] [int] NULL,
	[top_bot] [bit] NULL,
	[num] [int] NULL,
	[b] [int] NULL,
	[s] [int] NULL,
	[o] [int] NULL,
	[start_tfs_zulu] [datetime] NULL,
	[batter] [int] NULL,
	[stand] [varchar](1) NULL,
	[b_height] [varchar](5) NULL,
	[pitcher] [int] NULL,
	[p_throws] [varchar](1) NULL,
	[des] [nvarchar](1024) NULL,
	[des_es] [nvarchar](1024) NULL,
	[event_num] [int] NULL,
	[event] [varchar](255) NULL,
	[event_es] [varchar](255) NULL,
	[home_team_runs] [int] NULL,
	[away_team_runs] [int] NULL,
	[hit_x] [float] NULL,
	[hit_y] [float] NULL,
	[hit_type] [varchar](255) NULL,
	[create_date] [datetime] NULL,
	[modify_date] [datetime] NULL,
 CONSTRAINT [PK__atbat__3213E83F1443CD69] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
/****** Object:  Table [dbo].[game]    Script Date: 10/1/2016 8:48:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[game](
	[gid] [varchar](255) NOT NULL,
	[game_date] [datetime] NULL,
	[game_pk] [varchar](10) NULL,
	[venue] [varchar](255) NULL,
	[venue_id] [int] NULL,
	[description] [varchar](255) NULL,
	[away_team_id] [int] NULL,
	[home_team_id] [int] NULL,
	[home_time] [time](7) NULL,
	[home_time_zone] [varchar](5) NULL,
	[temp] [int] NULL,
	[condition] [varchar](255) NULL,
	[wind] [varchar](255) NULL,
	[game_nbr] [int] NULL,
	[home_team_runs] [int] NULL,
	[away_team_runs] [int] NULL,
	[umpire_home] [int] NULL,
	[umpire_first] [int] NULL,
	[umpire_second] [int] NULL,
	[umpire_third] [int] NULL,
	[is_perfect_game] [varchar](1) NULL,
	[is_no_hitter] [varchar](1) NULL,
	[create_date] [datetime] NULL,
	[modify_date] [datetime] NULL,
 CONSTRAINT [PK__game__DCD80EF86164032A] PRIMARY KEY CLUSTERED 
(
	[gid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
/****** Object:  Table [dbo].[pitch]    Script Date: 10/1/2016 8:48:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[pitch](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[atbat_id] [bigint] NULL,
	[des] [varchar](255) NULL,
	[des_es] [varchar](255) NULL,
	[pid] [int] NULL,
	[type] [varchar](1) NULL,
	[tfs] [varchar](10) NULL,
	[tfs_zulu] [datetime] NULL,
	[x] [decimal](18, 3) NULL,
	[y] [decimal](18, 3) NULL,
	[event_num] [int] NULL,
	[sv_id] [varchar](50) NULL,
	[play_guid] [varchar](50) NULL,
	[start_speed] [decimal](18, 2) NULL,
	[end_speed] [decimal](18, 2) NULL,
	[sz_top] [decimal](18, 2) NULL,
	[sz_bottom] [decimal](18, 2) NULL,
	[pfx_x] [decimal](18, 2) NULL,
	[pfx_z] [decimal](18, 2) NULL,
	[px] [decimal](18, 2) NULL,
	[pz] [decimal](18, 2) NULL,
	[x0] [decimal](18, 2) NULL,
	[y0] [decimal](18, 2) NULL,
	[z0] [decimal](18, 2) NULL,
	[vx0] [decimal](18, 2) NULL,
	[vy0] [decimal](18, 2) NULL,
	[vz0] [decimal](18, 2) NULL,
	[ax] [decimal](18, 2) NULL,
	[ay] [decimal](18, 2) NULL,
	[az] [decimal](18, 2) NULL,
	[break_y] [decimal](18, 2) NULL,
	[break_angle] [decimal](18, 2) NULL,
	[break_length] [decimal](18, 2) NULL,
	[pitch_type] [varchar](2) NULL,
	[type_confidence] [decimal](18, 3) NULL,
	[zone] [int] NULL,
	[nasty] [int] NULL,
	[spin_dir] [decimal](18, 3) NULL,
	[spin_rate] [decimal](18, 3) NULL,
	[create_date] [datetime] NULL,
	[modify_date] [datetime] NULL,
 CONSTRAINT [PK__pitch__3213E83F0D71E7B6] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
/****** Object:  Table [dbo].[player]    Script Date: 10/1/2016 8:48:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[player](
	[id] [int] NOT NULL,
	[first_name] [varchar](255) NULL,
	[last_name] [varchar](255) NULL,
	[dob] [datetime] NULL,
	[height] [varchar](5) NULL,
	[weight] [int] NULL,
	[bats] [varchar](1) NULL,
	[throws] [varchar](1) NULL,
	[create_date] [datetime] NULL,
	[modify_date] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
/****** Object:  Table [dbo].[team]    Script Date: 10/1/2016 8:48:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[team](
	[id] [int] NOT NULL,
	[abbreviation] [varchar](3) NULL,
	[file_code] [varchar](3) NULL,
	[city] [varchar](255) NULL,
	[name] [varchar](255) NULL,
	[stadium] [varchar](255) NULL,
	[create_date] [datetime] NULL,
	[modify_date] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
/****** Object:  Table [dbo].[umpire]    Script Date: 10/1/2016 8:48:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[umpire](
	[id] [int] NOT NULL,
	[first_name] [varchar](255) NULL,
	[last_name] [varchar](255) NULL,
	[create_date] [datetime] NULL,
	[modify_date] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
/****** Object:  Index [IX_atbat_batter]    Script Date: 10/1/2016 8:48:33 PM ******/
CREATE NONCLUSTERED INDEX [IX_atbat_batter] ON [dbo].[atbat]
(
	[batter] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [IX_atbat_pitcher]    Script Date: 10/1/2016 8:48:33 PM ******/
CREATE NONCLUSTERED INDEX [IX_atbat_pitcher] ON [dbo].[atbat]
(
	[pitcher] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [IX_pitch_atbat_id]    Script Date: 10/1/2016 8:48:33 PM ******/
CREATE NONCLUSTERED INDEX [IX_pitch_atbat_id] ON [dbo].[pitch]
(
	[atbat_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [dbo].[atbat] ADD  CONSTRAINT [DF__atbat__create_da__2F10007B]  DEFAULT (getdate()) FOR [create_date]
GO
ALTER TABLE [dbo].[game] ADD  CONSTRAINT [DF__game__create_dat__300424B4]  DEFAULT (getdate()) FOR [create_date]
GO
ALTER TABLE [dbo].[pitch] ADD  CONSTRAINT [DF__pitch__create_da__182C9B23]  DEFAULT (getdate()) FOR [create_date]
GO
ALTER TABLE [dbo].[player] ADD  DEFAULT (getdate()) FOR [create_date]
GO
ALTER TABLE [dbo].[team] ADD  DEFAULT (getdate()) FOR [create_date]
GO
ALTER TABLE [dbo].[umpire] ADD  DEFAULT (getdate()) FOR [create_date]
GO
ALTER TABLE [dbo].[atbat]  WITH CHECK ADD  CONSTRAINT [FK_atbat_batter] FOREIGN KEY([batter])
REFERENCES [dbo].[player] ([id])
GO
ALTER TABLE [dbo].[atbat] CHECK CONSTRAINT [FK_atbat_batter]
GO
ALTER TABLE [dbo].[atbat]  WITH CHECK ADD  CONSTRAINT [FK_atbat_game] FOREIGN KEY([game_id])
REFERENCES [dbo].[game] ([gid])
GO
ALTER TABLE [dbo].[atbat] CHECK CONSTRAINT [FK_atbat_game]
GO
ALTER TABLE [dbo].[atbat]  WITH CHECK ADD  CONSTRAINT [FK_atbat_pitcher] FOREIGN KEY([pitcher])
REFERENCES [dbo].[player] ([id])
GO
ALTER TABLE [dbo].[atbat] CHECK CONSTRAINT [FK_atbat_pitcher]
GO
ALTER TABLE [dbo].[game]  WITH CHECK ADD  CONSTRAINT [FK_game_away] FOREIGN KEY([away_team_id])
REFERENCES [dbo].[team] ([id])
GO
ALTER TABLE [dbo].[game] CHECK CONSTRAINT [FK_game_away]
GO
ALTER TABLE [dbo].[game]  WITH CHECK ADD  CONSTRAINT [FK_game_home] FOREIGN KEY([home_team_id])
REFERENCES [dbo].[team] ([id])
GO
ALTER TABLE [dbo].[game] CHECK CONSTRAINT [FK_game_home]
GO
ALTER TABLE [dbo].[game]  WITH CHECK ADD  CONSTRAINT [FK_game_umpire_first] FOREIGN KEY([umpire_first])
REFERENCES [dbo].[umpire] ([id])
GO
ALTER TABLE [dbo].[game] CHECK CONSTRAINT [FK_game_umpire_first]
GO
ALTER TABLE [dbo].[game]  WITH CHECK ADD  CONSTRAINT [FK_game_umpire_home] FOREIGN KEY([umpire_home])
REFERENCES [dbo].[umpire] ([id])
GO
ALTER TABLE [dbo].[game] CHECK CONSTRAINT [FK_game_umpire_home]
GO
ALTER TABLE [dbo].[game]  WITH CHECK ADD  CONSTRAINT [FK_game_umpire_second] FOREIGN KEY([umpire_second])
REFERENCES [dbo].[umpire] ([id])
GO
ALTER TABLE [dbo].[game] CHECK CONSTRAINT [FK_game_umpire_second]
GO
ALTER TABLE [dbo].[game]  WITH CHECK ADD  CONSTRAINT [FK_game_umpire_third] FOREIGN KEY([umpire_third])
REFERENCES [dbo].[umpire] ([id])
GO
ALTER TABLE [dbo].[game] CHECK CONSTRAINT [FK_game_umpire_third]
GO
ALTER TABLE [dbo].[pitch]  WITH CHECK ADD  CONSTRAINT [FK_pitch_pitch] FOREIGN KEY([atbat_id])
REFERENCES [dbo].[atbat] ([id])
GO
ALTER TABLE [dbo].[pitch] CHECK CONSTRAINT [FK_pitch_pitch]
GO
/****** Object:  StoredProcedure [dbo].[InsertGame_FromXML]    Script Date: 10/1/2016 8:48:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[InsertGame_FromXML](
	@gid VARCHAR(50),
	@file_dir VARCHAR(1000)
)
AS
-- Requires file structure:
--	-> @file_dir/
--		-> linescore.xml
--		-> players.xml
--		-> plays.xml
--		-> inning/
--			-> inning_all.xml
--			-> inning_hit.xml

SET XACT_ABORT ON;
BEGIN TRANSACTION
	IF NOT EXISTS (SELECT * FROM game WHERE gid = @gid)
	BEGIN
		DECLARE @xml XML, @QryStr NVARCHAR(MAX);

		SET @QryStr = '
		SELECT @XML_out = BulkColumn
		FROM OPENROWSET(
			BULK '''+@file_dir+'linescore.xml'+'''
			, SINGLE_CLOB
		) AS x;';
		EXECUTE sp_executesql @QryStr, N'@XML_out XML OUTPUT', @XML_out=@xml OUTPUT;

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
				,xt.xc.value('@away_team_City', 'VARCHAR(255)') AS city
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

		-- Update game record w/ weather from plays.xml
		SET @QryStr = '
		SELECT @XML_out = BulkColumn
		FROM OPENROWSET(
			BULK '''+@file_dir+'plays.xml'+'''
			, SINGLE_CLOB
		) AS x;';
		EXECUTE sp_executesql @QryStr, N'@XML_out XML OUTPUT', @XML_out=@xml OUTPUT;

		DECLARE @temp INT, @condition VARCHAR(255), @wind VARCHAR(255);
		SELECT 
			@temp = xt.xc.value('@temp', 'INT')
			,@condition = xt.xc.value('@condition', 'VARCHAR(255)')
			,@wind = xt.xc.value('@wind', 'VARCHAR(255)')
		FROM @xml.nodes('/game/weather') AS xt(xc);

		UPDATE game
		SET temp = @temp, condition = @condition, wind = @wind
		WHERE gid = @gid;

		SET @QryStr = '
		SELECT @XML_out = BulkColumn
		FROM OPENROWSET(
			BULK '''+@file_dir+'players.xml'+'''
			, SINGLE_CLOB
		) AS x;';
		EXECUTE sp_executesql @QryStr, N'@XML_out XML OUTPUT', @XML_out=@xml OUTPUT;

		-- Insert players that don't exist
		INSERT INTO player (id, first_name, last_name, bats, throws)
		SELECT x.id, x.first_name, x.last_name, x.bats, x.throws
		FROM (
			SELECT xt.xc.value('@id', 'INT') AS id
				,xt.xc.value('@first', 'VARCHAR(255)') AS first_name
				,xt.xc.value('@last', 'VARCHAR(255)') AS last_name
				,xt.xc.value('@bats', 'VARCHAR(255)') AS bats
				,xt.xc.value('@rl', 'VARCHAR(255)') AS throws
			FROM @xml.nodes('/game/team/player') AS xt(xc)
		) AS x
		LEFT OUTER JOIN player p ON x.id = p.id
		WHERE p.id IS NULL;

		-- Insert umpires that don't exist, update game with umpires
		INSERT INTO umpire (id, first_name, last_name)
		SELECT x.id, x.first_name, x.last_name
		FROM (
			SELECT xt.xc.value('@id', 'INT') AS id
				,xt.xc.value('@first', 'VARCHAR(255)') AS first_name
				,xt.xc.value('@last', 'VARCHAR(255)') AS last_name
			FROM @xml.nodes('/game/umpires/umpire') AS xt(xc)
		) x
		LEFT OUTER JOIN umpire u ON x.id = u.id
		WHERE u.id IS NULL;

		DECLARE @home_ump INT, @first_ump INT, @second_ump INT, @third_ump INT;

		SELECT @home_ump = xt.xc.value('@id', 'INT')
		FROM @xml.nodes('/game/umpires/umpire') AS xt(xc)
		WHERE xt.xc.value('@position', 'VARCHAR(255)') = 'home';

		SELECT @first_ump = xt.xc.value('@id', 'INT')
		FROM @xml.nodes('/game/umpires/umpire') AS xt(xc)
		WHERE xt.xc.value('@position', 'VARCHAR(255)') = 'first';

		SELECT @second_ump = xt.xc.value('@id', 'INT')
		FROM @xml.nodes('/game/umpires/umpire') AS xt(xc)
		WHERE xt.xc.value('@position', 'VARCHAR(255)') = 'second';

		SELECT @third_ump = xt.xc.value('@id', 'INT')
		FROM @xml.nodes('/game/umpires/umpire') AS xt(xc)
		WHERE xt.xc.value('@position', 'VARCHAR(255)') = 'third';

		UPDATE game
		SET umpire_home = @home_ump, umpire_first = @first_ump, umpire_second = @second_ump, umpire_third = @third_ump
		WHERE gid = @gid;

		-- Insert atbat records
		SET @QryStr = '
		SELECT @XML_out = BulkColumn
		FROM OPENROWSET(
			BULK '''+@file_dir+'inning\inning_all.xml'+'''
			, SINGLE_CLOB
		) AS x;';
		EXECUTE sp_executesql @QryStr, N'@XML_out XML OUTPUT', @XML_out=@xml OUTPUT;

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
				,xt.xc.value('@sz_bottom', 'FLOAT') AS sz_bottom
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

		-- Add hit information
		SET @QryStr = '
		SELECT @XML_out = BulkColumn
		FROM OPENROWSET(
			BULK '''+@file_dir+'inning\inning_hit.xml'+'''
			, SINGLE_CLOB
		) AS x;';
		EXECUTE sp_executesql @QryStr, N'@XML_out XML OUTPUT', @XML_out=@xml OUTPUT;

		--INSERT INTO hip ([des], x, y, batter, pitcher, [type], team, inning)
		--SELECT xt.xc.value('@des','VARCHAR(50)') AS [des]
		--	,xt.xc.value('@x', 'FLOAT') AS x
		--	,xt.xc.value('@y', 'FLOAT') AS y
		--	,xt.xc.value('@batter', 'INT') AS batter
		--	,xt.xc.value('@pitcher', 'INT') AS pitcher
		--	,xt.xc.value('@type', 'VARCHAR(1)') AS [type]
		--	,xt.xc.value('@team', 'VARCHAR(1)') AS team
		--	,xt.xc.value('@inning', 'INT') AS inning
		--FROM @xml.nodes('/hitchart/hip') AS xt(xc);
		END;

COMMIT TRANSACTION;
GO
/****** Object:  StoredProcedure [dbo].[sp_ImportGame]    Script Date: 10/1/2016 8:48:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_ImportGame]
(
	@gid VARCHAR(255),
	@xml XML
)
AS
SET XACT_ABORT ON;
BEGIN TRANSACTION
	IF NOT EXISTS (SELECT * FROM game WHERE gid = @gid)
	BEGIN
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
/****** Object:  StoredProcedure [dbo].[sp_ImportPlayers]    Script Date: 10/1/2016 8:48:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[sp_ImportPlayers]
(
	@xml XML
)
AS
SET XACT_ABORT ON;
BEGIN TRANSACTION
	-- Insert players that don't exist
	INSERT INTO player (id, first_name, last_name, bats, throws)
	SELECT x.id, x.first_name, x.last_name, x.bats, x.throws
	FROM (
		SELECT xt.xc.value('@id', 'INT') AS id
			,xt.xc.value('@first', 'VARCHAR(255)') AS first_name
			,xt.xc.value('@last', 'VARCHAR(255)') AS last_name
			,xt.xc.value('@bats', 'VARCHAR(255)') AS bats
			,xt.xc.value('@rl', 'VARCHAR(255)') AS throws
		FROM @xml.nodes('/game/team/player') AS xt(xc)
	) AS x
	LEFT OUTER JOIN player p ON x.id = p.id
	WHERE p.id IS NULL;
COMMIT TRANSACTION;

GO
/****** Object:  StoredProcedure [dbo].[sp_ImportUmpires]    Script Date: 10/1/2016 8:48:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[sp_ImportUmpires]
(
	@gid VARCHAR(50),
	@xml XML
)
AS
SET XACT_ABORT ON;
BEGIN TRANSACTION
	-- Insert umpires that don't exist, update game with umpires
	INSERT INTO umpire (id, first_name, last_name)
	SELECT x.id, x.first_name, x.last_name
	FROM (
		SELECT xt.xc.value('@id', 'INT') AS id
			,xt.xc.value('@first', 'VARCHAR(255)') AS first_name
			,xt.xc.value('@last', 'VARCHAR(255)') AS last_name
		FROM @xml.nodes('/game/umpires/umpire') AS xt(xc)
	) x
	LEFT OUTER JOIN umpire u ON x.id = u.id
	WHERE u.id IS NULL;

	DECLARE @home_ump INT, @first_ump INT, @second_ump INT, @third_ump INT;

	SELECT @home_ump = xt.xc.value('@id', 'INT')
	FROM @xml.nodes('/game/umpires/umpire') AS xt(xc)
	WHERE xt.xc.value('@position', 'VARCHAR(255)') = 'home';

	SELECT @first_ump = xt.xc.value('@id', 'INT')
	FROM @xml.nodes('/game/umpires/umpire') AS xt(xc)
	WHERE xt.xc.value('@position', 'VARCHAR(255)') = 'first';

	SELECT @second_ump = xt.xc.value('@id', 'INT')
	FROM @xml.nodes('/game/umpires/umpire') AS xt(xc)
	WHERE xt.xc.value('@position', 'VARCHAR(255)') = 'second';

	SELECT @third_ump = xt.xc.value('@id', 'INT')
	FROM @xml.nodes('/game/umpires/umpire') AS xt(xc)
	WHERE xt.xc.value('@position', 'VARCHAR(255)') = 'third';

	UPDATE game
	SET umpire_home = @home_ump, umpire_first = @first_ump, umpire_second = @second_ump, umpire_third = @third_ump
	WHERE gid = @gid;
COMMIT TRANSACTION;

GO
USE [master]
GO
ALTER DATABASE [PitchFX] SET  READ_WRITE 
GO
