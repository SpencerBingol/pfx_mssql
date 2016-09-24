USE [master]
GO
/****** Object:  Database [PitchFX]    Script Date: 1/22/2016 11:33:28 PM ******/
CREATE DATABASE [PitchFX]
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
USE [PitchFX]
GO
/****** Object:  Table [dbo].[atbat]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[atbat](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[game_id] [varchar](255) NULL,
	[inning] [int] NULL,
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
	[des] [varchar](255) NULL,
	[des_es] [varchar](255) NULL,
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
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[game]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
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
	[home_time] [datetime] NULL,
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
PRIMARY KEY CLUSTERED 
(
	[gid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[pitch]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
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
	[x] [float] NULL,
	[y] [float] NULL,
	[event_num] [int] NULL,
	[sv_id] [varchar](50) NULL,
	[play_guid] [varchar](50) NULL,
	[start_speed] [float] NULL,
	[end_speed] [float] NULL,
	[sz_top] [float] NULL,
	[sz_bottom] [float] NULL,
	[pfx_x] [float] NULL,
	[pfx_z] [float] NULL,
	[px] [float] NULL,
	[pz] [float] NULL,
	[x0] [float] NULL,
	[y0] [float] NULL,
	[z0] [float] NULL,
	[vx0] [float] NULL,
	[vy0] [float] NULL,
	[vz0] [float] NULL,
	[ax] [float] NULL,
	[ay] [float] NULL,
	[az] [float] NULL,
	[break_y] [float] NULL,
	[break_angle] [float] NULL,
	[break_length] [float] NULL,
	[pitch_type] [varchar](2) NULL,
	[type_confidence] [float] NULL,
	[zone] [int] NULL,
	[nasty] [int] NULL,
	[spin_dir] [float] NULL,
	[spin_rate] [float] NULL,
	[create_date] [datetime] NULL,
	[modify_date] [datetime] NULL,
 CONSTRAINT [PK__pitch__3213E83F0D71E7B6] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[player]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
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
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[player_weight]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[player_weight](
	[season] [int] NOT NULL,
	[player] [int] NOT NULL,
	[weight] [int] NULL,
	[create_date] [datetime] NULL,
	[modify_date] [datetime] NULL,
 CONSTRAINT [pk_player_weight] PRIMARY KEY CLUSTERED 
(
	[season] ASC,
	[player] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
/****** Object:  Table [dbo].[team]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
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
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[umpire]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
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
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[atbat] ADD  DEFAULT (getdate()) FOR [create_date]
GO
ALTER TABLE [dbo].[game] ADD  DEFAULT (getdate()) FOR [create_date]
GO
ALTER TABLE [dbo].[pitch] ADD  CONSTRAINT [DF__pitch__create_da__182C9B23]  DEFAULT (getdate()) FOR [create_date]
GO
ALTER TABLE [dbo].[player] ADD  DEFAULT (getdate()) FOR [create_date]
GO
ALTER TABLE [dbo].[player_weight] ADD  DEFAULT (getdate()) FOR [create_date]
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
ALTER TABLE [dbo].[player_weight]  WITH CHECK ADD  CONSTRAINT [FK_player_weight_player] FOREIGN KEY([player])
REFERENCES [dbo].[player] ([id])
GO
ALTER TABLE [dbo].[player_weight] CHECK CONSTRAINT [FK_player_weight_player]
GO
/****** Object:  Trigger [dbo].[ModDate_atbat]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TRIGGER [dbo].[ModDate_atbat]
ON [dbo].[atbat]
AFTER Update
AS
	UPDATE dbo.atbat
	SET modify_date = GETDATE()
	WHERE id IN (SELECT id FROM INSERTED)
GO
/****** Object:  Trigger [dbo].[ModDate_game]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TRIGGER [dbo].[ModDate_game]
ON [dbo].[game]
AFTER UPDATE
AS
	UPDATE dbo.game
	SET modify_date = GETDATE()
	WHERE gid IN (SELECT gid FROM INSERTED)
GO
/****** Object:  Trigger [dbo].[ModDate_pitch]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TRIGGER [dbo].[ModDate_pitch]
ON [dbo].[pitch]
AFTER UPDATE
AS
	UPDATE dbo.pitch
	SET modify_date = GETDATE()
	WHERE id IN (SELECT id FROM INSERTED)
GO
/****** Object:  Trigger [dbo].[ModDate_player]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TRIGGER [dbo].[ModDate_player]
ON [dbo].[player]
AFTER UPDATE
AS
	UPDATE dbo.player
	SET modify_date = GETDATE()
	WHERE id IN (SELECT id FROM INSERTED)
GO
/****** Object:  Trigger [dbo].[ModDate_player_wgt]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TRIGGER [dbo].[ModDate_player_wgt]
ON [dbo].[player_weight]
AFTER UPDATE
AS
	UPDATE dbo.player_weight
	SET modify_date = GETDATE()
	WHERE player + '-' + CAST(season AS VARCHAR(4)) IN (SELECT player + '-' + CAST(season AS VARCHAR(4)) FROM INSERTED)
GO
/****** Object:  Trigger [dbo].[ModDate_team]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TRIGGER [dbo].[ModDate_team]
ON [dbo].[team]
AFTER UPDATE
AS
	UPDATE dbo.team
	SET modify_date = GETDATE()
	WHERE id IN (SELECT id FROM INSERTED)
GO
/****** Object:  Trigger [dbo].[ModDate_umpire]    Script Date: 1/22/2016 11:33:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TRIGGER [dbo].[ModDate_umpire]
ON [dbo].[umpire]
AFTER UPDATE
AS
	UPDATE dbo.umpire
	SET modify_date = GETDATE()
	WHERE id IN (SELECT id FROM INSERTED)
GO
USE [master]
GO
ALTER DATABASE [PitchFX] SET  READ_WRITE 
GO
