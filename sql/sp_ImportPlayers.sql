USE [PitchFX]
GO

/****** Object:  StoredProcedure [dbo].[sp_ImportPlayers]    Script Date: 9/29/2016 6:31:28 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[sp_ImportPlayers]
(
	@fileLoc VARCHAR(255)
)
AS
SET XACT_ABORT ON;
BEGIN TRANSACTION
	DECLARE @xml XML, @QryStr NVARCHAR(MAX);

	SET @QryStr = '
	SELECT @XML_out = BulkColumn
	FROM OPENROWSET(
		BULK '''+@fileLoc+'''
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
COMMIT TRANSACTION;
GO


