USE [PitchFX]
GO

/****** Object:  StoredProcedure [dbo].[sp_ImportUmpires]    Script Date: 9/29/2016 6:31:32 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[sp_ImportUmpires]
(
	@gid VARCHAR(50),
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


