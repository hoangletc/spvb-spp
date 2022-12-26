
ALTER PROCEDURE [dbo].[HL_utils_rmProc]
    (
    @procName NVARCHAR(100)
)
AS BEGIN
    DECLARE @sql NVARCHAR(1000);
    BEGIN
        PRINT 'Procedure found: ' + @procname
        SET @sql = 'DROP PROC ' + @procname;
        EXEC sp_executesql @sql
    END
END
GO
