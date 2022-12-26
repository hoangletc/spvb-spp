


ALTER PROCEDURE [dbo].[HL_utils_rmTabl]
    (
    @tabName NVARCHAR(100)
)
AS
BEGIN
    DECLARE @sql NVARCHAR(1000);
    IF OBJECT_ID(@tabName) IS NOT NULL
    BEGIN
        PRINT 'Table found: ' + @tabName;
        SET @sql = 'DROP TABLE ' + @tabName;
        EXEC sp_executesql @sql
    END
END
GO
