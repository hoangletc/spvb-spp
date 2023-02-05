


-- CREATE TABLE [dbo].[W_AGING_LEFTOVER](
--     ITEM_NUM        INT          NOT NULL
--     , LAST_ID       INT          DEFAULT NULL
--     , LAST_ACT_DATE DATETIME2[6] DEFAULT NULL
--     , LEFTOVER      INT          DEFAULT 0
-- )
-- drop table [dbo].[W_AGING_LEFTOVER]

-------------------------------------------------------------------------------------------------
-- BEGIN V1 -------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------

-- IF (OBJECT_ID('[dbo].[AGING_V1]') IS NOT NULL)
-- BEGIN
--     DROP PROCEDURE [dbo].[AGING_V1]
-- END;
-- GO

-- CREATE PROCEDURE [dbo].[AGING_V1]
-- AS
-- BEGIN
--     DECLARE @item_num NVARCHAR(10) = '60000750'
--     DECLARE @month SMALLINT = 6;
--     DECLARE @year INT = 2020;

--     DECLARE @leftover FLOAT = 0;
--     DECLARE @qty FLOAT = 0;
--     DECLARE @tmp_lastID NVARCHAR(10) = '';
--     DECLARE @tmp_id_cur NVARCHAR(10) = '';
--     DECLARE @tmp_count int = 1000;

--     DECLARE @usage_month INT = ABS((
--         SELECT SUM(QUANTITY)
--         FROM dbo.W_CMMS_MATU_F
--         WHERE 1=1
--             AND MONTH(ACTUALDATE) = @month
--             AND YEAR(ACTUALDATE) = @year
--         GROUP BY YEAR(ACTUALDATE), MONTH(ACTUALDATE)
--     ))
--     DECLARE @tmp_adate_cur DATETIME = '2010-10-10 12:00:00'

--     WHILE @month BETWEEN 5 and 7
--     BEGIN
--         WHILE (@leftover <= @usage_month)
--         BEGIN
--             -- print '3. Start loop'
                
--             SELECT TOP 1
--                 @tmp_id_cur = MATR_ID
--                 , @tmp_adate_cur = CASE WHEN M.ACTUALDATE IS NULL 
--                 THEN NULL ELSE CONVERT(DATETIMEOFFSET, M.ACTUALDATE) 
--                 END
--                 , @qty = M.QUANTITY
--             FROM dbo.W_CMMS_MATR_F M
--             WHERE 1=1
--                 AND MONTH(M.ACTUALDATE) = @month
--                 AND YEAR(M.ACTUALDATE) = @year
--                 AND M.ACTUALDATE > @tmp_adate_cur
--             ORDER BY M.ACTUALDATE, M.MATR_ID;

--             SET @leftover = @leftover + @qty
--         END

--         -- 2. Update and upsert leftover
--         SET @leftover = @leftover - @usage_month
--         IF NOT EXISTS (SELECT * FROM [dbo].[W_AGING_LEFTOVER] WHERE ITEM_NUM = @item_num)
--             INSERT INTO [dbo].[W_AGING_LEFTOVER](
--                 ITEM_NUM
--                 , LAST_ID
--                 , LEFTOVER
--             ) VALUES (
--                 @item_num
--                 , @tmp_id_cur
--                 , @leftover
--             )
--         ELSE
--             UPDATE [dbo].[W_AGING_LEFTOVER]
--             SET
--                 LAST_ID = @tmp_id_cur
--                 , LEFTOVER = @leftover
--             WHERE 1=1
--                 AND ITEM_NUM = @item_num;

--         -- 
--         SET @month = @month + 1; 
        

--     END;
-- END

-------------------------------------------------------------------------------------------------
-- END V1 ---------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------

-- TRUNCATE TABLE [dbo].[W_AGING_LEFTOVER];
-- INSERT INTO [dbo].[W_AGING_LEFTOVER](
--     ITEM_NUM

-- ) SELECT DISTINCT ITEM_NUM FROM [dbo].[W_CMMS_MATR_F];
-- UPDATE AG SET
--     AG.AGING = NULL
--     , AG.AGING_GRP = NULL
-- FROM [dbo].[W_CMMS_AGING_F] AG 

IF (OBJECT_ID('[dbo].[AGING_V2]') IS NOT NULL)
BEGIN
    DROP PROCEDURE [dbo].[AGING_V2]
END;
GO

CREATE PROCEDURE [dbo].[AGING_V2]
    @month SMALLINT
    , @year INT
AS
BEGIN
    -- DECLARE @month SMALLINT = 6;
    -- DECLARE @year INT = 2020;
    DECLARE @minDT DATETIME2(6) = '2010-01-01 00:00:00.000000';
    DECLARE @lastDateInMonth DATETIME2(6) = EOMONTH(
        CONVERT(varchar(4), @year) + '-' + CONVERT(varchar(2), @month) + '-01'
    )


    -- Clean dump tables
    PRINT '--> 1';

    IF OBJECT_ID(N'tempdb..#TMP_W_CMMS_AGING_ACCUM') IS NOT NULL 
    BEGIN
        PRINT N'DELETE temporary table #TMP_W_CMMS_AGING_ACCUM'
        DROP Table #TMP_W_CMMS_AGING_ACCUM
    END;

    IF OBJECT_ID(N'tempdb..#TMP_TOTAL_USAGE_MONTH') IS NOT NULL 
    BEGIN
        PRINT N'DELETE temporary table #TMP_TOTAL_USAGE_MONTH'
        DROP Table #TMP_TOTAL_USAGE_MONTH
    END;

    IF OBJECT_ID(N'tempdb..#TMP_AGING_LASTDATE') IS NOT NULL 
    BEGIN
        PRINT N'DELETE temporary table #TMP_AGING_LASTDATE'
        DROP Table #TMP_AGING_LASTDATE
    END;


    -- IF OBJECT_ID(N'tempdb..#TMP_AGING') IS NOT NULL 
    -- BEGIN
    --     PRINT N'DELETE temporary table #TMP_AGING'
    --     DROP Table #TMP_AGING
    -- END;


    -- Start
    PRINT '--> 2';

    SELECT
        ITEM_NUM
        , sum(QUANTITY)          AS TOTAL_QTY
    INTO #TMP_TOTAL_USAGE_MONTH
    FROM dbo.W_CMMS_MATU_F
    WHERE 1=1
        AND MONTH(ACTUALDATE) = @month
        AND YEAR(ACTUALDATE) = @year
    GROUP BY ITEM_NUM;

    WITH TMP_ACCUM_SUM AS (
        SELECT
            M1.MATR_ID
            , sum(M2.QUANTITY)          AS TOTAL_QTY
        FROM dbo.W_CMMS_MATR_F M1
            LEFT JOIN [dbo].[W_AGING_LEFTOVER] LO ON 1=1
                AND LO.ITEM_NUM = M1.ITEM_NUM
            LEFT JOIN dbo.W_CMMS_MATR_F M2 ON 1=1
                AND M2.ACTUALDATE <= M1.ACTUALDATE
                AND M2.ACTUALDATE >= ISNULL(LO.LAST_ACT_DATE, @minDT)
                AND M2.ITEM_NUM = M1.ITEM_NUM
            
        WHERE 1=1
            AND MONTH(M1.ACTUALDATE) = @month
            AND YEAR(M1.ACTUALDATE) = @year
        GROUP BY M1.MATR_ID
    )
        SELECT 
            M.MATR_ID
            , M.ITEM_NUM
            , MONTH(M.ACTUALDATE)       AS [MONTH]
            , YEAR(M.ACTUALDATE)        AS [YEAR]
            , M.ACTUALDATE              AS ACTUAL_DATE
            , TMP_ACCUM_SUM.TOTAL_QTY   AS TOTAL_QTY
        INTO #TMP_W_CMMS_AGING_ACCUM
        FROM dbo.W_CMMS_MATR_F M
            LEFT JOIN TMP_ACCUM_SUM ON 1=1
                AND TMP_ACCUM_SUM.MATR_ID = M.MATR_ID
            LEFT JOIN #TMP_TOTAL_USAGE_MONTH USAGE ON 1=1
                AND USAGE.ITEM_NUM = M.ITEM_NUM
            LEFT JOIN [dbo].[W_AGING_LEFTOVER] LO ON 1=1
                AND LO.ITEM_NUM = M.ITEM_NUM
        WHERE 1=1
            AND MONTH(M.ACTUALDATE) = @month
            AND YEAR(M.ACTUALDATE) = @year
            AND TMP_ACCUM_SUM.TOTAL_QTY + LO.LEFTOVER + USAGE.TOTAL_QTY >= 0
        ORDER BY [YEAR], [MONTH];


    -- Store last day
    PRINT '--> 3';

    SELECT
        ISNULL(
            LAST_ACT_DATE, 
            CONVERT(
                DATETIME2(6), 
                '2010-01-01 00:00:00.000000'
            )
        )                                       AS LAST_ACT_DATE
        , ITEM_NUM                              AS ITEM_NUM
    INTO #TMP_AGING_LASTDATE
    FROM [dbo].[W_AGING_LEFTOVER];

    -- Update table LEFTOVER
    PRINT '--> 4';

    UPDATE LO SET
        LO.LAST_ID = ACC.MATR_ID
        , LO.LAST_ACT_DATE = ACC.ACTUAL_DATE
        , LO.LEFTOVER = ACC.TOTAL_QTY + USAGE.TOTAL_QTY
    FROM [dbo].[W_AGING_LEFTOVER] AS LO
        INNER JOIN #TMP_W_CMMS_AGING_ACCUM AS ACC ON 1=1
            AND ACC.ITEM_NUM = LO.ITEM_NUM
        INNER JOIN #TMP_TOTAL_USAGE_MONTH AS USAGE ON 1=1
            AND USAGE.ITEM_NUM = LO.ITEM_NUM;

    SELECT * FROM #TMP_AGING_LASTDATE;
    SELECT * FROM [dbo].[W_AGING_LEFTOVER];


    -- Update table AGING
    PRINT '--> 5';


    -- SELECT
    --     AG.MATR_ID
    --     , AG.ITEM_NUM
    --     , DATEDIFF(DAY, AG.ACTUAL_DATE, 
    --                 @lastDateInMonth)       AS AGING
    --     , AG.ACTUAL_DATE
    --     , LO.LAST_ACT_DATE
    --     , LD.LAST_ACT_DATE
    -- FROM [dbo].[W_CMMS_AGING_F] AG
    --     INNER JOIN [dbo].[W_AGING_LEFTOVER] AS LO ON 1=1
    --         AND LO.ITEM_NUM = AG.ITEM_NUM
    --     INNER JOIN #TMP_AGING_LASTDATE LD ON 1=1
    --         AND LD.ITEM_NUM = AG.ITEM_NUM
    -- WHERE 1=1
    --     AND AG.ACTUAL_DATE BETWEEN LD.LAST_ACT_DATE AND LO.LAST_ACT_DATE;



    WITH TMP_AGING AS (
        SELECT
            AG.MATR_ID
            , AG.ITEM_NUM
            , DATEDIFF(DAY, AG.ACTUAL_DATE, 
                        @lastDateInMonth)       AS AGING
        FROM [dbo].[W_CMMS_AGING_F] AG
            INNER JOIN [dbo].[W_AGING_LEFTOVER] AS LO ON 1=1
                AND LO.ITEM_NUM = AG.ITEM_NUM
            INNER JOIN #TMP_AGING_LASTDATE LD ON 1=1
                AND LD.ITEM_NUM = AG.ITEM_NUM
        WHERE 1=1
            AND AG.ACTUAL_DATE BETWEEN LD.LAST_ACT_DATE AND LO.LAST_ACT_DATE

    )
        UPDATE AG SET
            AG.AGING = TMP_AG.AGING
            , AG.AGING_GRP = CASE 
                WHEN TMP_AG.AGING <= 365 THEN '<= 1 YEAR'
                WHEN TMP_AG.AGING BETWEEN 365     AND 365 * 2 THEN '> 1 YEAR'
                WHEN TMP_AG.AGING BETWEEN 365 * 2 AND 365 * 3 THEN '> 2 YEAR'
                WHEN TMP_AG.AGING BETWEEN 365 * 3 AND 365 * 4 THEN '> 3 YEAR'
                WHEN TMP_AG.AGING BETWEEN 365 * 4 AND 365 * 5 THEN '> 4 YEAR'
                WHEN TMP_AG.AGING > 365 * 5 THEN '> 5 YEAR'
                ELSE NULL 
            END
        FROM [dbo].[W_CMMS_AGING_F] AG
            INNER JOIN TMP_AGING TMP_AG ON 1=1
                AND TMP_AG.MATR_ID = AG.MATR_ID
END;


SELECT * FROM [dbo].[W_CMMS_AGING_F]
ORDER BY ACTUAL_DATE;
SELECT * FROM [dbo].[W_AGING_LEFTOVER]
-- SELECT * FROM #TMP_W_CMMS_AGING_ACCUM


DECLARE @month SMALLINT = 5;
DECLARE @year INT = 2020;

WHILE (@month <= 12)
BEGIN
    EXEC [dbo].[AGING_V2] 
    @month=@month, @year=@year;

    SET @month = @month + 1
END;
