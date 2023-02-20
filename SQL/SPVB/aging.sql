-- drop table [dbo].[W_AGING_LEFTOVER]
-- CREATE TABLE [dbo].[W_AGING_LEFTOVER](
--     ITEM_NUM        INT          NOT NULL
--     , LAST_ID       INT          DEFAULT NULL
--     , LAST_ACT_DATE DATETIME2(6) DEFAULT NULL
--     , LEFTOVER      INT          DEFAULT 0
-- )
--     , PLANT         NVARCHAR(10) NOT NULL   

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
IF (OBJECT_ID('[dbo].[AGING_V2]') IS NOT NULL)
BEGIN
    DROP PROCEDURE [dbo].[AGING_V2]
END;
GO

CREATE PROCEDURE [dbo].[AGING_V2]
    @month SMALLINT
    ,
    @year INT
AS
BEGIN
    DECLARE @minDT DATETIME2(6) = '2010-01-01 00:00:00.000000';
    DECLARE @lastDateInMonth DATETIME2(6) = DATEADD(
        DAY, 
        1, 
        EOMONTH(
            CONVERT(varchar(4), @year) + '-' + CONVERT(varchar(2), @month) + '-01'
        )
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

    IF OBJECT_ID(N'tempdb..#TMP_LAST_ROW_DELTA') IS NOT NULL 
    BEGIN
        PRINT N'DELETE temporary table #TMP_LAST_ROW_DELTA'
        DROP Table #TMP_LAST_ROW_DELTA
    END;


    -- Create and update tmp tables
    PRINT '--> 2';

    SELECT
        ITEM_NUM
        -- , PLANT                 AS PLANT
        , sum(QUANTITY)         AS TOTAL_QTY
    INTO #TMP_TOTAL_USAGE_MONTH
    FROM dbo.W_CMMS_MATU_F
    WHERE 1=1
        AND MONTH(ACTUALDATE) = @month
        AND YEAR(ACTUALDATE) = @year
    GROUP BY ITEM_NUM;

    -- [?] Tại sao lại tồn tại bảng #TMP_LAST_ROW_DELTA ?
    -- Khi tính accum sum cho từng dòng bắt đầu từ last date của leftover,
    -- câu query sẽ tính dựa vào bảng MATR, mà con số QUANTITY của MATR
    -- là số gốc của transaction đó chứ không phải số còn lại sau khi tính aging
    -- nên bảng TMP_LAST_ROW_DELTA sẽ lưu lại phần cần trừ của last date trong leftover
    SELECT
        DISTINCT M.ITEM_NUM     AS ITEM_NUM
        , 0                     AS DELTA
    INTO #TMP_LAST_ROW_DELTA
    FROM dbo.W_CMMS_MATR_F M;

    WITH
        TMP_LEFTOVER
        AS
        (
            SELECT
                LO.ITEM_NUM                     AS ITEM_NUM
            , M.QUANTITY - LO.LEFTOVER      AS DELTA
            FROM [dbo].[W_AGING_LEFTOVER] LO
                JOIN [dbo].[W_CMMS_MATR_F] M ON 1=1
                    AND LO.LAST_ID = M.MATR_ID
        )
        UPDATE D SET 
            D.DELTA = ISNULL(TMP_LEFTOVER.DELTA, 0)
        FROM #TMP_LAST_ROW_DELTA AS D
        LEFT JOIN TMP_LEFTOVER ON 1=1
            AND TMP_LEFTOVER.ITEM_NUM = D.ITEM_NUM;


    -- Calculate accum sum table
    PRINT '--> 3';

    WITH
        TMP_ACCUM_SUM
        AS
        (
            SELECT
                M1.MATR_ID
            , sum(M2.QUANTITY)                  AS TOTAL_QTY
            FROM dbo.W_CMMS_MATR_F M1
                LEFT JOIN [dbo].[W_AGING_LEFTOVER] LO ON 1=1
                    AND LO.ITEM_NUM = M1.ITEM_NUM
                LEFT JOIN dbo.W_CMMS_MATR_F M2 ON 1=1
                    AND M2.ACTUALDATE BETWEEN ISNULL(LO.LAST_ACT_DATE, @minDT) AND M1.ACTUALDATE
                    AND M2.ITEM_NUM = M1.ITEM_NUM
            WHERE 1=1
                AND M1.ACTUALDATE BETWEEN ISNULL(LO.LAST_ACT_DATE, @minDT) and @lastDateInMonth
            GROUP BY M1.MATR_ID
        )
    SELECT
        M.MATR_ID
            , M.ITEM_NUM
            , MONTH(M.ACTUALDATE)                   AS [MONTH]
            , YEAR(M.ACTUALDATE)                    AS [YEAR]
            , M.ACTUALDATE                          AS ACTUAL_DATE
            , TMP_ACCUM_SUM.TOTAL_QTY - D.DELTA AS TOTAL_QTY
    INTO #TMP_W_CMMS_AGING_ACCUM
    FROM dbo.W_CMMS_MATR_F M
        LEFT JOIN TMP_ACCUM_SUM ON 1=1
            AND TMP_ACCUM_SUM.MATR_ID = M.MATR_ID
        LEFT JOIN #TMP_TOTAL_USAGE_MONTH USAGE ON 1=1
            AND USAGE.ITEM_NUM = M.ITEM_NUM
        LEFT JOIN #TMP_LAST_ROW_DELTA D ON 1=1
            AND D.ITEM_NUM = M.ITEM_NUM
    WHERE 1=1
        AND MONTH(M.ACTUALDATE) = @month
        AND YEAR(M.ACTUALDATE) = @year
        AND TMP_ACCUM_SUM.TOTAL_QTY - D.DELTA + USAGE.TOTAL_QTY >= 0
    ORDER BY ACTUAL_DATE;


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
            AND ACC.TOTAL_QTY = (
                SELECT MIN(A.TOTAL_QTY)
            FROM #TMP_W_CMMS_AGING_ACCUM AS A
            WHERE 1=1 AND A.ITEM_NUM = ACC.ITEM_NUM
            )
        INNER JOIN #TMP_TOTAL_USAGE_MONTH AS USAGE ON 1=1
            AND USAGE.ITEM_NUM = LO.ITEM_NUM;


    -- Update table AGING
    PRINT '--> 5';

    WITH
        TMP_AGING AS
        (
            SELECT
                AG.MATR_ID                          AS MATR_ID
            , AG.ITEM_NUM
            , DATEDIFF(DAY, AG.ACTUAL_DATE, 
                        @lastDateInMonth)       AS AGING
            , CASE WHEN AG.ACTUAL_DATE < LO.LAST_ACT_DATE
                THEN 0
                WHEN AG.ACTUAL_DATE = LO.LAST_ACT_DATE
                    THEN LO.LEFTOVER
                ELSE M.QUANTITY
            END                                 AS UPDATE_QTY
            FROM [dbo].[W_CMMS_AGING_F] AG
                JOIN [dbo].[W_AGING_LEFTOVER] AS LO ON 1=1
                    AND LO.ITEM_NUM = AG.ITEM_NUM
                JOIN [dbo].[W_CMMS_MATR_F] AS M ON 1=1
                    AND M.MATR_ID = AG.MATR_ID
                JOIN #TMP_AGING_LASTDATE AS TMP_AG ON 1=1
                    AND TMP_AG.ITEM_NUM = AG.ITEM_NUM
            WHERE 1=1
                AND AG.ACTUAL_DATE BETWEEN TMP_AG.LAST_ACT_DATE AND @lastDateInMonth
        )
        UPDATE AG SET
            AG.LEFTOVER_QTY = TMP_AG.UPDATE_QTY
            , AG.AGING_GRP = CASE 
                WHEN TMP_AG.UPDATE_QTY = 0 THEN NULL
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


    -- Pivot AGING
    PRINT '--> 6';


END;


DECLARE @month SMALLINT = 7;
DECLARE @year INT = 2020;
WHILE (@month <= 7)
BEGIN
    EXEC [dbo].[AGING_V2] @month=@month, @year=@year;
    SET @month = @month + 1
END;



SELECT * FROM [dbo].[W_CMMS_AGING_F] ORDER BY ACTUAL_DATE;
SELECT * FROM [dbo].[W_AGING_LEFTOVER]


-- TODO: Continue with pivot
SELECT * 
INTO #TMP_AGING_OUTPUT
FROM (
    SELECT *
    FROM [dbo].[W_CMMS_AGING_F]
    PIVOT (
        SUM(LEFTOVER_QTY)
        FOR AGING_GRP IN (
            [<= 1 YEAR]
            , [> 1 YEAR]
            , [> 2 YEAR]
            , [> 3 YEAR]
            , [> 4 YEAR]
        )
    ) AS TMP
) X


SELECT * FROM #TMP_AGING_OUTPUT