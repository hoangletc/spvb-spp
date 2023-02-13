DECLARE @P_BATCH_ID VARCHAR(8)  = FORMAT(GETDATE(), 'yyyyMMdd');
DECLARE @month      SMALLINT    = 5;
DECLARE @year       INT         = 2022;
DECLARE @fom        DATE        = CONVERT(
    DATE,
    CONCAT_WS('-', CONVERT(varchar, @year), CONVERT(varchar, @month + 1), '01')
);
DECLARE @eom        DATE        = FORMAT(
    EOMONTH(CONCAT_WS('-', CONVERT(varchar, @year), CONVERT(varchar, @month), '01')),
    'yyyyMMdd'
);
DECLARE @mat_num    VARCHAR(9)  = '61331006';
DECLARE @plant_code VARCHAR(4)  = '1030';


SELECT
    DATE_WID
    , PLANT_CODE
    , MATERIAL_NUMBER
    , MOVEMENT_TYPE
    , STOCK_VALUE
    , QUANTITY
    , LOCAL_AMOUNT
 FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
WHERE 1=1
    AND CONVERT(VARCHAR, DATE_WID) < @fom
    AND PLANT_CODE = @plant_code
    AND MATERIAL_NUMBER = @mat_num
ORDER BY DATE_WID;

PRINT '--> 0. Remove tmp tables'

IF OBJECT_ID(N'tempdb..#TMP_TRANS_AGING') IS NOT NULL 
BEGIN
    PRINT N'DELETE temporary table #TMP_TRANS_AGING'
    DROP Table #TMP_TRANS_AGING
END;

IF OBJECT_ID(N'tempdb..#TMP_PIVOT') IS NOT NULL 
BEGIN
    PRINT N'DELETE temporary table #TMP_PIVOT'
    DROP Table #TMP_PIVOT
END;

IF OBJECT_ID(N'tempdb..#TMP_1_ISS') IS NOT NULL 
BEGIN
    PRINT N'DELETE temporary table #TMP_1_ISS'
    DROP Table #TMP_1_ISS
END;

IF OBJECT_ID(N'tempdb..#TMP_1_REC') IS NOT NULL 
BEGIN
    PRINT N'DELETE temporary table #TMP_1_REC'
    DROP Table #TMP_1_REC
END;

IF OBJECT_ID(N'tempdb..#TMP_ACCUM_CUR_MONTH') IS NOT NULL 
BEGIN
    PRINT N'DELETE temporary table #TMP_ACCUM_CUR_MONTH'
    DROP Table #TMP_ACCUM_CUR_MONTH
END;

IF OBJECT_ID(N'tempdb..#TMP_1_AGING') IS NOT NULL 
BEGIN
    PRINT N'DELETE temporary table #TMP_1_AGING'
    DROP Table #TMP_1_AGING
END;


IF OBJECT_ID(N'tempdb..#TMP_REMAINING') IS NOT NULL 
BEGIN
    PRINT N'DELETE temporary table #TMP_REMAINING'
    DROP Table #TMP_REMAINING
END;

IF OBJECT_ID(N'tempdb..#TMP_BALANCE_FINAL') IS NOT NULL 
BEGIN
    PRINT N'DELETE temporary table #TMP_BALANCE_FINAL'
    DROP Table #TMP_BALANCE_FINAL
END;


PRINT '--> 1. Select all receipt transactions grouped by plant, mat_num up to current month';


SELECT 
    PLANT_CODE 
    , MATERIAL_NUMBER                                               AS MAT_NUM 
    , SUM(QUANTITY)                                                 AS ISS_QTY
    , SUM(LOCAL_AMOUNT)                                             AS ISS_AMOUNT
INTO #TMP_1_ISS
FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
WHERE 1=1
    AND CONVERT(VARCHAR, DATE_WID) < @fom
    -- AND MOVEMENT_TYPE IN ('201', '202', '551', '311', '122')
    AND QUANTITY < 0

    -- AND MATERIAL_NUMBER = @mat_num
    -- AND PLANT_CODE = @plant_code
GROUP BY PLANT_CODE, MATERIAL_NUMBER;


SELECT 
    PLANT_CODE
    , MATERIAL_NUMBER                                               AS MAT_NUM
    , CONVERT(DATE, CONVERT(VARCHAR, DATE_WID))                     AS GR_DATE 
    , SUM(QUANTITY)                                                 AS REC_QTY
    , SUM(LOCAL_AMOUNT)                                             AS REC_AMNT
INTO #TMP_1_REC
FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
WHERE 1=1
    AND CONVERT(DATE, CONVERT(VARCHAR, DATE_WID)) < @fom
    -- AND (
    --     MOVEMENT_TYPE NOT IN ('201', '202', '551', '311', '122')
    --     OR MOVEMENT_TYPE IS NULL
    -- )
    AND QUANTITY >=0

    -- AND MATERIAL_NUMBER = @mat_num
    -- AND PLANT_CODE = @plant_code
GROUP BY DATE_WID, PLANT_CODE, MATERIAL_NUMBER;


SELECT 
    R.*
    , I.ISS_QTY
    , I.ISS_AMOUNT
    , SUM(R.REC_QTY) OVER (
        PARTITION BY R.PLANT_CODE, R.MAT_NUM 
        ORDER BY GR_DATE
    )                                                               AS ACCUM_QTY_REC_ONLY
    , SUM(R.REC_AMNT) OVER (
        PARTITION BY R.PLANT_CODE, R.MAT_NUM 
        ORDER BY GR_DATE
    )                                                               AS ACCUM_AMNT_REC_ONLY

    , CASE WHEN SUM(R.REC_QTY) OVER 
        (PARTITION BY R.PLANT_CODE, R.MAT_NUM 
        ORDER BY GR_DATE) + I.ISS_QTY <= 0 THEN 0 
        ELSE 1 
    END                                                             AS FLG_LEFTOVER_QTY
    , CASE WHEN SUM(R.REC_AMNT) OVER 
        (PARTITION BY R.PLANT_CODE, R.MAT_NUM 
        ORDER BY GR_DATE) + I.ISS_AMOUNT <= 0 THEN 0 
        ELSE 1 
    END                                                             AS FLG_LEFTOVER_AMNT
INTO #TMP_ACCUM_CUR_MONTH
FROM #TMP_1_REC R
    LEFT JOIN #TMP_1_ISS I ON 1=1
        AND R.PLANT_CODE = I.PLANT_CODE 
        AND R.MAT_NUM = I.MAT_NUM;



WITH TMP_BALANCE AS (
SELECT
    PLANT_CODE
    , MAT_NUM
    , GR_DATE
    , CASE WHEN FLG_LEFTOVER_QTY = 0 THEN NULL 
        WHEN GR_DATE = MIN(GR_DATE) OVER (
            PARTITION BY PLANT_CODE, MAT_NUM, FLG_LEFTOVER_QTY
        ) THEN ISNULL(ISS_QTY, 0) + ACCUM_QTY_REC_ONLY
        ELSE REC_QTY
    END                                                             AS REMAINING_QTY
    , CASE WHEN GR_DATE = MAX(GR_DATE) OVER (
            PARTITION BY PLANT_CODE, MAT_NUM
        ) THEN ISNULL(ISS_AMOUNT, 0) + ACCUM_AMNT_REC_ONLY
        ELSE 0
    END                                                             AS REMAINING_AMNT
    FROM #TMP_ACCUM_CUR_MONTH
)
    SELECT
        PLANT_CODE
        , MAT_NUM
        , GR_DATE
        , REMAINING_AMNT
        , REMAINING_QTY
        , CASE WHEN REMAINING_QTY > 0 
            THEN DATEDIFF(M, GR_DATE, CONVERT(DATE, @fom)) 
            ELSE NULL 
        END                                                           AS AGING_MONTH
    INTO #TMP_1_AGING
    FROM TMP_BALANCE;   


SELECT
    PLANT_CODE
    , MAT_NUM

    , SUM(REMAINING_AMNT)   AS RMN_AMNT
    , SUM(REMAINING_QTY)    AS RMN_QTY
INTO #TMP_REMAINING
FROM #TMP_1_AGING
GROUP BY PLANT_CODE, MAT_NUM;

SELECT
    CASE WHEN ACC.AGING_MONTH < 4 THEN '< 4 MONTHS'
        WHEN ACC.AGING_MONTH BETWEEN 4 AND 12 THEN '4 - 12 MONTHS'
        WHEN ACC.AGING_MONTH BETWEEN 12 AND 24 THEN '1 - 2 YEARS'
        WHEN ACC.AGING_MONTH BETWEEN 24 AND 36 THEN '2 - 3 YEARS'
        WHEN ACC.AGING_MONTH BETWEEN 36 AND 48 THEN '3 - 4 YEARS'
        WHEN ACC.AGING_MONTH BETWEEN 48 AND 60 THEN '4 - 5 YEARS'
        ELSE '> 5 YEARS'
    END                                                             AS AGING_GROUP
    , ACC.PLANT_CODE
    , ACC.MAT_NUM
    , ACC.GR_DATE
    , ACC.REMAINING_QTY
    , ACC.REMAINING_AMNT
    , TRANS.PURCHASING_GROUP
    , TRANS.STORAGE_LOCATION
    , TRANS.COMPANY_CODE
    , TRANS.VALUATION_AREA
    , TRANS.VALUATION_CLASS
    , TRANS.MATERIAL_TYPE
    , TRANS.MATERIAL_GROUP
    , TRANS.BASE_UNIT_OF_MEASURE
    , TRANS.CURRENCY
    , TRANS.PRICE_CONTROL
INTO #TMP_TRANS_AGING
FROM #TMP_1_AGING ACC
    LEFT JOIN [dbo].[W_SAP_SPP_TRANSACTION_F] TRANS ON 1=1
        AND TRANS.DATE_WID = CONVERT(INT, FORMAT(ACC.GR_DATE, 'yyyyMMdd'))
        AND TRANS.PLANT_CODE = ACC.PLANT_CODE
        AND TRANS.MATERIAL_NUMBER = ACC.MAT_NUM
        AND (
            TRANS.MOVEMENT_TYPE NOT IN ('201', '202', '901', '902') 
            OR TRANS.MOVEMENT_TYPE IS NULL
        );



-- SELECT * FROM #TMP_1_ISS;
-- SELECT * FROM #TMP_1_REC  ORDER BY GR_DATE;
-- SELECT * FROM #TMP_ACCUM_CUR_MONTH ORDER BY GR_DATE;
-- SELECT * FROM #TMP_1_AGING  ORDER BY GR_DATE;;
-- SELECT * FROM #TMP_REMAINING;

-- select sum(RMN_AMNT), sum(RMN_QTY) from #TMP_REMAINING;
























-- SELECT
--     SUM(ISS_AMOUNT) AS TOT_AMNT
--     , SUM(ISS_QTY) AS TOT_QTY
-- FROM #TMP_1_ISS;
-- SELECT
--     SUM(REC_AMNT) AS TOT_AMNT
--     , SUM(REC_QTY) AS TOT_QTY
-- FROM #TMP_1_REC;

-- SELECT * FROM #TMP_1_REC  ORDER BY GR_DATE;

-- WITH XXX AS (
--     SELECT
--         PLANT_CODE
--         , MATERIAL_NUMBER
--         , SUM(LOCAL_AMOUNT) AS SUM_LOCAL_AMNT
--         , SUM(QUANTITY)     AS SUM_QTY
--     FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
--     WHERE 1=1
--         and CONVERT(VARCHAR, DATE_WID) < '20220701'
--     GROUP BY MATERIAL_NUMBER, PLANT_CODE
-- )
--     SELECT
--         R.MAT_NUM
--         , R.PLANT_CODE
--         , RMN_AMNT              AS CAL_AMNT
--         , RMN_QTY               AS CAL_QTY
--         , XXX.SUM_LOCAL_AMNT    AS REF_AMNT
--         , XXX.SUM_QTY           AS REF_QTY

--     FROM #TMP_REMAINING R
--     JOIN XXX ON 1=1
--         AND XXX.PLANT_CODE = R.PLANT_CODE
--         AND XXX.MATERIAL_NUMBER = R.MAT_NUM
--     WHERE 1=1
--         AND (
--             ISNULL(RMN_AMNT, 0) <> XXX.SUM_LOCAL_AMNT
--             OR ISNULL(RMN_QTY, 0) <> XXX.SUM_QTY
--         );















-- DECLARE @month      SMALLINT    = 5;
-- DECLARE @year       INT         = 2022;
-- DECLARE @fom        DATE        = CONVERT(
--     DATE,
--     CONCAT_WS('-', CONVERT(varchar, @year), CONVERT(varchar, @month + 1), '01')
-- );
-- DECLARE @eom        DATE        = FORMAT(
--     EOMONTH(CONCAT_WS('-', CONVERT(varchar, @year), CONVERT(varchar, @month), '01')),
--     'yyyyMMdd'
-- )
-- DECLARE @mat_num    VARCHAR(9)  = '60004372';
-- DECLARE @plant_code VARCHAR(4)  = '1040';

-- SELECT * FROM #TMP_ACCUM_CUR_MONTH;
-- SELECT *
-- FROM #TMP_1_REC
-- WHERE 1=1
--     AND MAT_NUM = @mat_num
--     AND PLANT_CODE = @plant_code
--     AND GR_DATE < @fom;

-- SELECT *
-- FROM #TMP_1_AGING
-- WHERE 1=1
--     AND MAT_NUM = @mat_num
--     AND PLANT_CODE = @plant_code;

-- SELECT
--     -- DATE_WID
--     -- , PLANT_CODE
--     -- , MATERIAL_NUMBER
--     -- , MOVEMENT_TYPE
--     -- , QUANTITY
--     @eom                AS [PERIOD]
--     , SUM(REMAINING_AMNT) AS SUM_AMNT
--     , SUM(REMAINING_QTY)  AS SUM_QTY
-- FROM #TMP_1_AGING WHERE 1=1
--     AND MAT_NUM = @mat_num
--     AND PLANT_CODE = @plant_code
--     and GR_DATE < @fom
-- GROUP BY MAT_NUM, PLANT_CODE;








PRINT '--> 2. Pivot';

SELECT
    TMP.MAT_NUM
    , TMP.PLANT_CODE
    , [< 4 MONTHS]
    , [4 - 12 MONTHS]
    , [1 - 2 YEARS]
    , [2 - 3 YEARS]
    , [3 - 4 YEARS]
    , [4 - 5 YEARS]
INTO #TMP_PIVOT
FROM (
    SELECT MAT_NUM, PLANT_CODE, AGING_GROUP
    FROM #TMP_TRANS_AGING
) AS SB
PIVOT (
    COUNT(AGING_GROUP)
    FOR AGING_GROUP IN (
        [< 4 MONTHS]
        , [4 - 12 MONTHS]
        , [1 - 2 YEARS]
        , [2 - 3 YEARS]
        , [3 - 4 YEARS]
        , [4 - 5 YEARS]
    )
) AS TMP;


PRINT '--> 3. Create balance_month table';

-- select count(*) from #TMP_PIVOT;
-- select count(*) from #TMP_TRANS_AGING;

-- DECLARE @P_BATCH_ID VARCHAR(8)  = FORMAT(GETDATE(), 'yyyyMMdd');
-- DECLARE @month      SMALLINT    = 5;
-- DECLARE @year       INT         = 2022;
-- DECLARE @fom        DATE        = CONVERT(
--     DATE,
--     CONCAT_WS('-', CONVERT(varchar, @year), CONVERT(varchar, @month + 1), '01')
-- );
-- DECLARE @eom        DATE        = FORMAT(
--     EOMONTH(CONCAT_WS('-', CONVERT(varchar, @year), CONVERT(varchar, @month), '01')),
--     'yyyyMMdd'
-- );

WITH TMP_B AS(
    SELECT
        PLANT_CODE
        , MAT_NUM
        , [< 4 MONTHS]
        , [4 - 12 MONTHS]
        , [1 - 2 YEARS]
        , [2 - 3 YEARS]
        , [3 - 4 YEARS]
        , [4 - 5 YEARS]
        , [4 - 12 MONTHS] + [1 - 2 YEARS]
        + [2 - 3 YEARS] + [3 - 4 YEARS] + [4 - 5 YEARS]     AS TOTAL_LEFTOVER
        , [3 - 4 YEARS] + [4 - 5 YEARS]                     AS SLOW_MOVING
    FROM #TMP_PIVOT
),
TMP_AG AS (
    SELECT *
    , ROW_NUMBER() OVER (PARTITION BY T.PLANT_CODE, T.MAT_NUM ORDER BY GR_DATE DESC) AS RN
    FROM #TMP_TRANS_AGING T
)
    SELECT
        CONVERT(VARCHAR, FORMAT(@eom, 'yyyyMMdd'))          AS DATE_WID
        , PL.PLANT_WID                                      AS PLANT_WID
        , @eom                                              AS [PERIOD]
        , TMP_B.PLANT_CODE
        , PL.PLANT_NAME_2									AS PLANT
        , TMP_B.MAT_NUM
        , R.RMN_QTY                                         AS QUANTITY
        , R.RMN_AMNT                                        AS AMOUNT
        , NULL												AS [TYPE]
        , [< 4 MONTHS]
        , [4 - 12 MONTHS]
        , [1 - 2 YEARS]
        , [2 - 3 YEARS]
        , [3 - 4 YEARS]
        , [4 - 5 YEARS]
        , TMP_AG.STORAGE_LOCATION
        , TMP_AG.COMPANY_CODE
        , TMP_AG.VALUATION_AREA
        , TMP_AG.VALUATION_CLASS
        , TMP_AG.MATERIAL_TYPE
        , TMP_AG.MATERIAL_GROUP
        , TMP_AG.BASE_UNIT_OF_MEASURE
        , TMP_AG.CURRENCY
        , TMP_AG.PRICE_CONTROL
        , MM.MAX                                            AS MAX
        , MM.MIN                                            AS MIN
        , CASE WHEN TOTAL_LEFTOVER > MM.MAX
            THEN TOTAL_LEFTOVER - MM.MAX
            ELSE 0
        END                                                 AS OVER_MAX
        , CASE WHEN TOTAL_LEFTOVER < MM.MIN
            THEN MM.MIN - TOTAL_LEFTOVER
            ELSE 0
        END                                                 AS UNDER_MIN
        , SLOW_MOVING
        , CONCAT_WS(
            '~' 
            , CONVERT(VARCHAR, FORMAT(@eom, 'yyyyMMdd'))
            , TMP_B.MAT_NUM
            , TMP_B.PLANT_CODE
            , TMP_AG.STORAGE_LOCATION
        )                                                   AS W_INTEGRATION_ID
        , 'N'                                               AS W_DELETE_FLG
        , 1                                                 AS W_DATASOURCE_NUM_ID
        , GETDATE()                                         AS W_INSERT_DT
        , GETDATE()                                         AS W_UPDATE_DT
        , NULL                                              AS W_BATCH_ID
        , 'N'                                               AS W_UPDATE_FLG
        INTO #TMP_BALANCE_FINAL
        FROM TMP_B
            LEFT JOIN TMP_AG ON 1=1
                AND TMP_AG.PLANT_CODE = TMP_B.PLANT_CODE
                AND TMP_AG.MAT_NUM = TMP_B.MAT_NUM     
                AND TMP_AG.RN = 1           
            LEFT JOIN #TMP_REMAINING R ON 1=1
                AND TMP_B.PLANT_CODE = R.PLANT_CODE
                AND TMP_B.MAT_NUM = R.MAT_NUM
            LEFT JOIN [FND].[W_CMMS_MINMAX_D] MM ON 1=1
                AND MM.ITEM_NUM = TMP_B.MAT_NUM
                AND MM.PLANT = TMP_B.PLANT_CODE
            LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL ON 1=1
                AND PL.PLANT = TMP_B.PLANT_CODE
                AND PL.STO_LOC = TMP_AG.STORAGE_LOCATION;

-- SELECT COUNT(*) FROM #TMP_PIVOT;
-- SELECT COUNT(*) FROM #TMP_BALANCE_FINAL;

SELECT
    SUM(AMOUNT)         AS TOTAL_AMNT
    , SUM(QUANTITY)          AS TOTAL_QTY
FROM #TMP_BALANCE_FINAL
GROUP BY PLANT_CODE;

SELECT
    SUM(REMAINING_AMNT)         AS TOTAL_AMNT
    , SUM(REMAINING_QTY)          AS TOTAL_QTY
FROM #TMP_1_AGING;


SELECT
    top 10 *
FROM #TMP_BALANCE_FINAL;


-- SELECT
--     MAX
--     , MIN
--     , PLANT
--     , ITEM_NUM
-- FROM (
--     SELECT *,
--         COUNT(*) OVER (PARTITION BY PLANT, ITEM_NUM) AS N
--     FROM [FND].[W_CMMS_MINMAX_D]
-- ) XXX WHERE N > 1


-- SELECT * FROM [FND].[W_CMMS_MINMAX_D] WHERE ITEM_NUM = '61194084'


-- SELECT TOP 10 * FROM [dbo].[W_SAP_PLANT_EXTENDED_D];
-- sp_rename '[dbo].[W_SAP_PLANT_EXTENDED_D].LGOBE', 'STO_LOC_DES', 'COLUMN';