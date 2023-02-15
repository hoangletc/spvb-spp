DECLARE @p_batch_id VARCHAR(8)  = FORMAT(GETDATE(), 'yyyyMMdd');
DECLARE @period     DATE        = '20220430';
DECLARE @eom        DATE        = EOMONTH(@period);
DECLARE @mat_num    VARCHAR(9)  = '60000003';
DECLARE @plant_code VARCHAR(4)  = '1030';
DECLARE @sloc       VARCHAR(4)  = 'SP01';


-- SELECT
--     DATE_WID
--     , PLANT_CODE
--     , MATERIAL_NUMBER
--     , MOVEMENT_TYPE
--     , STOCK_VALUE
--     , QUANTITY
--     , LOCAL_AMOUNT
--  FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
-- WHERE 1=1
--     AND CONVERT(VARCHAR, DATE_WID) <= @eom
--     AND PLANT_CODE = @plant_code
--     AND MATERIAL_NUMBER = @mat_num
-- ORDER BY DATE_WID;

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


IF OBJECT_ID(N'tempdb..#TMP_MVM_TYPE') IS NOT NULL 
BEGIN
    PRINT N'DELETE temporary table #TMP_MVM_TYPE'
    DROP Table #TMP_MVM_TYPE
END;


PRINT '--> 1. Select all receipt transactions grouped by plant, mat_num up to current month';


SELECT 
    PLANT_CODE 
    , MATERIAL_NUMBER                                               AS MAT_NUM
    , STORAGE_LOCATION                                              AS STO_LOC
    , SUM(QUANTITY)                                                 AS ISS_QTY
    , SUM(LOCAL_AMOUNT)                                             AS ISS_AMOUNT
INTO #TMP_1_ISS
FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
WHERE 1=1
    -- AND MOVEMENT_TYPE IN ('201', '202', '551', '311', '122')
    AND QUANTITY < 0

    -- AND MATERIAL_NUMBER = @mat_num
    -- AND PLANT_CODE = @plant_code
    -- AND STORAGE_LOCATION = @sloc
    AND CONVERT(VARCHAR, DATE_WID) <= @eom
GROUP BY PLANT_CODE, MATERIAL_NUMBER, STORAGE_LOCATION;


SELECT 
    PLANT_CODE
    , MATERIAL_NUMBER                                               AS MAT_NUM
    , STORAGE_LOCATION                                              AS STO_LOC
    , CONVERT(DATE, CONVERT(VARCHAR, DATE_WID))                     AS GR_DATE 
    , SUM(QUANTITY)                                                 AS REC_QTY
    , SUM(LOCAL_AMOUNT)                                             AS REC_AMNT
INTO #TMP_1_REC
FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
WHERE 1=1
    -- AND (
    --     MOVEMENT_TYPE NOT IN ('201', '202', '551', '311', '122')
    --     OR MOVEMENT_TYPE IS NULL
    -- )
    AND QUANTITY >=0

    -- AND MATERIAL_NUMBER = @mat_num
    -- AND PLANT_CODE = @plant_code
    -- AND STORAGE_LOCATION = @sloc
    AND CONVERT(DATE, CONVERT(VARCHAR, DATE_WID)) <= @eom
GROUP BY DATE_WID, PLANT_CODE, MATERIAL_NUMBER, STORAGE_LOCATION;

SELECT DISTINCT F.MOVEMENT_TYPE
INTO #TMP_MVM_TYPE
FROM [dbo].[W_SAP_SPP_TRANSACTION_F] F, #TMP_1_REC R
WHERE 1=1
    AND F.PLANT_CODE = R.PLANT_CODE
    AND F.MATERIAL_NUMBER = R.MAT_NUM
    AND F.STORAGE_LOCATION = R.STO_LOC
;

WITH TMP_ACCUM AS (
    SELECT
        R.PLANT_CODE
        , R.MAT_NUM
        , R.STO_LOC
        , R.GR_DATE
        , SUM(R.REC_QTY) OVER (
            PARTITION BY R.PLANT_CODE, R.MAT_NUM, R.STO_LOC
        )     AS TOT_QTY 
        , SUM(R.REC_AMNT) OVER (
            PARTITION BY R.PLANT_CODE, R.MAT_NUM, R.STO_LOC
        )    AS TOT_AMNT
        , SUM(R.REC_QTY) OVER (
            PARTITION BY R.PLANT_CODE, R.MAT_NUM, R.STO_LOC
            ORDER BY GR_DATE
        )     AS ACCUM_QTY_REC_ONLY
        , SUM(R.REC_AMNT) OVER (
            PARTITION BY R.PLANT_CODE, R.MAT_NUM, R.STO_LOC
            ORDER BY GR_DATE
        )     AS ACCUM_AMNT_REC_ONLY
    FROM #TMP_1_REC R    
)
    SELECT 
        R.*
        , I.ISS_QTY
        , I.ISS_AMOUNT
        , A.ACCUM_QTY_REC_ONLY
        , A.ACCUM_AMNT_REC_ONLY
        , ISNULL(ISS_QTY, 0) + A.TOT_QTY                                AS RMN_QTY
        , ISNULL(ISS_AMOUNT, 0) + A.TOT_AMNT                            AS RMN_AMNT

        , CASE WHEN ACCUM_QTY_REC_ONLY + I.ISS_QTY <= 0 THEN 0 ELSE 1 
        END                                                             AS FLG_LEFTOVER_QTY
        , CASE WHEN ACCUM_AMNT_REC_ONLY + I.ISS_AMOUNT <= 0 THEN 0 ELSE 1 
        END                                                             AS FLG_LEFTOVER_AMNT
    INTO #TMP_ACCUM_CUR_MONTH
    FROM #TMP_1_REC R
        LEFT JOIN #TMP_1_ISS I ON 1=1
            AND R.PLANT_CODE = I.PLANT_CODE 
            AND R.MAT_NUM = I.MAT_NUM
            AND R.STO_LOC = I.STO_LOC
        LEFT JOIN TMP_ACCUM A ON 1=1
            AND A.[PLANT_CODE] = R.PLANT_CODE
            AND A.MAT_NUM = R.MAT_NUM
            AND A.STO_LOC = R.STO_LOC
            AND A.GR_DATE = R.GR_DATE
;

WITH TMP_BALANCE AS (
SELECT
    PLANT_CODE
    , MAT_NUM
    , STO_LOC
    , GR_DATE
    , CASE WHEN FLG_LEFTOVER_QTY = 0 THEN 
            ( CASE WHEN RMN_QTY < 0 AND GR_DATE = MAX(GR_DATE) OVER (
                    PARTITION BY PLANT_CODE, MAT_NUM, STO_LOC
                ) THEN ACCUM_QTY_REC_ONLY + ISNULL(ISS_QTY, 0)
                ELSE NULL END
            ) 
        WHEN GR_DATE = MIN(GR_DATE) OVER (
            PARTITION BY PLANT_CODE, MAT_NUM, STO_LOC, FLG_LEFTOVER_QTY
        ) THEN ACCUM_QTY_REC_ONLY + ISNULL(ISS_QTY, 0)
        ELSE REC_QTY
    END                                                             AS REMAINING_QTY
    , CASE WHEN GR_DATE = MAX(GR_DATE) OVER (
            PARTITION BY PLANT_CODE, MAT_NUM, STO_LOC
        ) THEN ACCUM_AMNT_REC_ONLY + ISNULL(ISS_AMOUNT, 0)
        ELSE 0
    END                                                             AS REMAINING_AMNT
    FROM #TMP_ACCUM_CUR_MONTH
)
    SELECT
        PLANT_CODE
        , MAT_NUM
        , STO_LOC
        , GR_DATE
        , REMAINING_AMNT
        , REMAINING_QTY
        , CASE WHEN REMAINING_QTY > 0 
            THEN DATEDIFF(M, GR_DATE, CONVERT(DATE, @eom)) 
            ELSE NULL 
        END                                                           AS AGING_MONTH
    INTO #TMP_1_AGING
    FROM TMP_BALANCE
;   

INSERT INTO #TMP_1_AGING
SELECT
    T.PLANT_CODE
    , T.MATERIAL_NUMBER                                                 AS MAT_NUM
    , T.STORAGE_LOCATION                                                AS STO_LOC
    , CONVERT(DATE, CONVERT(VARCHAR, DATE_WID))                         AS GR_DATE
    , LOCAL_AMOUNT                                                      AS REMAINING_AMNT
    , QUANTITY                                                          AS REMAINING_QTY
    , -1                                                                AS AGING_MONTH
FROM [dbo].[W_SAP_SPP_TRANSACTION_F] T
WHERE 1=1
    and CONVERT(VARCHAR, DATE_WID) <= @eom
    AND NOT EXISTS (
        SELECT MAT_NUM, PLANT_CODE, STO_LOC FROM #TMP_1_AGING A
        WHERE 1=1
            AND T.MATERIAL_NUMBER = A.MAT_NUM
            AND T.PLANT_CODE = A.PLANT_CODE
            AND T.STORAGE_LOCATION = A.STO_LOC
    )
;

SELECT
    PLANT_CODE
    , MAT_NUM
    , STO_LOC

    , SUM(REMAINING_AMNT)   AS RMN_AMNT
    , SUM(REMAINING_QTY)    AS RMN_QTY
INTO #TMP_REMAINING
FROM #TMP_1_AGING
GROUP BY PLANT_CODE, MAT_NUM, STO_LOC;

SELECT
    CASE WHEN ACC.AGING_MONTH BETWEEN 0 AND 4 THEN '< 4 MONTHS'
        WHEN ACC.AGING_MONTH BETWEEN 4 AND 12 THEN '4 - 12 MONTHS'
        WHEN ACC.AGING_MONTH BETWEEN 12 AND 24 THEN '1 - 2 YEARS'
        WHEN ACC.AGING_MONTH BETWEEN 24 AND 36 THEN '2 - 3 YEARS'
        WHEN ACC.AGING_MONTH BETWEEN 36 AND 48 THEN '3 - 4 YEARS'
        WHEN ACC.AGING_MONTH BETWEEN 48 AND 60 THEN '4 - 5 YEARS'
        WHEN ACC.AGING_MONTH > 60 THEN '> 5 YEARS'
        ELSE 'AGE_NULL'
    END                                                             AS AGING_GROUP
    , ACC.PLANT_CODE
    , ACC.MAT_NUM
    , ACC.GR_DATE
    , ACC.STO_LOC
    , ACC.REMAINING_QTY
    , ACC.REMAINING_AMNT
INTO #TMP_TRANS_AGING
FROM #TMP_1_AGING ACC
;


---------- 
-- Testing section
----------
-- DECLARE @period     DATE        = '20220531';
-- DECLARE @mat_num    VARCHAR(9)  = '60000003';
-- DECLARE @plant_code VARCHAR(4)  = '1030';
-- DECLARE @sloc       VARCHAR(4)  = 'SP01';
-- DECLARE @eom        DATE        = EOMONTH(@period);




-- SELECT * FROM #TMP_1_ISS;
-- SELECT * FROM #TMP_1_REC  ORDER BY GR_DATE;
-- SELECT * FROM #TMP_ACCUM_CUR_MONTH ORDER BY GR_DATE;
-- SELECT * FROM #TMP_1_AGING  ORDER BY GR_DATE;;

-- SELECT COUNT(*) FROM #TMP_TRANS_AGING;
-- SELECT COUNT(*) FROM #TMP_1_AGING;

-- WITH A AS (
-- SELECT
--     MAT_NUM, PLANT_CODE, STO_LOC
--     , SUM(REMAINING_QTY)    AS CAL_QTY
--     , SUM(REMAINING_AMNT)   AS CAL_AMNT
-- FROM #TMP_TRANS_AGING
-- GROUP BY MAT_NUM, PLANT_CODE, STO_LOC
-- ),
-- B AS (
-- SELECT 
--     MATERIAL_NUMBER, PLANT_CODE, STORAGE_LOCATION
--     , SUM(QUANTITY) AS REF_QTY
--     , SUM(LOCAL_AMOUNT) AS REF_AMNT
-- FROM dbo.W_SAP_SPP_TRANSACTION_F
-- where CONVERT(VARCHAR, DATE_WID) <= @eom
-- GROUP BY MATERIAL_NUMBER, PLANT_CODE, STORAGE_LOCATION
-- )
--     SELECT A.*,
--         B.REF_QTY
--         , B.REF_AMNT
--         , CASE WHEN ISNULL(A.CAL_AMNT,0) = B.REF_AMNT 
--             AND ISNULL(A.CAL_QTY, 0) = B.REF_QTY
--             THEN 'CORRECT' ELSE 'INCORRECT'
--         END AS RESULT
--     INTO  #TMP_TEST
--     FROM B, A
--     WHERE 1=1
--         AND B.MATERIAL_NUMBER = A.MAT_NUM
--         AND B.PLANT_CODE = A.PLANT_CODE
--         AND B.STORAGE_LOCATION = A.STO_LOC
-- ;

-- SELECT * FROM #TMP_TEST WHERE RESULT = 'INCORRECT';

-- select
--     sum(REMAINING_AMNT) AS TOTAL_AMNT,
--     sum(REMAINING_QTY) AS TOTAL_QTY
-- FROM #TMP_TRANS_AGING


-- SELECT TOP 10 * FROM #TMP_REMAINING;

-- SELECT COUNT(*) FROM #TMP_1_ISS;
-- SELECT COUNT(*) FROM #TMP_REMAINING;

-- SELECT * FROM #TMP_1_ISS
-- WHERE 1=1
--     AND MAT_NUM = @mat_num
--     AND PLANT_CODE = @plant_code
--     AND STO_LOC = @sloc;

-- SELECT * FROM #TMP_1_REC 
-- WHERE 1=1
--     AND MAT_NUM = @mat_num
--     AND PLANT_CODE = @plant_code
--     AND STO_LOC = @sloc
-- ORDER BY GR_DATE;

-- SELECT * 
-- FROM #TMP_ACCUM_CUR_MONTH 
-- WHERE 1=1
--     AND MAT_NUM = @mat_num
--     AND PLANT_CODE = @plant_code
--     AND STO_LOC = @sloc
-- ORDER BY GR_DATE;

-- SELECT * 
-- FROM #TMP_1_AGING 
-- WHERE 1=1
--     AND MAT_NUM = @mat_num
--     AND PLANT_CODE = @plant_code
--     AND STO_LOC = @sloc
-- ORDER BY GR_DATE;

-- SELECT * 
-- FROM #TMP_REMAINING
-- WHERE 1=1
--     AND MAT_NUM = @mat_num
--     AND PLANT_CODE = @plant_code
--     AND STO_LOC = @sloc;




-- SELECT
--     SUM(ISS_AMOUNT) AS TOT_AMNT
--     , SUM(ISS_QTY) AS TOT_QTY
-- FROM #TMP_1_ISS;
-- SELECT
--     SUM(REC_AMNT) AS TOT_AMNT
--     , SUM(REC_QTY) AS TOT_QTY
-- FROM #TMP_1_REC;



-- IF OBJECT_ID(N'tempdb..#TMP_XXX_TRANS') IS NOT NULL 
-- BEGIN
--     PRINT N'DELETE temporary table #TMP_XXX_TRANS'
--     DROP Table #TMP_XXX_TRANS
-- END;


-- SELECT
--     PLANT_CODE
--     , MATERIAL_NUMBER
--     , STORAGE_LOCATION
--     , SUM(LOCAL_AMOUNT) AS SUM_LOCAL_AMNT
--     , SUM(QUANTITY)     AS SUM_QTY
--     , NULL              AS J
-- INTO #TMP_XXX_TRANS
-- FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
-- WHERE 1=1
--     and CONVERT(VARCHAR, DATE_WID) <= '20220531'
-- GROUP BY MATERIAL_NUMBER, PLANT_CODE, STORAGE_LOCATION;


-- UPDATE
--     #TMP_XXX_TRANS
-- SET
--     #TMP_XXX_TRANS.J = 123456789
-- FROM 
--     #TMP_XXX_TRANS c
--     INNER JOIN #TMP_REMAINING t
--         ON c.MATERIAL_NUMBER = t.MAT_NUM
--         AND c.PLANT_CODE = t.PLANT_CODE
--         AND c.STORAGE_LOCATION = t.STO_LOC
-- ;

-- SELECT TOP 10 * FROM #TMP_XXX_TRANS
-- WHERE J IS NULL;

-- FROM #TMP_REMAINING R
--     JOIN XXX ON 1=1
--         AND XXX.PLANT_CODE = R.PLANT_CODE
--         AND XXX.MATERIAL_NUMBER = R.MAT_NUM
--         AND XXX.STORAGE_LOCATION = R.STO_LOC
-- SELECT FROM #TMP_XXX_TRANS
--     LEFT JOIN XXX ON 1=1
--         AND XXX.PLANT_CODE = R.PLANT_CODE
--         AND XXX.MATERIAL_NUMBER = R.MAT_NUM
--         AND XXX.STORAGE_LOCATION = R.STO_LOC


-- SELECT TOP 10 * FROM #TMP_XXX_TRANS;
-- SELECT SUM(SUM_LOCAL_AMNT), SUM(SUM_QTY) FROM #TMP_XXX_TRANS;

-- SELECT COUNT(*) FROM #TMP_REMAINING;
-- SELECT SUM(RMN_AMNT), SUM(RMN_QTY) FROM #TMP_REMAINING;


-- WITH XXX AS (
--     SELECT
--         PLANT_CODE
--         , MATERIAL_NUMBER
--         , STORAGE_LOCATION
--         , SUM(LOCAL_AMOUNT) AS SUM_LOCAL_AMNT
--         , SUM(QUANTITY)     AS SUM_QTY
--     FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
--     WHERE 1=1
--         and CONVERT(VARCHAR, DATE_WID) <= @eom
--     GROUP BY MATERIAL_NUMBER, PLANT_CODE, STORAGE_LOCATION
-- )
--     SELECT
--         R.MAT_NUM
--         , R.PLANT_CODE
--         , R.STO_LOC 
--         , RMN_AMNT              AS CAL_AMNT
--         , RMN_QTY               AS CAL_QTY
--         , XXX.SUM_LOCAL_AMNT    AS REF_AMNT
--         , XXX.SUM_QTY           AS REF_QTY

--     FROM #TMP_REMAINING R
--     JOIN XXX ON 1=1
--         AND XXX.PLANT_CODE = R.PLANT_CODE
--         AND XXX.MATERIAL_NUMBER = R.MAT_NUM
--         AND XXX.STORAGE_LOCATION = R.STO_LOC
--     WHERE 1=1
--         AND (
--             ISNULL(RMN_AMNT, 0) <> XXX.SUM_LOCAL_AMNT
--             OR ISNULL(RMN_QTY, 0) <> XXX.SUM_QTY
--         )
-- ;


-- select count(*) from #TMP_REMAINING;
-- select count(*) from (
--     SELECT
--         PLANT_CODE
--         , MATERIAL_NUMBER
--         , STORAGE_LOCATION
--         , SUM(LOCAL_AMOUNT) AS SUM_LOCAL_AMNT
--         , SUM(QUANTITY)     AS SUM_QTY
--     FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
--     WHERE 1=1
--         and CONVERT(VARCHAR, DATE_WID) <= '20220531'
--     GROUP BY MATERIAL_NUMBER, PLANT_CODE, STORAGE_LOCATION
-- ) X
-- ;


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
    , TMP.STO_LOC
    , [< 4 MONTHS]
    , [4 - 12 MONTHS]
    , [1 - 2 YEARS]
    , [2 - 3 YEARS]
    , [3 - 4 YEARS]
    , [4 - 5 YEARS]
    , [> 5 YEARS]
    , [AGE_NULL]
    , CONCAT_WS('-', TMP.MAT_NUM, TMP.PLANT_CODE, TMP.STO_LOC) AS [KEY]
INTO #TMP_PIVOT
FROM (
    SELECT MAT_NUM, PLANT_CODE, AGING_GROUP, STO_LOC
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
        , [> 5 YEARS]
        , [AGE_NULL]
    )
) AS TMP;



-- DROP TABLE #TMP_TEST;
-- SELECT
--     MAT_NUM
--     , PLANT_CODE
--     , STO_LOC
--     , SUM(REMAINING_AMNT) AS TOT_AMNT
--     , SUM(REMAINING_QTY) AS TOT_QTY
--     , CONCAT_WS('-', MAT_NUM, PLANT_CODE, STO_LOC) AS [KEY]
-- INTO #TMP_TEST
-- FROM #TMP_TRANS_AGING
-- GROUP BY MAT_NUM, PLANT_CODE, STO_LOC;



-- SELECT *
-- FROM  #TMP_TEST T
-- WHERE 1=1
--     AND NOT EXISTS (
--         SELECT MAT_NUM, PLANT_CODE, STO_LOC FROM #TMP_PIVOT P
--         WHERE 1=1
--             AND T.[KEY] = T.[KEY]
--     );

-- DROP TABLE #TMP_TEST2;
-- SELECT
--     P.MAT_NUM
--     , P.STO_LOC
--     , P.PLANT_CODE
--     , TOT_QTY
--     , TOT_AMNT
-- INTO #TMP_TEST2
-- FROM #TMP_PIVOT P 
-- LEFT JOIN #TMP_TEST T ON 1=1
--     AND T.[KEY] = P.[KEY]
-- ;


PRINT '--> 3. Create balance_month table';

WITH TMP_B AS(
    SELECT
        PLANT_CODE
        , MAT_NUM
        , STO_LOC
        , [< 4 MONTHS]
        , [4 - 12 MONTHS]
        , [1 - 2 YEARS]
        , [2 - 3 YEARS]
        , [3 - 4 YEARS]
        , [4 - 5 YEARS]
        , [> 5 YEARS]
        , [AGE_NULL]
        , [4 - 12 MONTHS] + [1 - 2 YEARS]
        + [2 - 3 YEARS] + [3 - 4 YEARS] + [4 - 5 YEARS]     AS TOTAL_LEFTOVER
        , [3 - 4 YEARS] + [4 - 5 YEARS]                     AS SLOW_MOVING
        , [KEY]
    FROM #TMP_PIVOT
),
TMP_R AS (
    SELECT
        SUM(REMAINING_AMNT) AS TOT_AMNT
        , SUM(REMAINING_QTY) AS TOT_QTY
        , CONCAT_WS('-', MAT_NUM, PLANT_CODE, STO_LOC) AS [KEY]
    FROM #TMP_TRANS_AGING
    GROUP BY MAT_NUM, PLANT_CODE, STO_LOC
)
    SELECT
        CONVERT(VARCHAR, FORMAT(@eom, 'yyyyMMdd'))          AS DATE_WID
        , PL.PLANT_WID                                      AS PLANT_WID
        , IT.ITEM_WID                                       AS ITEM_WID

        , @eom                                              AS [PERIOD]
        , TMP_B.PLANT_CODE
        , PL.PLANT_NAME_2									AS PLANT
        , TMP_B.MAT_NUM
        , TMP_R.TOT_QTY                                         AS QUANTITY
        , TMP_R.TOT_AMNT                                        AS AMOUNT
        , NULL												AS [TYPE]
        , [< 4 MONTHS]
        , [4 - 12 MONTHS]
        , [1 - 2 YEARS]
        , [2 - 3 YEARS]
        , [3 - 4 YEARS]
        , [4 - 5 YEARS]
        , [> 5 YEARS]
        , [AGE_NULL]
        , TMP_B.STO_LOC
        , T.COMPANY_CODE
        , T.VALUATION_AREA
        , T.VALUATION_CLASS
        , T.MATERIAL_TYPE
        , T.MATERIAL_GROUP
        , T.BASE_UNIT_OF_MEASURE
        , T.CURRENCY
        , T.PRICE_CONTROL
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
            , T.STORAGE_LOCATION
        )                                                   AS W_INTEGRATION_ID
        , 'N'                                               AS W_DELETE_FLG
        , 1                                                 AS W_DATASOURCE_NUM_ID
        , GETDATE()                                         AS W_INSERT_DT
        , GETDATE()                                         AS W_UPDATE_DT
        , @p_batch_id                                       AS W_BATCH_ID
        , 'N'                                               AS W_UPDATE_FLG
        INTO #TMP_BALANCE_FINAL
        FROM TMP_B
            LEFT JOIN [dbo].[W_CMMS_ITEM_D] IT ON 1=1
                AND TMP_B.MAT_NUM = IT.ITEM_NUM
          
            LEFT JOIN TMP_R ON 1=1
                AND TMP_R.[KEY] = TMP_B.[KEY]
            LEFT JOIN [FND].[W_CMMS_MINMAX_D] MM ON 1=1
                AND MM.ITEM_NUM = TMP_B.MAT_NUM
                AND MM.PLANT = TMP_B.PLANT_CODE
            LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL ON 1=1
                AND PL.PLANT = TMP_B.PLANT_CODE
                AND PL.STO_LOC = TMP_B.STO_LOC
            OUTER APPLY ( 
                SELECT top 1
                    PLANT_CODE, MATERIAL_NUMBER,STORAGE_LOCATION, QUANTITY, LOCAL_AMOUNT,
                    PURCHASING_GROUP, COMPANY_CODE, VALUATION_AREA, VALUATION_CLASS, MATERIAL_TYPE,
                    MATERIAL_GROUP, BASE_UNIT_OF_MEASURE, CURRENCY, PRICE_CONTROL
                FROM [dbo].[W_SAP_SPP_TRANSACTION_F] TRANS
                WHERE 1=1
                        AND CONVERT(DATE, CONVERT(VARCHAR, TRANS.DATE_WID)) <= @eom
                        AND TRANS.PLANT_CODE = TMP_B.PLANT_CODE
                        AND TRANS.MATERIAL_NUMBER = TMP_B.MAT_NUM
                        AND TRANS.STORAGE_LOCATION = TMP_B.STO_LOC
                        AND TRANS.MOVEMENT_TYPE IN (SELECT MOVEMENT_TYPE FROM #TMP_MVM_TYPE)
                        -- AND (
                        --     TRANS.MOVEMENT_TYPE NOT IN ('201', '202', '901', '902') 
                        --     OR TRANS.MOVEMENT_TYPE IS NULL
                        -- )
                ORDER BY DATE_WID DESC
            ) T
;


-- Testing section
DECLARE @period     DATE        = '20220430';
DECLARE @eom        DATE        = EOMONTH(@period);

-- Test 0: data
-- select top 10 * from #TMP_BALANCE_FINAL;

-- Test 1: Total AMNT/QTY
-- SELECT
--     SUM(AMOUNT)                 AS TOTAL_AMNT
--     , SUM(QUANTITY)             AS TOTAL_QTY
-- FROM #TMP_BALANCE_FINAL;

-- SELECT
--     SUM(LOCAL_AMOUNT)         AS TOTAL_AMNT
--     , SUM(QUANTITY)            AS TOTAL_QTY
-- FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
-- WHERE CONVERT(DATE, CONVERT(VARCHAR, DATE_WID)) <= @eom;


-- Test 2: By Plant AMNT/QTY
-- WITH A AS (
--     SELECT
--         CONCAT_WS('-', MAT_NUM, PLANT_CODE)         AS [KEY]
--         , SUM(AMOUNT)                 AS TOTAL_AMNT
--         , SUM(QUANTITY)             AS TOTAL_QTY
--     FROM #TMP_BALANCE_FINAL
--     GROUP BY MAT_NUM, PLANT_CODE
-- ), 
-- B AS (
--     SELECT
--         SUM(LOCAL_AMOUNT)         AS TOTAL_AMNT
--         , SUM(QUANTITY)            AS TOTAL_QTY
--         , CONCAT_WS('-', MATERIAL_NUMBER, PLANT_CODE) AS [KEY]
--     FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
--     WHERE CONVERT(DATE, CONVERT(VARCHAR, DATE_WID)) <= @eom
--     GROUP BY MATERIAL_NUMBER, PLANT_CODE
-- ),
-- C AS (
--     SELECT
--         A.TOTAL_AMNT    AS CAL_AMNT
--         , A.TOTAL_QTY    AS CAL_QTY
--         , B.TOTAL_AMNT    AS REF_AMNT
--         , B.TOTAL_QTY    AS REF_QTY
--         , CASE WHEN ISNULL(A.TOTAL_AMNT, 0) <> ISNULL(B.TOTAL_AMNT, 0)
--             OR ISNULL(A.TOTAL_QTY, 0) <> ISNULL(B.TOTAL_QTY, 0)
--             THEN 'INCORRECT'
--             ELSE 'CORRECT' END AS RESULT
--     FROM B, A WHERE A.[KEY] = B.[KEY]
-- )
--     SELECT * FROM C WHERE RESULT = 'INCORRECT';


-- Test 3: By SLoc AMNT/QTY
WITH A AS (
    SELECT
        CONCAT_WS('-', MAT_NUM, PLANT_CODE, STO_LOC)         AS [KEY]
        , SUM(AMOUNT)                 AS TOTAL_AMNT
        , SUM(QUANTITY)             AS TOTAL_QTY
    FROM #TMP_BALANCE_FINAL
    GROUP BY MAT_NUM, PLANT_CODE, STO_LOC
), 
B AS (
    SELECT
        SUM(LOCAL_AMOUNT)         AS TOTAL_AMNT
        , SUM(QUANTITY)            AS TOTAL_QTY
        , CONCAT_WS('-', MATERIAL_NUMBER, PLANT_CODE, STORAGE_LOCATION) AS [KEY]
    FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
    WHERE CONVERT(DATE, CONVERT(VARCHAR, DATE_WID)) <= @eom
    GROUP BY MATERIAL_NUMBER, PLANT_CODE, STORAGE_LOCATION
),
C AS (
    SELECT
        A.TOTAL_AMNT    AS CAL_AMNT
        , A.TOTAL_QTY    AS CAL_QTY
        , B.TOTAL_AMNT    AS REF_AMNT
        , B.TOTAL_QTY    AS REF_QTY
        , CASE WHEN ISNULL(A.TOTAL_AMNT, 0) <> ISNULL(B.TOTAL_AMNT, 0)
            OR ISNULL(A.TOTAL_QTY, 0) <> ISNULL(B.TOTAL_QTY, 0)
            THEN 'INCORRECT'
            ELSE 'CORRECT' END AS RESULT
    FROM B, A WHERE A.[KEY] = B.[KEY]
)
    SELECT * FROM C WHERE RESULT = 'INCORRECT';