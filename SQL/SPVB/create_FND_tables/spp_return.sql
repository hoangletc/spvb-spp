WITH 
ISSUE_S1 AS (
    SELECT
          F.ASSET_NUM
        , F.TO_SITEID
        , F.ITEM_NUM
        , CASE WHEN WO.ISTASK = 1 THEN WO.PARENT
            ELSE WO.WONUM
        END                                     AS WO_NUM
        , ISNULL(SUM(F.QUANTITY), 0)            AS QUANTITY
    FROM FND.W_CMMS_MATU_F F
        LEFT JOIN dbo.W_CMMS_INVUL_D I ON 1=1
            AND I.[FROM] = 'MATU'
            AND I.INVUSELINE_ID = F.INVUSELINE_ID
    
        LEFT JOIN FND.W_CMMS_WO_F WO ON 1=1
            AND F.REFWO = WO.WONUM
            AND F.TO_SITEID = WO.SITE_ID
    WHERE 1=1
        AND F.REFWO IS NOT NULL
        AND I.SPVB_MUSTRETURN = 1
        AND F.ISSUE_TYPE ='ISSUE'
        AND F.STORELOC LIKE '%S1'
    GROUP BY 
        F.ISSUE_UNIT
        , F.ASSET_NUM
        , F.TO_SITEID
        , F.ITEM_NUM
        , F.STORELOC
        , CASE WHEN WO.ISTASK = 1 THEN WO.PARENT ELSE WO.WONUM
        END
    HAVING SUM(F.quantity) <> 0
)
, ISSUE_R1 AS (
    SELECT
          F.ASSET_NUM
        , F.TO_SITEID
        , F.ITEM_NUM
        , CASE WHEN WO.ISTASK =1
                THEN WO.PARENT
            ELSE WO.WONUM
        END                                     AS WO_NUM
        , ISNULL(SUM(F.QUANTITY), 0)            AS QUANTITY
    FROM FND.W_CMMS_MATU_F F
        LEFT JOIN dbo.W_CMMS_INVUL_D I ON 1=1
            AND I.[FROM] = 'MATU'
            AND I.INVUSELINE_ID = F.INVUSELINE_ID
    
        LEFT JOIN FND.W_CMMS_WO_F WO ON 1=1
            AND F.REFWO = WO.WONUM
            AND F.TO_SITEID = WO.SITE_ID
    WHERE 1=1
        AND F.REFWO IS NOT NULL
        AND I.SPVB_MUSTRETURN = 1
        AND F.ISSUE_TYPE ='ISSUE'
        AND F.STORELOC LIKE '%R1'
    GROUP BY 
        F.ISSUE_UNIT
        , F.ASSET_NUM
        , F.TO_SITEID
        , F.ITEM_NUM
        , F.STORELOC
        , CASE
            WHEN WO.ISTASK =1
            THEN WO.PARENT
            ELSE WO.WONUM
        END
    HAVING SUM(F.quantity) <> 0
)
, ISSUE AS (
    SELECT
        F.ISSUE_UNIT
        , F.ASSET_NUM
        , F.TO_SITEID
        , F.ITEM_NUM
        , CASE WHEN WO.ISTASK = 1
                THEN WO.PARENT
            ELSE WO.WONUM
        END                                     AS WO_NUM
        , I.SPVB_MUSTRETURN
        , I.SPVB_MUSTRETURN_ORG
        , MAX(F.ACTUALDATE)                     AS ACTUAL_DATE
        , SUM(F.QUANTITY)                       AS QUANTITY
    FROM FND.W_CMMS_MATU_F F
        LEFT JOIN dbo.W_CMMS_INVUL_D I ON 1=1
            AND I.[FROM] = 'MATU'
            AND I.INVUSELINE_ID = F.INVUSELINE_ID
    
        LEFT JOIN FND.W_CMMS_WO_F WO ON 1=1
            AND F.REFWO = WO.WONUM
            AND F.TO_SITEID = WO.SITE_ID
    WHERE 1=1
        AND F.REFWO IS NOT NULL
        AND I.SPVB_MUSTRETURN = 1
        AND F.ISSUE_TYPE ='ISSUE'
    GROUP BY 
         F.ISSUE_UNIT
        , F.ASSET_NUM
        , F.TO_SITEID
        , F.ITEM_NUM
        , CASE
            WHEN WO.ISTASK =1
            THEN WO.PARENT
            ELSE WO.WONUM
        END
        , I.SPVB_MUSTRETURN
        , I.SPVB_MUSTRETURN_ORG
    HAVING SUM(F.quantity) <> 0
)
    SELECT
        -- ROW_NUMBER() OVER(ORDER BY F.ITEM_NUM) AS ROW_ID -- Xoá dòng này sau khi test xong
        F.ISSUE_UNIT
        , F.ASSET_NUM
        , F.TO_SITEID
        , F.ITEM_NUM
        , F.WO_NUM
        , CASE WHEN W.SPVB_OVERHAUL = 1
            THEN 'Y' ELSE 'N' END               AS OVERHAUL
        , F.SPVB_MUSTRETURN                     AS MUST_RETURN_INPUT
        , F.SPVB_MUSTRETURN_ORG                 AS MUST_RETURN_ORG

        , F.ACTUAL_DATE
        , F.QUANTITY

        , F.ACTUAL_DATE                         AS ISSUE_DATE
        , ISNULL(R1.QUANTITY, 0)                AS ISSUE_QTY_R1
        , ISNULL(S1.QUANTITY, 0)                AS ISSUE_QTY_S1
        , F.QUANTITY                            AS ISSUE_QTY_TOTAL

        , F.SPVB_MUSTRETURN
        , F.SPVB_MUSTRETURN_ORG
    INTO #TMP_ISSUE
    FROM ISSUE F
        LEFT JOIN ISSUE_R1 R1 ON 1=1
            AND R1.WO_NUM    = F.WO_NUM
            AND R1.ASSET_NUM = F.ASSET_NUM
            AND R1.TO_SITEID = F.TO_SITEID
        LEFT JOIN ISSUE_S1 S1 ON 1=1
            and S1.ITEM_NUM = F.ITEM_NUM
            AND S1.WO_NUM   = F.WO_NUM
            AND S1.ASSET_NUM = F.ASSET_NUM
            AND S1.TO_SITEID = F.TO_SITEID
        LEFT JOIN FND.W_CMMS_WO_F W ON 1=1
            AND W.WONUM = F.WO_NUM
            AND W.SITE_ID = F.TO_SITEID
    WHERE 1=1
        -- AND F.ITEM_NUM = '61484482'
;



SELECT * 
FROM FND.W_CMMS_MATU_F
WHERE 1=1
    AND ITEM_NUM = '61484482'
--     -- AND REFWO = 'WO6000120191'






WITH
RET_DATE AS (
    select 
        F.ITEM_NUM
        , F.TO_SITEID
        , F.WO_NUM
        , MAX(M.ACTUALDATE)                                   AS RETURN_DATE

    FROM #TMP_ISSUE F
    JOIN FND.W_CMMS_MATU_F M ON 1=1
        AND M.ITEM_NUM = F.ITEM_NUM
        AND M.TO_SITEID = F.TO_SITEID
        AND M.STORELOC LIKE '%S1'
        AND M.STORELOC <>'3S1.S1'
        AND M.ISSUE_TYPE ='RETURN'

    JOIN dbo.W_CMMS_INVUL_D I ON 1=1
        AND [FROM] = 'MATU'
        AND M.INVUSELINE_ID = I.INVUSELINE_ID
        AND (
            I.SPVB_WONUMREF IN (SELECT WONUM FROM FND.W_CMMS_WO_F WHERE PARENT = F.WO_NUM)
            OR I.SPVB_WONUMREF = F.WO_NUM
            OR (M.REFWO = F.WO_NUM AND I.USE_TYPE = 'RETURN')
            OR (
                M.refwo IN (SELECT WONUM FROM FND.W_CMMS_WO_F WHERE PARENT = F.WO_NUM)
                AND I.USE_TYPE = 'RETURN'
            )
        )
    GROUP BY
        F.ITEM_NUM
        , F.TO_SITEID
        , F.WO_NUM
)
, RET_S1 AS (
    select 
        F.ITEM_NUM
        , F.TO_SITEID
        , F.WO_NUM
        , ISNULL(SUM(M.QUANTITY), 0) AS RETURN_QTY_S1

    FROM #TMP_ISSUE F
    JOIN FND.W_CMMS_MATU_F M ON 1=1
        AND M.ITEM_NUM = F.ITEM_NUM
        AND M.TO_SITEID = F.TO_SITEID
        AND M.STORELOC LIKE '%S1'
        AND M.STORELOC <>'3S1.S1'
        AND M.ISSUE_TYPE ='RETURN'

    JOIN dbo.W_CMMS_INVUL_D I ON 1=1
        AND [FROM] = 'MATU'
        AND M.INVUSELINE_ID = I.INVUSELINE_ID
        AND (
            I.SPVB_WONUMREF IN (SELECT WONUM FROM FND.W_CMMS_WO_F WHERE PARENT = F.WO_NUM)
            OR M.refwo IN (SELECT WONUM FROM FND.W_CMMS_WO_F WHERE PARENT = F.WO_NUM)
            OR F.WO_NUM IN (I.SPVB_WONUMREF, M.REFWO)
        )
        WHERE 1=1
            AND M.ITEM_NUM = '61484482'
            AND M.TO_SITEID = '160'
    GROUP BY
        F.ITEM_NUM
        , F.TO_SITEID
        , F.WO_NUM
    
)
, RET_R1 AS (
    select 
        F.ITEM_NUM
        , F.TO_SITEID
        , F.WO_NUM
        , ISNULL(SUM(M.QUANTITY), 0) AS RETURN_QTY_R1

    FROM #TMP_ISSUE F
    JOIN FND.W_CMMS_MATU_F M ON 1=1
        AND M.ITEM_NUM = F.ITEM_NUM
        AND M.TO_SITEID = F.TO_SITEID
        AND M.STORELOC LIKE '%R1'
    JOIN dbo.W_CMMS_INVUL_D I ON 1=1
        AND [FROM] = 'MATU'
        AND M.INVUSELINE_ID = I.INVUSELINE_ID
        AND (
            I.SPVB_WONUMREF IN (SELECT WONUM FROM FND.W_CMMS_WO_F WHERE PARENT = F.WO_NUM)
            OR I.SPVB_WONUMREF = F.WO_NUM
            OR (M.REFWO = F.WO_NUM AND I.USE_TYPE = 'RETURN')
            OR (
                M.refwo IN (SELECT WONUM FROM FND.W_CMMS_WO_F WHERE PARENT = F.WO_NUM)
                AND I.USE_TYPE = 'RETURN'
            )
        )
    GROUP BY
        F.ITEM_NUM
        , F.TO_SITEID
        , F.WO_NUM
)
, RET_D1 AS (
    select 
        F.ITEM_NUM
        , F.TO_SITEID
        , F.WO_NUM
        , ISNULL(SUM(M.QUANTITY), 0) AS RETURN_QTY_D1

    FROM #TMP_ISSUE F
    JOIN FND.W_CMMS_MATU_F M ON 1=1
        AND M.ITEM_NUM = F.ITEM_NUM
        AND M.TO_SITEID = F.TO_SITEID
        AND M.STORELOC LIKE '%D1'
        AND M.ISSUE_TYPE ='RETURN'
    JOIN dbo.W_CMMS_INVUL_D I ON 1=1
        AND [FROM] = 'MATU'
        AND M.INVUSELINE_ID = I.INVUSELINE_ID
        AND I.SPVB_WONUMREF = F.WO_NUM
        AND (I.REFWO IS NULL OR I.REFWO = '')
    GROUP BY
        F.ITEM_NUM
        , F.TO_SITEID
        , F.WO_NUM
)
, RET_TOTAL AS (
    select 
        F.ITEM_NUM
        , F.TO_SITEID
        , F.WO_NUM
        , ISNULL(SUM(M.QUANTITY), 0) AS RETURN_QTY_TOTAL

    FROM #TMP_ISSUE F
    LEFT JOIN FND.W_CMMS_MATU_F M ON 1=1
        AND M.ITEM_NUM = F.ITEM_NUM
        AND M.TO_SITEID = F.TO_SITEID
        AND M.STORELOC NOT IN ('3S1.S1','7S0.H1','6S0.H1','4S0.H1')
        AND M.ISSUE_TYPE ='RETURN'
    JOIN dbo.W_CMMS_INVUL_D I ON 1=1
        AND [FROM] = 'MATU'
        AND (I.SPVB_WONUMREF = F.WO_NUM OR M.REFWO = F.WO_NUM)
        AND M.INVUSELINE_ID = I.INVUSELINE_ID
        AND I.SPVB_WONUMREF = F.WO_NUM
    WHERE 1=1
        AND F.ITEM_NUM = '61484482'
        AND F.TO_SITEID = '160'
    GROUP BY
        F.ITEM_NUM
        , F.TO_SITEID
        , F.WO_NUM
)
, RETURN_REMARK AS (
    SELECT DISTINCT
        ITEM_NUM
        , REFWO
        , ACTUALDATE
        , SPVB_REASON
    FROM dbo.W_CMMS_INVUL_D INVUL
    WHERE 1=1
        AND INVUL.[FROM] = 'MATU'
)
    SELECT
        F.ITEM_NUM
        , I.DESCRIPTION
        , I.ISSUE_UNIT                                                  AS UNIT
        , A_LINE.DESCRIPTION                                            AS [LINE]
        , CONCAT(A.ASSET_NUM, '-', A.DESCRIPTION)                       AS MACHINE
        , F.WO_NUM                                                      AS WO_NUMBER
        , WO.OVERHAUL                                                   AS OVERHAUL
        , CASE WHEN F.SPVB_MUSTRETURN_ORG = 1 THEN 'Y' ELSE 'N' END     AS MUST_RETURN_ORG
        , CASE WHEN F.SPVB_MUSTRETURN = 1 THEN 'Y' ELSE 'N' END         AS MUST_RETURN
        , R.SPVB_REASON                                                 AS REMARK
        , F.TO_SITEID                                                   AS SITE_ID

        , F.ISSUE_DATE
        , F.ISSUE_QTY_R1
        , F.ISSUE_QTY_S1
        , F.ISSUE_QTY_TOTAL

        , RET_DATE.RETURN_DATE
        , ISNULL(RET_S1.RETURN_QTY_S1, 0)                               AS RETURN_QTY_S1
        , ISNULL(RET_R1.RETURN_QTY_R1, 0)                               AS RETURN_QTY_R1
        , ISNULL(RET_D1.RETURN_QTY_D1, 0)                               AS RETURN_QTY_D1
        , ISNULL(RET_TOTAL.RETURN_QTY_TOTAL, 0)                         AS RETURN_QTY_TOTAL

        , (-1) * (
            ISNULL(F.ISSUE_QTY_R1, 0)
            + ISNULL(F.ISSUE_QTY_S1, 0)
        ) 
        - (
            ISNULL(RET_S1.RETURN_QTY_S1, 0)
            + ISNULL(RET_R1.RETURN_QTY_R1, 0)
            + ISNULL(RET_D1.RETURN_QTY_D1, 0)
        )                                                               AS PENDING_QTY
        , WO.DATE_CLOSED                                                AS CLOSED_WO_DATE
    INTO #TMP_HOANGLE_SPP_RETURN
    FROM #TMP_ISSUE F
    LEFT JOIN dbo.W_CMMS_ITEM_D I ON 1=1
        AND I.ITEM_NUM = F.ITEM_NUM
    LEFT JOIN dbo.W_CMMS_ASSET_D A ON 1=1
        AND A.ASSET_NUM = F.ASSET_NUM
        AND A.SITE_ID = F.TO_SITEID
    LEFT JOIN dbo.W_CMMS_ASSET_D A_LINE ON 1=1
        AND A_LINE.ASSET_NUM = A.LINE_ASSET_NUM
        AND A_LINE.SITE_ID = A.SITE_ID
    LEFT JOIN dbo.W_CMMS_WO_F WO ON 1=1
        AND WO.WORK_ORDERS = F.WO_NUM
        AND WO.SITE = F.TO_SITEID
    LEFT JOIN RETURN_REMARK R ON 1=1
        AND R.ITEM_NUM = F.ITEM_NUM
        AND R.REFWO = F.WO_NUM
        AND R.ACTUALDATE = F.ACTUAL_DATE

    LEFT JOIN RET_S1 ON 1=1
        AND F.ITEM_NUM = RET_S1.ITEM_NUM
        AND F.WO_NUM = RET_S1.WO_NUM
        AND F.TO_SITEID = RET_S1.TO_SITEID
    LEFT JOIN RET_DATE ON 1=1
        AND F.ITEM_NUM = RET_DATE.ITEM_NUM
        AND F.WO_NUM = RET_DATE.WO_NUM
        AND F.TO_SITEID = RET_DATE.TO_SITEID
    LEFT JOIN RET_R1 ON 1=1
        AND F.ITEM_NUM = RET_R1.ITEM_NUM
        AND F.WO_NUM = RET_R1.WO_NUM
        AND F.TO_SITEID = RET_R1.TO_SITEID
    LEFT JOIN RET_D1 ON 1=1
        AND F.ITEM_NUM = RET_D1.ITEM_NUM
        AND F.WO_NUM = RET_D1.WO_NUM
        AND F.TO_SITEID = RET_D1.TO_SITEID
    LEFT JOIN RET_TOTAL ON 1=1
        AND F.ITEM_NUM = RET_TOTAL.ITEM_NUM
        AND F.WO_NUM = RET_TOTAL.WO_NUM
        AND F.TO_SITEID = RET_TOTAL.TO_SITEID

    WHERE 1=1
        AND F.TO_SITEID = 160
        -- AND F.ITEM_NUM = '61484482'
;





select * from #TMP_HOANGLE_SPP_RETURN
where 1=1
    and ITEM_NUM = '61484482';

select 
 from #TMP_HOANGLE_SPP_RETURN
select COUNT(*) from #TMP_ISSUE
WHERE 1=1
    AND TO_SITEID = 160

------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------

SELECT * FROM dbo.W_CMMS_INVUL_D
WHERE SPVB_WONUMREF = 'WO6000120191'
SELECT * FROM FND.W_CMMS_MATU_F
WHERE REFWO = 'WO6000120191'
SELECT * FROM dbo.W_CMMS_INVUL_D I
    JOIN FND.W_CMMS_MATU_F M ON 1=1
        AND M.INVUSELINE_ID = I.INVUSELINE_ID
        AND M.ITEM_NUM = '61484482'
        AND M.TO_SITEID = '160'
        AND M.ISSUE_TYPE ='RETURN'
        -- AND 'WO6000120191' IN (M.REFWO, I.SPVB_WONUMREF)
        
SELECT *
FROM FND.W_CMMS_MATU_F M
LEFT JOIN 
WHERE 1=1
    AND M.ITEM_NUM = '61484482'
    AND M.TO_SITEID = '160'
    AND M.ISSUE_TYPE ='RETURN'
    AND 'WO6000120191' IN (M.REFWO, I.SPVB_WONUMREF)
    AND M.STORELOC NOT IN ('3S1.S1','7S0.H1','6S0.H1','4S0.H1')


select

    (
        SELECT 
            ISNULL(SUM(M.QUANTITY),0)                   AS RETURN_QTY_TOTAL
        FROM W_CMMS_INVUL_D I
            LEFT JOIN FND.W_CMMS_MATU_F M
                ON M.INVUSELINE_ID = I.INVUSELINE_ID
        WHERE 1=1
            AND I.[FROM] = 'MATU'
            AND (
                I.SPVB_WONUMREF IN (SELECT WONUM FROM FND.W_CMMS_WO_F WHERE PARENT = F.WO_NUM)
                OR M.refwo IN (SELECT WONUM FROM FND.W_CMMS_WO_F WHERE PARENT = F.WO_NUM)
                OR F.WO_NUM IN (I.SPVB_WONUMREF, M.REFWO)
            )
            -- AND F.WO_NUM IN (M.REFWO, I.SPVB_WONUMREF)
            AND M.ITEM_NUM = F.ITEM_NUM
            AND M.ISSUE_TYPE = 'RETURN'
            AND M.TO_SITEID = F.TO_SITEID
            AND M.STORELOC NOT IN ('3S1.S1','7S0.H1','6S0.H1','4S0.H1')
    )
from #TMP_ISSUE F
WHERE 1=1
    and F.ITEM_NUM = '61484482'
    AND F.TO_SITEID = 160;