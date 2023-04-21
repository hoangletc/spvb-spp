WITH ISSUE_S1 AS (
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
        AND F.STORELOC LIKE '%S1'
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
), 
ISSUE_R1 AS (
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
        , CASE WHEN WO.ISTASK =1
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
            AND S1.WO_NUM    = F.WO_NUM
            AND S1.ASSET_NUM = F.ASSET_NUM
            AND S1.TO_SITEID = F.TO_SITEID
        LEFT JOIN FND.W_CMMS_WO_F W ON 1=1
            AND W.WONUM = F.WO_NUM
            AND W.SITE_ID = F.TO_SITEID
;





WITH RETURN_REMARK AS (
    SELECT DISTINCT
        ITEM_NUM
        , REFWO
        , ACTUALDATE
        , SPVB_REASON
    FROM dbo.W_CMMS_INVUL_D INVUL
    WHERE 1=1
        AND INVUL.[FROM] = 'MATU'
)
, WO_PARENT AS (
    SELECT
        WO_NUM      AS WONUM
        , I.WO_NUM  AS ISSUE_WO
    FROM FND.W_CMMS_WO_F W
    JOIN #TMP_ISSUE I ON 1=1
        AND I.WO_NUM = W.PARENT
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
        , RET_S1.RETURN_QTY_S1
        , RET_R1.RETURN_QTY_R1
        , RET_D1.RETURN_QTY_D1
        , RET_TOTAL.RETURN_QTY_TOTAL

        , (-1) * (F.ISSUE_QTY_R1 + F.ISSUE_QTY_S1) 
        - (RET_S1.RETURN_QTY_S1 + RET_R1.RETURN_QTY_R1 
            + RET_D1.RETURN_QTY_D1)                                     AS PENDING_QTY
        , WO.DATE_CLOSED                                                AS CLOSED_WO_DATE
        INTO #TMP_SPP_RETURN
        
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

            OUTER APPLY (
                SELECT
                    MAX(M.ACTUALDATE)                                   AS RETURN_DATE
                FROM dbo.W_CMMS_INVUL_D I
                    LEFT JOIN FND.W_CMMS_MATU_F M ON 1=1
                        AND M.INVUSELINE_ID = I.INVUSELINE_ID
                WHERE (
                    EXISTS (SELECT WONUM FROM WO_PARENT W WHERE W.ISSUE_WO = F.WO_NUM AND W.WONUM = I.SPVB_WONUMREF)
                    OR I.SPVB_WONUMREF = F.WO_NUM
                    OR (M.REFWO = F.WO_NUM AND I.USE_TYPE = 'RETURN')
                    OR (
                        EXISTS (SELECT WONUM FROM WO_PARENT W WHERE W.WONUM = M.refwo AND W.ISSUE_WO = F.WO_NUM)
                        AND I.USE_TYPE = 'RETURN'
                    )
                )
                AND M.ITEM_NUM = F.ITEM_NUM
                AND M.TO_SITEID  = F.TO_SITEID
            ) AS RET_DATE
            OUTER APPLY (
                SELECT
                    ISNULL(SUM(M.QUANTITY),0)                           AS RETURN_QTY_S1
            FROM dbo.W_CMMS_INVUL_D I
                LEFT JOIN FND.W_CMMS_MATU_F M ON 1=1
                    AND M.INVUSELINE_ID = I.INVUSELINE_ID
            WHERE 1=1
                AND [FROM] = 'MATU'
                AND (
                    I.SPVB_WONUMREF IN (SELECT WONUM FROM FND.W_CMMS_WO_F WHERE PARENT = F.WO_NUM)
                    OR M.refwo IN (SELECT WONUM FROM FND.W_CMMS_WO_F WHERE PARENT = F.WO_NUM)
                    OR I.SPVB_WONUMREF = F.WO_NUM
                    OR M.REFWO = F.WO_NUM
                )
                AND M.ITEM_NUM = F.ITEM_NUM
                AND M.TO_SITEID = F.TO_SITEID
                AND M.STORELOC LIKE '%S1'
                AND M.STORELOC <>'3S1.S1'
                AND M.ISSUE_TYPE ='RETURN'
            ) AS RET_S1
            OUTER APPLY (
                SELECT 
                    ISNULL(SUM(M.QUANTITY),0)                   AS RETURN_QTY_R1
            FROM dbo.W_CMMS_INVUL_D I
                LEFT JOIN FND.W_CMMS_MATU_F M ON 1=1
                    AND M.INVUSELINE_ID = I.INVUSELINE_ID
            WHERE 1=1
                AND [FROM] = 'MATU'
                AND (
                    I.SPVB_WONUMREF IN (SELECT WONUM FROM FND.W_CMMS_WO_F WHERE PARENT = F.WO_NUM)
                    OR I.SPVB_WONUMREF = F.WO_NUM
                    OR (M.REFWO = F.WO_NUM AND I.USE_TYPE = 'RETURN')
                    OR (
                        M.refwo IN (SELECT WONUM FROM FND.W_CMMS_WO_F WHERE PARENT = F.WO_NUM)
                        AND I.USE_TYPE = 'RETURN'
                    )
                )
                AND M.ITEM_NUM = F.ITEM_NUM
                AND M.TO_SITEID = F.TO_SITEID
                AND M.STORELOC LIKE '%R1'
            ) AS RET_R1
            OUTER APPLY (
                SELECT 
                    ISNULL(SUM(M.QUANTITY),0)                   AS RETURN_QTY_D1
                FROM dbo.W_CMMS_INVUL_D I
                    LEFT JOIN FND.W_CMMS_MATU_F M ON 1=1
                        AND M.INVUSELINE_ID = I.INVUSELINE_ID
                WHERE 1=1
                    AND [FROM] = 'MATU'
                    AND I.SPVB_WONUMREF = F.WO_NUM
                    AND (I.REFWO IS NULL OR I.REFWO = '')
                    AND M.ITEM_NUM = F.ITEM_NUM
                    AND M.TO_SITEID = F.TO_SITEID
                    AND M.STORELOC LIKE '%D1'
                    AND M.ISSUE_TYPE = 'RETURN'
            ) AS RET_D1
            OUTER APPLY (
                SELECT 
                    ISNULL(SUM(M.QUANTITY),0)                   AS RETURN_QTY_TOTAL
            FROM W_CMMS_INVUL_D I
                LEFT JOIN FND.W_CMMS_MATU_F M
                    ON M.INVUSELINE_ID = I.INVUSELINE_ID
            WHERE 1=1
                AND I.[FROM] = 'MATU'
                AND (I.SPVB_WONUMREF = F.WO_NUM OR M.REFWO = F.WO_NUM)
                AND M.ITEM_NUM = F.ITEM_NUM
                AND M.ISSUE_TYPE = 'RETURN'
                AND M.TO_SITEID = F.TO_SITEID
                AND M.STORELOC NOT IN ('3S1.S1','7S0.H1','6S0.H1','4S0.H1')
            ) AS RET_TOTAL

        -- WHERE 1=1
        --     AND F.TO_SITEID = 160
        --     AND F.ITEM_NUM = '61025778'
;

------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
WITH INVU_TMP AS (
    SELECT
        M.REFWO
        , M.ITEM_NUM
        , M.TO_SITEID
        , M.ACTUALDATE
        , I.SPVB_WONUMREF
        , I.USE_TYPE

    FROM dbo.W_CMMS_INVUL_D I
    LEFT JOIN FND.W_CMMS_MATU_F M ON 1=1
        AND M.INVUSELINE_ID = I.INVUSELINE_ID

)
, RET_DATE AS (
    SELECT
        F.ITEM_NUM
        , F.WO_NUM
        , F.TO_SITEID
        , MAX(I.ACTUALDATE) AS RETURN_DATE
    FROM #TMP_ISSUE F
        LEFT JOIN FND.W_CMMS_WO_F WOOO1 ON 1=1
            AND WOOO1.PARENT = F.WO_NUM
        LEFT JOIN FND.W_CMMS_WO_F WOOO2 ON 1=1
            AND WOOO2.PARENT = F.WO_NUM
        left join INVU_TMP I ON 1=1
            AND (
                WOOO2.WONUM IS NOT NULL AND WOOO2.WONUM = I.SPVB_WONUMREF
                OR I.SPVB_WONUMREF = F.WO_NUM
                OR (I.REFWO = F.WO_NUM AND I.USE_TYPE = 'RETURN')
                OR (
                    WOOO1.WONUM IS NOT NULL 
                    AND I.USE_TYPE = 'RETURN'
                    AND WOOO1.WONUM = I.REFWO
                )
            )
            AND I.ITEM_NUM = F.ITEM_NUM
            AND I.TO_SITEID  = F.TO_SITEID
    GROUP BY
        F.ITEM_NUM
        , F.WO_NUM
        , F.TO_SITEID
)
    SELECT
        F.ITEM_NUM
        , F.WO_NUM                                                      AS WO_NUMBER
        , F.TO_SITEID                                                   AS SITE_ID

        , F.ISSUE_DATE
        , F.ISSUE_QTY_R1
        , F.ISSUE_QTY_S1
        , F.ISSUE_QTY_TOTAL

        , RD.RETURN_DATE       AS RETURN_DATE
    
        FROM #TMP_ISSUE F
            LEFT JOIN RET_DATE RD ON 1=1
                AND F.ITEM_NUM = RD.ITEM_NUM
                AND F.WO_NUM = RD.WO_NUM
                AND F.TO_SITEID = RD.TO_SITEID
        WHERE 1=1
            AND F.TO_SITEID = 160
            -- AND F.ITEM_NUM = '61025778'
;    
    





SELECT DISTINCT
    ITEM_NUM
    , REFWO
    , ACTUALDATE
    , SPVB_REASON
FROM dbo.W_CMMS_INVUL_D
WHERE 1=1
    and [FROM] = 'MATU'
    AND ITEM_NUM = '61025508'
    AND REFWO = 'WO6000003356'
    AND ACTUALDATE = CONVERT(DATETIME2, '2019-12-16 18:00:00.0000000')
    
GROUP BY 

SELECT
    *
FROM dbo.W_CMMS_INVUL_D
WHERE 1=1
    and [FROM] = 'MATU'
    AND ITEM_NUM = '61025508'
    AND REFWO = 'WO6000003356'
    AND ACTUALDATE = CONVERT(DATETIME2, '2019-12-16 18:00:00.0000000')
;