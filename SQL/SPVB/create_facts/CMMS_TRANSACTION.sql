DROP TABLE #TMP_CMMS_TRANS;

;WITH TMP_WO_X AS (
    SELECT
        WO.*
        , CASE WHEN WO.IS_SCHED = 0
            THEN WO.DATE_FINISHED_BFR
            ELSE WO.DATE_FINISHED_AFT
        END                                 	AS ACTUAL_FINISH
        , WO_STA.CHANGEDATE               		AS LAST_STATUS_DATE
        , AST.LINE_ASSET_NUM
    FROM [dbo].[W_CMMS_WO_F] AS WO
        LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_STA ON 1=1
            AND WO_STA.PARENT = WO.WORK_ORDERS
            AND WO_STA.STATUS = WO.[STATUS]
        LEFT JOIN [dbo].W_CMMS_ASSET_D AST ON 1=1
            AND WO.ASSET_NUM IS NOT NULL
            AND WO.ASSET_NUM <> ''
            AND WO.ASSET_NUM = AST.ASSET_NUM
            AND LEFT(WO.LOCATION, 3) = LEFT(AST.LOCATION, 3)
)
    SELECT * 
    INTO #TMP_CMMS_TRANS
    FROM (
    SELECT
        LOC.LOC_WID                             AS LOC_WID
        , TMP_WO_X.ASSET_WID                    AS ASSET_WID
        , INVU.INVU_WID                         AS INVU_WID
        , INVUL.INVUL_WID                       AS INVUL_WID
        , IT.ITEM_WID                           AS ITEM_WID
        , TMP_WO_X.WO_WID                       AS WO_WID
        , FORMAT(
            CONVERT(DATE, MATU.ACTUALDATE), 
            'yyyymmdd'
        )                                       AS DATE_WID
        , INVU.INVUSE_NUM                       AS USAGE
        , MATU.STORELOC                         AS WAREHOUSE
        , LOC.[DESCRIPTION]                     AS WAREHOUSE_NAME
        , CASE WHEN MATU.REFWO IS NULL
        AND MATU.ISSUE_TYPE = 'RETURN' 
            THEN 'RECEIPT MISC'
            ELSE MATU.ISSUE_TYPE 
        END                                     AS TRANSACTION_TYPE
        , MATU.TRANSDATE                        AS TRANSACTION_DATE
        , MATU.ACTUALDATE                       AS ACTUAL_DATE
        , MATU.ITEM_NUM                         AS ITEM_NO
        , IT.DESCRIPTION                        AS [DESCRIPTION]
        , MATU.QUANTITY                         AS TRANSACTION_QUANT
        , MATU.ISSUE_UNIT                       AS TRANSACTION_UOM
        , MATU.BINNUM                           AS BINNUM
        , TMP_WO_X.OVERHAUL                     AS OVERHAUL
        , CASE WHEN INVUL.SPVB_MUSTRETURN_ORG = 'True'
            THEN 'Y'
            ELSE 'N'
        END                                     AS MUST_RETURN_ORIGINAL
        , CASE WHEN INVUL.SPVB_MUSTRETURN = 'True'
            THEN 'Y'
            ELSE 'N'
        END                                     AS MUST_RETURN_USER_INPUT
        , INVUL.SPVB_REASON                     AS MUST_RETURN_REMARK
        , MATU.UNITCOST                         AS PRICE
        , MATU.LINECOST                         AS AMOUNT
        , MATU.MRNUM                            AS MRNUM
        , CASE WHEN TMP_WO_X.PARENT IS NULL 
            THEN TMP_WO_X.WORK_ORDERS
            ELSE NULL 
        END                                     AS WORK_ORDER
        , MATU.ASSET_NUM                        AS ASSET
        , TMP_WO_X.LINE_ASSET_NUM               AS [LINE]
        , TMP_WO_X.[TYPE]                       AS WORK_TYPE
        , TMP_WO_X.[STATUS]                     AS WORKORDER_STATUS
        , TMP_WO_X.ACTUAL_FINISH                AS ACTUAL_FINISH
        , TMP_WO_X.LAST_STATUS_DATE             AS WO_LAST_STATUSDATE
        , TMP_WO_X.SUPERVISOR                   AS WO_DONE_BY
        , INVUL.ENTER_BY                        AS USER_ID
        , INVUL.SPVB_EXTREASONCODE              AS REASON_CODE
        , INVU.DESCRIPTION                      AS JOURNAL_CMT_HEADER
        , INVUL.REMARK                          AS JOURNAL_CMT
        , MATU.PONUM                            AS PONUM
        , NULL                                  AS SAP_DND
        , INVUL.SPVB_WONUMREF                   AS RET_WONUM
    FROM FND.W_CMMS_MATU_F MATU
        LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
            AND LOC.[LOCATION] = MATU.STORELOC
        LEFT JOIN [dbo].[W_CMMS_ITEM_D] IT ON 1=1
            AND IT.ITEM_NUM = MATU.ITEM_NUM
        LEFT JOIN [dbo].[W_CMMS_ASSET_D] AST ON 1=1
            AND AST.ASSET_NUM = MATU.ASSET_NUM
            AND LEFT(AST.LOCATION, 3) = LEFT(MATU.LOCATION, 3)
        LEFT JOIN [TMP_WO_X] ON 1=1
            AND TMP_WO_X.WORK_ORDERS = MATU.REFWO
            AND TMP_WO_X.ASSET_NUM = MATU.ASSET_NUM
        LEFT JOIN [dbo].[W_CMMS_INVUL_D] INVUL ON 1=1
            AND MATU.INVUSELINE_ID IS NOT NULL
            AND MATU.INVUSELINE_ID <> ''
            AND INVUL.INVUSELINE_ID = MATU.INVUSELINE_ID
        OUTER APPLY (
            SELECT TOP 1 * FROM[dbo].[W_CMMS_INVU_D] TMP_INVU
            WHERE 1=1
                AND MATU.INVUSE_ID IS NOT NULL
                AND MATU.INVUSE_ID <> ''
                AND TMP_INVU.INVU_ID = MATU.INVUSE_ID
        ) INVU

    UNION ALL

    SELECT
        LOC.LOC_WID                             AS LOC_WID
        , TMP_WO_X.ASSET_WID                    AS ASSET_WID
        , INVU_WID                              AS INVU_WID
        , INVUL_WID                             AS INVUL_WID
        , ITEM_WID                              AS ITEM_WID
        , TMP_WO_X.WO_WID                       AS WO_WID
        , FORMAT(
            CONVERT(DATE, MATR.ACTUALDATE), 
            'yyyymmdd'
        )                                       AS DATE_WID
        , CASE WHEN MATR.ISSUETYPE = 'TRANSFER'
            THEN INVU.INVUSE_NUM       
            ELSE MATR.SPVB_SAPRECEIPT
        END                                     AS USAGE
        , CASE WHEN MATR.FROMSTORELOC = 'INSPECTION'
                THEN MATR.FROMSTORELOC
            WHEN MATR.TOSTORELOC = 'INSPECTION'
                THEN MATR.FROMSTORELOC
            WHEN MATR.FROMSTORELOC = MATR.TOSTORELOC
                THEN MATR.FROMSTORELOC
            ELSE 'From ' + MATR.FROMSTORELOC + ' To ' + MATR.TOSTORELOC
        END                                     AS WAREHOUSE
        , LOC.[DESCRIPTION]                     AS WAREHOUSE_NAME
        , CASE WHEN MATR.ISSUETYPE = 'TRANSFER' 
                AND MATR.FROMSTORELOC = 'INSPECTION'
                AND MATR.SHIPMENTNUM IS NOT NULL
            THEN 'SHIPRECEIPT' 
            WHEN MATR.ISSUETYPE = 'TRANSFER'
                AND MATR.FROMSTORELOC = 'INSPECTION'
                AND MATR.SHIPMENTNUM IS NULL
            THEN 'RECEIPT'
            ELSE MATR.ISSUETYPE
        END                                     AS TRANSACTION_TYPE
        , MATR.TRANSDATE                        AS TRANSACTION_DATE
        , MATR.ACTUALDATE                       AS ACTUAL_DATE
        , MATR.ITEMNUM                          AS ITEM_NO
        , IT.DESCRIPTION                        AS [DESCRIPTION]
        , MATR.QUANTITY                         AS TRANSACTION_QUANT
        , MATR.ISSUEUNIT                        AS TRANSACTION_UOM
        , MATR.BINNUM                           AS BINNUM
        , [TMP_WO_X].OVERHAUL                   AS OVERHAUL
        , CASE WHEN INVUL.SPVB_MUSTRETURN_ORG = 'True'
            THEN 'Y'
            ELSE 'N'
        END                                     AS MUST_RETURN_ORIGINAL
        , NULL                                  AS MUST_RETURN_USER_INPUT
        , INVUL.SPVB_REASON                     AS MUST_RETURN_REMARK
        , MATR.UNITCOST                         AS PRICE
        , MATR.LINECOST                         AS AMOUNT
        , MATR.MRNUM                            AS MRNUM
        , CASE WHEN TMP_WO_X.PARENT IS NULL 
            THEN TMP_WO_X.WORK_ORDERS
            ELSE NULL 
        END                                     AS WORK_ORDER
        , MATR.ASSETNUM                         AS ASSET
        , TMP_WO_X.LINE_ASSET_NUM               AS [LINE]
        , [TMP_WO_X].[TYPE]                     AS WORK_TYPE
        , [TMP_WO_X].[STATUS]                   AS WORKORDER_STATUS
        , [TMP_WO_X].ACTUAL_FINISH              AS ACTUAL_FINISH
        , [TMP_WO_X].LAST_STATUS_DATE           AS WO_LAST_STATUSDATE
        , [TMP_WO_X].SUPERVISOR                 AS WO_DONE_BY
        , INVUL.ENTER_BY                        AS USER_ID
        , INVUL.SPVB_EXTREASONCODE              AS REASON_CODE
        , INVU.DESCRIPTION                      AS JOURNAL_CMT_HEADER
        , MATR.SPVB_SAPREMARK                   AS JOURNAL_CMT
        , MATR.SPVB_SAPPO                       AS PONUM
        , MATR.SPVB_DND                         AS SAO_DND
        , INVUL.SPVB_WONUMREF                   AS RET_WONUM
    FROM FND.W_CMMS_MATR_F MATR
        OUTER APPLY (
            SELECT TOP 1 * FROM[dbo].[W_CMMS_INVU_D] TMP_INVU
            WHERE 1=1
                AND MATR.INVUSE_ID IS NOT NULL
                AND MATR.INVUSE_ID <> ''
                AND TMP_INVU.INVU_ID = MATR.INVUSE_ID
        ) INVU
        LEFT JOIN [dbo].[W_CMMS_INVUL_D] INVUL ON 1=1
            AND MATR.INVUSELINE_ID IS NOT NULL
            AND MATR.INVUSELINE_ID <> ''
            AND INVUL.INVUSELINE_ID = MATR.INVUSELINE_ID
        LEFT JOIN [dbo].[W_CMMS_ITEM_D] IT ON 1=1
            AND IT.ITEM_NUM = MATR.ITEMNUM
        LEFT JOIN [TMP_WO_X] ON 1=1
            AND TMP_WO_X.WORK_ORDERS = MATR.REFWO
        LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
            AND MATR.TOSTORELOC <> 'INSPECTION'
            AND LOC.[LOCATION] = MATR.TOSTORELOC

    UNION ALL

    SELECT
        LOC.LOC_WID                             AS LOC_WID
        , NULL                                  AS ASSET_WID
        , NULL                                  AS INVU_WID
        , NULL                                  AS INVUL_WID
        , IT.ITEM_WID                           AS ITEM_WID
        , NULL                                  AS WO_WID
        , FORMAT(
            CONVERT(DATE, INVT.ACTUALDATE), 
            'yyyymmdd'
        )                                       AS DATE_WID

        , INVT.EXTERNAL_REFID                   AS USAGE
        , INVT.STORELOC                         AS WAREHOUSE
        , LOC.[DESCRIPTION]                     AS WAREHOUSE_NAME
        , 'Physical Count'                      AS TRANSACTION_TYPE
        , INVT.TRANSDATE                        AS TRANSACTION_DATE
        , INVT.ACTUALDATE                       AS ACTUAL_DATE
        , INVT.ITEM_NUM                         AS ITEM_NO
        , IT.DESCRIPTION                        AS [DESCRIPTION]
        , INVT.QUANTITY                         AS TRANSACTION_QUANT
        , NULL                                  AS TRANSACTION_UOM
        , INVT.BIN_NUM                          AS BINNUM
        , NULL                                  AS OVERHAUL
        , NULL                                  AS MUST_RETURN_ORIGINAL
        , NULL                                  AS MUST_RETURN_USER_INPUT
        , NULL                                  AS MUST_RETURN_REMARK
        , 0                                     AS PRICE
        , 0                                     AS AMOUNT
        , NULL                                  AS MRNUM
        , NULL                                  AS WORK_ORDER
        , NULL                                  AS ASSET
        , NULL                                  AS [LINE]
        , NULL                                  AS WORK_TYPE
        , NULL                                  AS WORKORDER_STATUS
        , NULL                                  AS ACTUAL_FINISH
        , NULL                                  AS WO_LAST_STATUSDATE
        , NULL                                  AS WO_DONE_BY
        , NULL                                  AS USER_ID
        , NULL                                  AS REASON_CODE
        , NULL                                  AS JOURNAL_CMT_HEADER
        , NULL                                  AS JOURNAL_CMT
        , NULL                                  AS PONUM
        , NULL                                  AS SAO_DND
        , NULL                                  AS RET_WONUM
    FROM [FND].[W_CMMS_INVT_F] INVT
        LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
            AND LOC.[LOCATION] = INVT.STORELOC
        LEFT JOIN [dbo].[W_CMMS_ITEM_D] IT ON 1=1
            AND IT.ITEM_NUM = INVT.ITEM_NUM

    UNION ALL

    SELECT
        NULL                                    AS LOC_WID
        , NULL                                  AS ASSET_WID
        , NULL                                  AS INVU_WID
        , NULL                                  AS INVUL_WID
        , NULL                                  AS ITEM_WID
        , NULL                                  AS WO_WID
        , FORMAT(
            CONVERT(DATE, SERV.ACTUALDATE), 
            'yyyymmdd'
        )                                       AS DATE_WID

        , SERV.SPVB_SAPRECEIPT                  AS USAGE
        , NULL                                  AS WAREHOUSE
        , NULL                                  AS WAREHOUSE_NAME
        , CASE WHEN SERV.ISSUE_TYPE = 'RETURN'
            THEN 'SERVICE RETURN'
            ELSE 'SERVICE RECEIPT'
        END                                     AS TRANSACTION_TYPE
        , SERV.TRANSDATE                        AS TRANSACTION_DATE
        , SERV.ACTUALDATE                       AS ACTUAL_DATE
        , SERV.ITEM_NUM                         AS ITEM_NO
        , SERV.[DESCRIPTION]                    AS [DESCRIPTION]
        , SERV.QUANTITY                         AS TRANSACTION_QUANT
        , NULL                                  AS TRANSACTION_UOM
        , NULL                                  AS BINNUM
        , CASE WHEN TMP_WO_X.OVERHAUL = 'Y'
            THEN 'Y'
            ELSE 'N'
        END                                     AS OVERHAUL
        , NULL                                  AS MUST_RETURN_ORIGINAL
        , NULL                                  AS MUST_RETURN_USER_INPUT
        , NULL                                  AS MUST_RETURN_REMARK
        , SERV.UNITCOST                         AS PRICE
        , SERV.LINECOST                         AS AMOUNT
        , NULL                                  AS MRNUM
        , NULL                                  AS WORK_ORDER
        , SERV.ASSET_NUM                        AS ASSET
        , [TMP_WO_X].LINE_ASSET_NUM             AS [LINE]
        , TMP_WO_X.[TYPE]                       AS WORK_TYPE
        , TMP_WO_X.[STATUS]                     AS WORKORDER_STATUS
        , TMP_WO_X.ACTUAL_FINISH                AS ACTUAL_FINISH
        , TMP_WO_X.LAST_STATUS_DATE             AS WO_LAST_STATUSDATE
        , TMP_WO_X.SUPERVISOR                   AS WO_DONE_BY
        , NULL                                  AS USER_ID
        , NULL                                  AS REASON_CODE
        , NULL                                  AS JOURNAL_CMT_HEADER
        , NULL                                  AS JOURNAL_CMT
        , SERV.SPVB_SAPPO                       AS PONUM
        , NULL                                  AS SAO_DND
        , NULL                                  AS RET_WONUM
    FROM [FND].[W_CMMS_SERV_F] SERV
        LEFT JOIN [TMP_WO_X] ON 1=1
            AND TMP_WO_X.WORK_ORDERS = SERV.REFWO
    ) XXX
;