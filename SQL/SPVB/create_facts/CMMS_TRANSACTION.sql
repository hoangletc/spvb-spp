;WITH
TMP_WO_X AS (
    SELECT
        WO.*
    , WO_STA.CHANGEDATE               AS LAST_STATUS_DATE
    FROM [dbo].[W_CMMS_WO_F] AS WO
        LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_STA ON 1=1
            AND WO_STA.PARENT = WO.WORK_ORDERS
            AND WO_STA.STATUS = WO.[STATUS]
),
TMP_MATU_X AS (
    SELECT 
        LOC.LOC_WID AS LOC_WID
        , LOC.[DESCRIPTION] AS LOC_DES
        , MATU.LINECOST
        , MATU.MRNUM
        , MATU.ASSET_NUM
        , MATU.ITEM_NUM
        , MATU.UNITCOST
    FROM FND.W_CMMS_MATU_F MATU
    LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
        AND LOC.[LOCATION] = LEFT(MATU.[LOCATION], 7)
)

    SELECT TOP 10
        LOC.LOC_WID                     AS LOC_WID
        , AST.ASSET_WID                 AS ASSET_WID
        , INVU.INVU_WID                 AS INVU_WID
        , INVUL.INVUL_WID               AS INVUL_WID
        , IT.ITEM_WID                   AS ITEM_WID
        , TMP_WO_X.WO_WID               AS WO_WID

        , INVU.INVUSE_NUM               AS USAGE
        , MATU.STORELOC                 AS WAREHOUSE
        , LOC.[DESCRIPTION]             AS WAREHOUSE_NAME
        , CASE WHEN MATU.REFWO IS NULL
        AND MATU.ISSUE_TYPE = 'RETURN' 
            THEN 'RECEIPT MISC'
            ELSE MATU.ISSUE_TYPE 
        END                             AS TRANSACTION_TYPE
        , MATU.TRANSDATE                AS TRANSACTION_DATE
        , MATU.ACTUALDATE               AS ACTUAL_DATE
        , MATU.ITEM_NUM                 AS ITEM_NO
        , IT.DESCRIPTION                AS [DESCRIPTION]
        , MATU.QUANTITY                 AS TRANSACTION_QUANT
        , MATU.ISSUE_UNIT               AS TRANSACTION_UOM
        , MATU.BINNUM                   AS BINNUM
        , TMP_WO_X.OVERHAUL             AS OVERHAUL
        , CASE WHEN INVUL.SPVB_MUSTRETURN_ORG = 'True'
            THEN 'Y'
            ELSE 'N'
        END                             AS MUST_RETURN_ORIGINAL
        , CASE WHEN INVUL.SPVB_MUSTRETURN = 'True'
            THEN 'Y'
            ELSE 'N'
        END                             AS MUST_RETURN_USER_INPUT
        , INVUL.SPVB_REASON             AS MUST_RETURN_REMARK
        , MATU.UNITCOST                 AS PRICE
        , MATU.LINECOST                 AS AMOUNT
        , MATU.MRNUM                    AS MRNUM
        , CASE WHEN TMP_WO_X.PARENT IS NULL 
            THEN TMP_WO_X.WORK_ORDERS
            ELSE NULL 
        END                             AS WORK_ORDER
        , MATU.ASSET_NUM                AS ASSET
        , AST.LINE_ASSET_NUM            AS [LINE]
        , TMP_WO_X.[TYPE]               AS WORK_TYPE
        , TMP_WO_X.[STATUS]             AS WORKORDER_STATUS
        , TMP_WO_X.DATE_FINISHED        AS ACTUAL_FINISH
        , TMP_WO_X.LAST_STATUS_DATE     AS WO_LAST_STATUSDATE
        , TMP_WO_X.SUPERVISOR           AS WO_DONE_BY
        , INVUL.ENTER_BY                AS USER_ID
        , INVUL.SPVB_EXTREASONCODE      AS REASON_CODE
        , INVU.DESCRIPTION              AS JOURNAL_CMT_HEADER
        , INVUL.REMARK                  AS JOURNAL_CMT
        , MATU.PONUM                    AS PONUM
        , NULL                          AS SAP_DND
        , INVUL.SPVB_WONUMREF           AS RET_WONUM

    FROM FND.W_CMMS_MATU_F MATU
        LEFT JOIN [dbo].[W_CMMS_INVU_D] INVU ON 1=1
            AND INVU.MATU_ID = MATU.MATU_ID
            AND INVU.ASSET_NUM = MATU.ASSET_NUM
            AND INVU.ITEM_NUM = MATU.ITEM_NUM
        LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
            AND LOC.[LOCATION] = LEFT(MATU.[LOCATION], 7)
        LEFT JOIN [dbo].[W_CMMS_ITEM_D] IT ON 1=1
            AND IT.ITEM_NUM = MATU.ITEM_NUM
        LEFT JOIN [dbo].[W_CMMS_INVUL_D] INVUL ON 1=1
            AND INVU.MATU_ID = MATU.MATU_ID
            AND INVU.ASSET_NUM = MATU.ASSET_NUM
            AND INVU.ITEM_NUM = MATU.ITEM_NUM
        LEFT JOIN [dbo].[W_CMMS_ASSET_D] AST ON 1=1
            AND AST.ASSET_NUM = MATU.ASSET_NUM
        LEFT JOIN [TMP_WO_X] ON 1=1
            AND TMP_WO_X.WORK_ORDERS = MATU.REFWO

    UNION ALL

    
    SELECT TOP 10
        TMP_MATU_X.LOC_WID                      AS LOC_WID
        , AST.ASSET_WID                         AS ASSET_WID
        , INVU_WID                              AS INVU_WID
        , INVUL_WID                             AS INVUL_WID
        , ITEM_WID                              AS ITEM_WID
        , TMP_WO_X.WO_WID                       AS WO_WID

        , CASE WHEN MATR.ISSUE_TYPE = 'TRANSFER'
            THEN INVU.INVUSE_NUM       
            ELSE MATR.SPVB_SAPRECEIPT
        END                                     AS USAGE
        , CASE WHEN MATR.FROM_STORELOC = 'INSPECTION' THEN MATR.FROM_STORELOC
            WHEN MATR.TO_STORELOC = 'INSPECTION' THEN MATR.FROM_STORELOC
            WHEN MATR.FROM_STORELOC = MATR.TO_STORELOC THEN MATR.FROM_STORELOC
            ELSE 'From ' + MATR.FROM_STORELOC + ' To ' + MATR.TO_STORELOC
        END                                     AS WAREHOUSE
        , TMP_MATU_X.LOC_DES                    AS WAREHOUSE_NAME
        , CASE WHEN MATR.ISSUE_TYPE = 'TRANSFER' 
                AND MATR.FROM_STORELOC = 'INSPECTION'
                AND MATR.SHIPMENT_NUM IS NOT NULL
            THEN 'SHIPRECEIPT' 
            WHEN MATR.ISSUE_TYPE = 'TRANSFER'
                AND MATR.FROM_STORELOC = 'INSPECTION'
                AND MATR.SHIPMENT_NUM IS NULL
            THEN 'RECEIPT'
            ELSE MATR.ISSUE_TYPE
        END                                     AS TRANSACTION_TYPE
        , MATR.TRANSDATE                        AS TRANSACTION_DATE
        , MATR.ACTUALDATE                       AS ACTUAL_DATE
        , MATR.ITEM_NUM                         AS ITEM_NO
        , IT.DESCRIPTION                        AS [DESCRIPTION]
        , MATR.QUANTITY                         AS TRANSACTION_QUANT
        , MATR.ISSUE_UNIT                       AS TRANSACTION_UOM
        , MATR.BINNUM                           AS BINNUM
        , [TMP_WO_X].OVERHAUL                   AS OVERHAUL
        , CASE WHEN INVUL.SPVB_MUSTRETURN_ORG = 'True'
            THEN 'Y'
            ELSE 'N'
        END                                     AS MUST_RETURN_ORIGINAL
        , NULL                                  AS MUST_RETURN_USER_INPUT
        , INVUL.SPVB_REASON                     AS MUST_RETURN_REMARK
        , [TMP_MATU_X].UNITCOST                 AS PRICE
        , [TMP_MATU_X].LINECOST                 AS AMOUNT
        , [TMP_MATU_X].MRNUM                    AS MRNUM
        , CASE WHEN TMP_WO_X.PARENT IS NULL 
            THEN TMP_WO_X.WORK_ORDERS
            ELSE NULL 
        END                                     AS WORK_ORDER
        , [TMP_MATU_X].ASSET_NUM                AS ASSET
        , AST.LINE_ASSET_NUM                    AS [LINE]
        , [TMP_WO_X].[TYPE]                     AS WORK_TYPE
        , [TMP_WO_X].[STATUS]                   AS WORKORDER_STATUS
        , [TMP_WO_X].DATE_FINISHED              AS ACTUAL_FINISH
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
    LEFT JOIN [dbo].[W_CMMS_INVU_D] INVU ON 1=1
            AND INVU.ASSET_NUM = MATR.ASSET_NUM
            AND INVU.ITEM_NUM = MATR.ITEM_NUM
        LEFT JOIN [dbo].[W_CMMS_ITEM_D] IT ON 1=1
            AND IT.ITEM_NUM = MATR.ITEM_NUM
        LEFT JOIN [dbo].[W_CMMS_INVUL_D] INVUL ON 1=1
            AND INVUL.ASSET_NUM = MATR.ASSET_NUM
            AND INVUL.ITEM_NUM = MATR.ITEM_NUM
        LEFT JOIN [dbo].[W_CMMS_ASSET_D] AST ON 1=1
            AND AST.ASSET_NUM = MATR.ASSET_NUM
        LEFT JOIN [TMP_WO_X] ON 1=1
            AND TMP_WO_X.WORK_ORDERS = MATR.REFWO
        LEFT JOIN [TMP_MATU_X] ON 1=1
            AND [TMP_MATU_X].ASSET_NUM = MATR.ASSET_NUM
            AND  [TMP_MATU_X].ITEM_NUM = MATR.ITEM_NUM

    UNION ALL

    SELECT TOP 10
        TMP_MATU_X.LOC_WID              AS LOC_WID
        , NULL                          AS ASSET_WID
        , NULL                          AS INVU_WID
        , INVUL.INVUL_WID               AS INVUL_WID
        , IT.ITEM_WID                   AS ITEM_WID
        , NULL                          AS WO_WID

        , INVT.EXTERNAL_REFID           AS USAGE
        , INVT.STORELOC                 AS WAREHOUSE
        , TMP_MATU_X.[LOC_DES]          AS WAREHOUSE_NAME
        , 'Physical Count'              AS TRANSACTION_TYPE
        , INVT.TRANSDATE                AS TRANSACTION_DATE
        , INVT.ACTUALDATE               AS ACTUAL_DATE
        , INVT.ITEM_NUM                 AS ITEM_NO
        , IT.DESCRIPTION                AS [DESCRIPTION]
        , INVT.QUANTITY                 AS TRANSACTION_QUANT
        , NULL                          AS TRANSACTION_UOM
        , INVT.BIN_NUM                  AS BINNUM
        , NULL                          AS OVERHAUL
        , CASE WHEN INVUL.SPVB_MUSTRETURN_ORG = 1
            THEN 'Y'
            ELSE 'N'
        END                             AS MUST_RETURN_ORIGINAL
        , NULL                          AS MUST_RETURN_USER_INPUT
        , NULL                          AS MUST_RETURN_REMARK
        , 0                             AS PRICE
        , 0                             AS AMOUNT
        , NULL                          AS MRNUM
        , NULL                          AS WORK_ORDER
        , NULL                          AS ASSET
        , NULL                          AS [LINE]
        , NULL                          AS WORK_TYPE
        , NULL                          AS WORKORDER_STATUS
        , NULL                          AS ACTUAL_FINISH
        , NULL                          AS WO_LAST_STATUSDATE
        , NULL                          AS WO_DONE_BY
        , NULL                          AS USER_ID
        , NULL                          AS REASON_CODE
        , NULL                          AS JOURNAL_CMT_HEADER
        , NULL                          AS JOURNAL_CMT
        , NULL                          AS PONUM
        , NULL                          AS SAO_DND
        , NULL                          AS RET_WONUM
    FROM [FND].[W_CMMS_INVT_F] INVT
        LEFT JOIN [TMP_MATU_X] ON 1=1
            AND TMP_MATU_X.ITEM_NUM = INVT.ITEM_NUM
        LEFT JOIN [dbo].[W_CMMS_ITEM_D] IT ON 1=1
            AND IT.ITEM_NUM = INVT.ITEM_NUM
        LEFT JOIN [dbo].[W_CMMS_INVUL_D] INVUL ON 1=1
            AND INVUL.ITEM_NUM = INVT.ITEM_NUM

    UNION ALL

    SELECT TOP 10
        TMP_MATU_X.LOC_WID              AS LOC_WID
        , NULL                          AS ASSET_WID
        , NULL                          AS INVU_WID
        , NULL                          AS INVUL_WID
        , NULL                          AS ITEM_WID
        , NULL                          AS WO_WID

        , SERV.SPVB_SAPRECEIPT          AS USAGE
        , NULL                          AS WAREHOUSE
        , NULL                          AS WAREHOUSE_NAME
        , CASE WHEN SERV.ISSUE_TYPE = 'RETURN'
            THEN 'SERVICE RETURN'
            ELSE 'SERVICE RECEIPT'
        END                             AS TRANSACTION_TYPE
        , SERV.TRANSDATE                AS TRANSACTION_DATE
        , SERV.ACTUALDATE               AS ACTUAL_DATE
        , SERV.ITEM_NUM                 AS ITEM_NO
        , SERV.[DESCRIPTION]            AS [DESCRIPTION]
        , SERV.QUANTITY                 AS TRANSACTION_QUANT
        , NULL                          AS TRANSACTION_UOM
        , NULL                          AS BINNUM
        , CASE WHEN TMP_WO_X.OVERHAUL = 1
            THEN 'Y'
            ELSE 'N'
        END                             AS OVERHAUL
        , NULL                          AS MUST_RETURN_ORIGINAL
        , NULL                          AS MUST_RETURN_USER_INPUT
        , NULL                          AS MUST_RETURN_REMARK
        , SERV.UNITCOST                 AS PRICE
        , SERV.LINECOST                 AS AMOUNT
        , NULL                          AS MRNUM
        , NULL                          AS WORK_ORDER
        , SERV.ASSET_NUM                AS ASSET
        , AST.LINE_ASSET_NUM            AS [LINE]
        , TMP_WO_X.[TYPE]               AS WORK_TYPE
        , TMP_WO_X.[STATUS]             AS WORKORDER_STATUS
        , TMP_WO_X.DATE_FINISHED        AS ACTUAL_FINISH
        , TMP_WO_X.LAST_STATUS_DATE     AS WO_LAST_STATUSDATE
        , TMP_WO_X.SUPERVISOR           AS WO_DONE_BY
        , NULL                          AS USER_ID
        , NULL                          AS REASON_CODE
        , NULL                          AS JOURNAL_CMT_HEADER
        , NULL                          AS JOURNAL_CMT
        , SERV.SPVB_SAPPO               AS PONUM
        , NULL                          AS SAO_DND
        , NULL                          AS RET_WONUM
    FROM [FND].[W_CMMS_SERV_F] SERV
        LEFT JOIN [TMP_MATU_X] ON 1=1
            AND [TMP_MATU_X].ASSET_NUM = SERV.ASSET_NUM
            AND [TMP_MATU_X].ITEM_NUM = SERV.ITEM_NUM
        LEFT JOIN [TMP_WO_X] ON 1=1
            AND TMP_WO_X.WORK_ORDERS = SERV.REFWO
        LEFT JOIN [dbo].[W_CMMS_ASSET_D] AST ON 1=1
            AND AST.ASSET_NUM = SERV.ASSET_NUM
