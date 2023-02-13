DECLARE @p_batch_id VARCHAR(8) = FORMAT(GETDATE(), 'yyyyMMdd')

;WITH MATDOC_EXTENDED AS (
    SELECT
        PL_X.PLANT_WID
        , M.LGORT
        , M.DMBTR_STOCK
        , MANDT
        , MBLNR
        , ZEILE
        , BWART
        , ABLAD
        , WAERS
    FROM [FND].[W_SAP_MATDOC_F_TEMP] M
    LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL_X ON 1=1
        AND PL_X.LGORT = M.LGORT 
        AND M.WERKS = PL_X.WERKS
), Reverse_doc AS (
        select F.RCLNT, RLDNR, RBUKRS, GJAHR, BELNR, DOCLN, R.BUDAT AS ORIGINAL_POSTING_DATE
        from [FND].[W_SAP_ACDOCA_SPP_F] F
            LEFT JOIN MATDOC_EXTENDED M ON M.MANDT = F.RCLNT AND M.MBLNR = F.AWREF AND CONCAT('00', ZEILE) = F.AWITEM
            LEFT JOIN 
                (select F.AWREF_REV,AWITEM, BUDAT,AWREF  from [FND].[W_SAP_ACDOCA_SPP_F] F where XREVERSING <> 'X' AND RACCT = '0000120050') R 
                ON F.AWREF = R.AWREF_REV AND F.AWITEM = R.AWITEM AND F.AWREF_REV = R.AWREF 
        where  F.XREVERSING = 'X'
        and F.RACCT = '0000120050' 
        AND M.BWART IN ('102')
)
    SELECT 
        F.BUDAT                                    AS DATE_WID
        , ISNULL(PRODUCT.PRODUCT_WID, 0)                AS PRODUCT_WID
        , ISNULL(M_X.PLANT_WID, 0)                      AS PLANT_WID
        , ISNULL(COST_CENTER.COST_CENTER_WID, 0)        AS COST_CENTER_WID	
        , 0 											AS ASSET_WID

        , CONVERT(DATE, R.ORIGINAL_POSTING_DATE)		AS ORIGINAL_POSTING_DATE
        , CONVERT(NVARCHAR(20), F.WERKS)           AS PLANT_CODE
        , CONVERT(NVARCHAR(20), F.BWTAR)           AS VALUATION_TYPE
        , CONVERT(
            VARCHAR, 
            REPLACE(LTRIM(REPLACE(F.MATNR, '0', ' ')), 
                    ' ', 
                    '0')
        ) 												AS MATERIAL_NUMBER
        , CONVERT(NVARCHAR(20), F.RCLNT)                  AS CLIENT_CODE
        , CONVERT(NVARCHAR(20), F.RLDNR)                AS LEDGER_CODE
        , CONVERT(NVARCHAR(20), M_X.LGORT)              AS STORAGE_LOCATION
        , CONVERT(NVARCHAR(20), F.RBUKRS)                 AS COMPANY_CODE
        , CONVERT(NVARCHAR(20), F.BWKEY)           AS VALUATION_AREA
        , '6000'                                        AS VALUATION_CLASS
        , CONVERT(NVARCHAR(8), MAT_DAT.MTART)           AS MATERIAL_TYPE
        , CONVERT(NVARCHAR(8), MAT_DAT.MATKL)           AS MATERIAL_GROUP
        , NULL                                          AS PURCHASING_GROUP
        , CONVERT(NVARCHAR(5), RRUNIT)                  AS BASE_UNIT_OF_MEASURE
        , CONVERT(NVARCHAR(5), F.VPRSV)            AS PRICE_CONTROL
        , CASE WHEN F.BLDAT = '00000000' 
            THEN NULL 
            ELSE CONVERT(DATE, F.BLDAT) 
        END  											AS DOCUMENT_DATE
        , CONVERT(NVARCHAR(15), F.BELNR)           AS DOCUMENT_NUMBER
        , CONVERT(NVARCHAR(10), F.DOCLN)                  AS LINE_ITEM
        , CONVERT(NVARCHAR(5), RUNIT)                   AS UNIT
        , CONVERT(NVARCHAR(5), RVUNIT)                  AS BASE_UNIT
        , CONVERT(NVARCHAR(5), RTCUR)                   AS CURRENCY
        , CONVERT(NVARCHAR(20), RACCT)                  AS ACCOUNT_NUMBER
        , CONVERT(NVARCHAR(5), DRCRK)                   AS DEBIT_INDICATOR
        , CONVERT(NVARCHAR(20), RCNTR)                  AS COST_CENTER
        , CONVERT(
            NVARCHAR(50), 
            COST_CENTER.COST_CENTER_DESC
        )                                               AS COST_CENTER_DESC
        , CONVERT(DECIMAL(38, 20), MSL)					AS QUANTITY
        , CONVERT(NVARCHAR(50), AWREF) 					AS MATERIAL_DOCUMENT
        , CONVERT(NVARCHAR(50), AWITEM) 				AS MATERIAL_LINE
        , CASE WHEN RHCUR IN ('VND', 'JPY') 
            THEN CONVERT(DECIMAL(38, 20), M_X.DMBTR_STOCK) * 100
            ELSE CONVERT(DECIMAL(38, 20), M_X.DMBTR_STOCK)
        END												AS STOCK_VALUE
        , CASE WHEN RHCUR IN ('VND', 'JPY') 
            THEN CONVERT(DECIMAL(38, 20), HSL) * 100 
            ELSE CONVERT(DECIMAL(38, 20), HSL) 
        END												AS LOCAL_AMOUNT
        , CONVERT(VARCHAR(10), BWART) 					AS MOVEMENT_TYPE
        , CONVERT(VARCHAR(100), ABLAD) 					AS UPLOADING_POINT
        , CONVERT(varchar(20), F.EBELN)         	AS PURCHASE_DOCUMENT
        , CONVERT(varchar(20), F.EBELP) 		    AS PURCHASE_LINE_ITEM

        , CONVERT(nvarchar(500), XREVERSING)            AS FLG_RESERVING
        , CONVERT(nvarchar(500), XREVERSED)             AS FLG_RESERVED
        , CONVERT(nvarchar(500), AWORG_REV)             AS FISCAL_YEAR_RESERVED
        , CONVERT(nvarchar(500), AWREF_REV)             AS REVERTED_DOCUMENT
        -- , CONVERT(nvarchar(500), XTRUEREV)              AS 
        -- , CONVERT(nvarchar(500), AWTYP_REV)             AS 
        -- , CONVERT(nvarchar(500), SUBTA_REV)             AS 

        , CONCAT_WS('~', F.RCLNT, F.RLDNR, F.RBUKRS, 
            F.GJAHR, F.BELNR, F.DOCLN)  		AS W_INTEGRATION_ID
        , 'N'                                           AS W_DELETE_FLG
        , 1                                             AS W_DATASOURCE_NUM_ID
        , DATEADD(HH, 7, GETDATE())                     AS W_INSERT_DT
        , DATEADD(HH, 7, GETDATE())                     AS W_UPDATE_DT
        , @p_batch_id                                   AS W_BATCH_ID
        , 'N'                                           AS W_UPDATE_FLG
    INTO #W_SAP_SPP_TRANSACTION_F_tmp
    FROM [FND].[W_SAP_ACDOCA_SPP_F] F
        LEFT JOIN [dbo].[W_PRODUCT_D] PRODUCT ON 1=1
            AND REPLACE(LTRIM(REPLACE(F.MATNR, '0', ' ')), ' ', '0') = PRODUCT.PRODUCT_CODE
            AND PRODUCT.W_DATASOURCE_NUM_ID = 1
        LEFT JOIN [FND].[W_SAP_MARA_D] MAT_DAT ON 1=1
            AND MAT_DAT.MATNR = F.MATNR
        LEFT JOIN [dbo].[W_SAP_COST_CENTER_D] COST_CENTER ON 1=1
            AND COST_CENTER.COST_CENTER = F.RCNTR 
            AND COST_CENTER.CLIENT = '300'
        LEFT JOIN MATDOC_EXTENDED M_X ON 1=1
            AND M_X.MANDT = F.RCLNT
            AND M_X.MBLNR = F.AWREF
            AND CONCAT('00', M_X.ZEILE) = F.AWITEM
        LEFT JOIN Reverse_doc R ON 1=1
            AND F.GJAHR = R.GJAHR AND F.BELNR = R.BELNR AND F.DOCLN = R.BELNR
    WHERE 1=1
        AND F.RCLNT = '300'
        AND F.RLDNR = '0L'
        AND F.RACCT in ('0000120050' /*, '0000530302','0000530301','0000530303','0000530304','0000530305' ,'0000530300'*/ )
        AND F.VPRSV = 'V'
        AND (
            REPLACE(LTRIM(REPLACE(F.MATNR, '0', ' ')), ' ', '0') LIKE '6%'
            OR REPLACE(LTRIM(REPLACE(F.MATNR, '0', ' ')), ' ', '0') LIKE '9%'
        )

-- DECLARE @month SMALLINT = 3
-- DECLARE @year INT = 2019
-- WHILE 


-- "SELECT
--     RCLNT, RLDNR, RBUKRS, GJAHR, BELNR, DOCLN, RYEAR, BUDAT
--     , RTCUR, RWCUR, RHCUR, RACCT, RCNTR, PRCTR, RFAREA
--     , RBUSA, KOKRS, WERKS, MATNR, MATNR_COPA, VKBUR_PA, VTWEG
--     , AUGDT, KTOPL, AUART_PA, KVGR2_PA as KDGRP,
--     TSL, WSL, HSL, MSL, VMSL, ANLN1, SGTXT,
--     BWKEY, RRUNIT, VPRSV, BLDAT, RUNIT, RVUNIT, DRCRK
-- FROM SAPHANADB.ACDOCA
-- WHERE 1=1
--     and RCLNT = '300'
--     AND RLDNR = '0L'
--     AND RACCT = '0000120050'"
