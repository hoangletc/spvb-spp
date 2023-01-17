SELECT TOP 100
    BUDAT AS DATE_WID
    , ISNULL(PRODUCT.PRODUCT_WID, 0)                AS PRODUCT_WID
    , ISNULL(PLANT.PLANT_WID, 0)                    AS PLANT_WID
    , ISNULL(LOC.ASSET_WID, 0)                      AS LOCATION_WID
    , ISNULL(CC.ASSET_WID, 0)                       AS COST_CENTER_WID
    
    , ACDOCA.WERKS                                  AS PLANT_CODE
    , BWTAR                                         AS VALUATION_TYPE
    , CONVERT(nvarchar(100), REPLACE(LTRIM(
        REPLACE(ACDOCA.MATNR, '0', ' ')), ' ', '0')
    )                                               AS MATERIAL_NUMBER
    , RCLNT                                         AS CLIENT_CODE
    , RLDNR                                         AS LEDGER_CODE
    , LOC.LGORT                                     AS STORAGE_LOCATION
    , RBUKRS                                        AS COMPANY_CODE
    , ACDOCA.BWKEY                                  AS VALUATION_AREA
    , NULL                                          AS VALUATION_CLASS
    , CONVERT(NVARCHAR(8), MAT_DAT.MTART)           AS MATERIAL_TYPE
    , CONVERT(NVARCHAR(8), MAT_DAT.MATKL)           AS MATERIAL_GROUP --
    , NULL                                          AS PURCHASING_GROUP
    -- , CONVERT(nvarchar(100), MAT_DES.MAKTX) AS MATERIAL_DESCRIPTION
    , CONVERT(NVARCHAR(5), RRUNIT)                  AS BASE_UNIT_OF_MEASURE
    , CONVERT(NVARCHAR(5), VPRSV)                   AS PRICE_CONTROL
    , CASE WHEN BLDAT = '00000000' THEN NULL 
        ELSE CONVERT(DATE, BLDAT) END 				AS DOCUMENT_DATE
    , CONVERT(NVARCHAR(15), BELNR)                  AS DOCUMENT_NUMBER
    , CONVERT(NVARCHAR(10), DOCLN)                  AS LINE_ITEM
    , CONVERT(NVARCHAR(5), RUNIT)                   AS UNIT
    , CONVERT(NVARCHAR(5), RVUNIT)                  AS BASE_UNIT
    , CONVERT(NVARCHAR(5), RTCUR)                   AS CURRENCY
    , CONVERT(NVARCHAR(20), RACCT)                  AS ACCOUNT_NUMBER
    , CONVERT(NVARCHAR(5), DRCRK)                   AS DEBIT_INDICATOR

    , CASE WHEN RCNTR = '' THEN NULL 
        ELSE CONVERT(DECIMAL(38, 20), RCNTR) END    AS COST_CENTER
    , CASE WHEN CC.KTEXT IS NULL THEN NULL 
        ELSE CONVERT(DECIMAL(38, 20), CC.KTEXT) END AS COST_CENTER_DESCRIPTION

    , CONVERT(INT, MSL)								AS QUANTITY
    , CONVERT(DECIMAL(38, 20), HSL)					AS LOCAL_AMOUNT

    , CONCAT_WS('~', RCLNT, RLDNR, RBUKRS, 
                GJAHR, BELNR, DOCLN)                AS W_INTEGRATION_ID
    , 'N'                                           AS W_DELETE_FLG
    , 1                                             AS W_DATASOURCE_NUM_ID
    , GETDATE()                                     AS W_INSERT_DT
    , GETDATE()                                     AS W_UPDATE_DT
    , NULL                                          AS W_BATCH_ID
    , 'N'                                           AS W_UPDATE_FLG

FROM [FND].[W_SAP_ACDOCA_SPP_F] ACDOCA

    LEFT JOIN [dbo].[W_PRODUCT_D] PRODUCT ON 1=1
        AND REPLACE(LTRIM(REPLACE(ACDOCA.MATNR, '0', ' ')), ' ', '0') = PRODUCT.PRODUCT_CODE
        AND PRODUCT.W_DATASOURCE_NUM_ID = 1
    LEFT JOIN [dbo].[W_PLANT_SAP_D] PLANT ON 1=1
        AND PLANT.PLANT_CODE = ACDOCA.WERKS
    LEFT JOIN [FND].[W_SAP_MARA_D] MAT_DAT ON 1=1
        AND MAT_DAT.MATNR = ACDOCA.MATNR
    LEFT JOIN [dbo].[W_SAP_CSKS_D] CV ON 1=1
        AND CV.MANDT = ACDOCA.RCLNT
        AND CV.BUKRS = ACDOCA.RBUKRS
        AND CV.WERKS = ACDOCA.WERKS
    LEFT JOIN [dbo].[W_SAP_CSKT_D] CC ON 1=1
        AND CC.KOSTL = ACDOCA.RCNTR
    -- LEFT JOIN [dbo].[W_SAP_CSKt_D] CC ON 1=1 -- note: vẫn chưa biết load để làm gì
    LEFT JOIN [dbo].[W_SAP_T001L_D] LOC ON 1=1
        AND LOC.MANDT = ACDOCA.RCLNT
        AND LOC.WERKS = ACDOCA.WERKS

WHERE ACDOCA.RCLNT = '300'