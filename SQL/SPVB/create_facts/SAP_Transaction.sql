SELECT TOP 100
    BUDAT AS DATE_WID
    , ISNULL(P.PRODUCT_WID, 0)                          AS PRODUCT_WID
    , ISNULL(PL_X.PLANT_WID, 0)                         AS PLANT_WID
    , ISNULL(WO.WO_WID, 0)                              AS WO_WID

    , LIFNR                                             AS VENDOR_CODE
    , MD.WERKS                                          AS PLANT_CODE
    , BUKRS                                             AS COMPANY_CODE
    , BSTAUS_SG                                         AS STOCK_STATUS
    , WAERS                                             AS CURRENCY_CODE
    , CASE WHEN WAERS IN ('VND', 'JPY') 
        THEN CONVERT(DECIMAL(38, 20), DMBTR_STOCK) * 100 
        ELSE CONVERT(DECIMAL(38, 20), DMBTR_STOCK) END  AS STOCK_VALUE
    , MEINS                                             AS BASE_UNIT
    , CONVERT(DECIMAL(38,20), STOCK_QTY)                AS STOCK_QUANTITY_IN_BASE_UNIT
    , ERFME AS ENTRY_UNIT
    , CONVERT(DECIMAL(38,20), ERFMG)                    AS STOCK_QUANTITY_IN_ENTRY_UNIT
    , CONVERT(DATE, BUDAT)                              AS POSTING_DATE
    , CONVERT(DATE, CPUDT)                              AS CREATE_DATE
    , CONVERT(DATE, BLDAT)                              AS DOCUMENT_DATE
    , CASE WHEN AEDAT = '00000000' 
        THEN NULL ELSE CONVERT(DATE, AEDAT) END         AS UPDATE_DATE
    , MBLNR                                             AS DOCUMENT_NUMBER
    , ZEILE                                             AS DOCUMENT_LINE_ITEM
    , LINE_ID                                           AS LINE_ID
    , EBELN                                             AS PURCHASE_DOCUMENT
    , EBELP                                             AS PURCHASE_LINE_ITEM
    , SMBLN                                             AS ORIGINAL_DOCUMENT_NUM
    , SMBLP                                             AS ORIGINAL_DOCUMENT_LINE
    , XBLNR                                             AS REFERENCE_DOCUMENT
    , VBELN_IM                                          AS DELIVERY_DOCUMENT
    , VBELP_IM                                          AS DELIVER_LINE_ITEM
    , XAUTO                                             AS IS_AUTO_FLG
    , REPLACE(
        LTRIM(REPLACE(MD.MATNR, '0', ' ')), ' ', '0'
    )                                                   AS MATERIAL_NUMBER
    , MD.LGORT                                          AS STORAGE_LOCATION
    , CHARG                                             AS BATCH_NUMBER
    , BWTAR                                             AS VALUATION_TYPE
    , SHKZG                                             AS CR_DR_FLG
    , SGTXT                                             AS [TEXT]
    , VPRSV                                             AS PRICE_INDICATOR
    , BLART                                             AS DOCUMENT_TYPE
    , PRCTR                                             AS PROFIT_CENTER
    , KOSTL                                             AS COST_CENTER
    , ABLAD                                             AS UPLOADING_POINT
    , BWART                                             AS MOVEMENT_TYPE
    , MD.VPRSV                                          AS PRICE_INDICATOR
	, MD.BWTAR                                          AS VALUATION_TYPE

    , CONCAT_WS('~', KEY1, KEY2, KEY3, 
                KEY4, KEY5, KEY6)                       AS W_INTEGRATION_ID
    , 'N'                                               AS W_DELETE_FLG
	, 1                                                 AS W_DATASOURCE_NUM_ID
	, GETDATE()                                         AS W_INSERT_DT
	, GETDATE()                                         AS W_UPDATE_DT
	, NULL                                              AS W_BATCH_ID
-- INTO #W_SAP_TRANSACTION_F_tmp
FROM [FND].[W_SAP_MATDOC_F_temp] MD
    LEFT JOIN [dbo].[W_PRODUCT_D] P ON 1=1
        AND REPLACE(LTRIM(REPLACE(MD.MATNR, '0', ' ')), ' ', '0') = P.PRODUCT_CODE
        AND P.W_DATASOURCE_NUM_ID = 1
    LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL_X ON 1=1
        AND PL_X.LGORT = MD.LGORT
    LEFT JOIN [FND].[W_CMMS_WO_F] WO ON 1=1
        AND WO.WONUM =  MD.ABLAD
    LEFT JOIN [dbo].[W_VENDOR_D] as V on 1=1
        AND V.VENDOR_CODE = MD.LIFNR 
        AND V.CLIENT_CODE = '300'
WHERE  1=1
    AND MD.MANDT = '300'
    AND (
        REPLACE(LTRIM(REPLACE(MD.MATNR, '0', ' ')), ' ', '0') LIKE '6%'
        OR REPLACE(LTRIM(REPLACE(MD.MATNR, '0', ' ')), ' ', '0') LIKE '9%'
    )


-- SELECT top 10 * from [FND].[W_CMMS_WO_F]
-- select top 10 * from [dbo].[W_PLANT_SAP_D]
-- select top 10 * from STG.W_SAP_MATDOC_FS_TEMP

-- SELECT *
-- FROM SAPHANADB.MATDOC 
-- where 1=1 
--     AND MANDT = '300' 
--     AND BUDAT >= '20200101' 
--     AND (
--         REPLACE(LTRIM(REPLACE(MATNR, '0', ' ')), ' ', '0') LIKE '6%'
--     OR REPLACE(LTRIM(REPLACE(MATNR, '0', ' ')), ' ', '0') LIKE '9%'
--     )
