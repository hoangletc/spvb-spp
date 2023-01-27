SELECT TOP 100
    FORMAT(CONVERT(DATE, CHANGEDATE), 'yyyymmdd')   AS DATE_WID
    , ISNULL(PLANT.PLANT_WID, 0)                    AS PLANT_WID
    , ISNULL(LOC.LOC_WID, 0)                        AS LOCATION_WID
    , ISNULL(AST.ASSET_WID, 0)                      AS ASSET_WID

    , AST.LINE_ASSET_NUM                            AS LINE_ASSET_NUM
    , AST.LINE_ASSET_DES                            AS LINE_ASSET_DESC
    , AS_ST.ASSETNUM                                AS ASSET_NUM
    , CONVERT(DECIMAL(38,20), DOWNTIME) * 60        AS DOWNTIME
    , CONVERT(nvarchar(100), AST.DESCRIPTION)       AS [NAME]
    , CASE WHEN WONUM IS NULL 
        THEN 'PRO' ELSE 'ME' END                    AS ANALYSIS_1 
    , CASE 
        WHEN LEFT(CODE, 1) = 'M' THEN 'Material'  
        WHEN LEFT(CODE, 1) = 'O' THEN 'Operation' 
        WHEN LEFT(CODE, 1) = 'E' THEN 'Equipment' 
        WHEN LEFT(CODE, 1) = 'A' THEN 'Adjustment' 
        WHEN LEFT(CODE, 1) = 'S' THEN 'Shutdown' 
        WHEN LEFT(CODE, 1) = 'R' THEN 'Routine' 
        ELSE ' ' END                                AS ANALYSIS_2
    , CONVERT(nvarchar(5), CODE)                    AS ANALYSIS_3
    , CONVERT(nvarchar(50), CODE_DESCRIPTION)       AS DOWNTIME_CODE
    , CASE WHEN SPVB_ISSUE IS NULL THEN NULL
        ELSE CONVERT(NVARCHAR(100), SPVB_ISSUE) END AS ISSUE
    , CASE WHEN SPVB_CA IS NULL THEN NULL
        ELSE CONVERT(NVARCHAR(50), SPVB_CA) END     AS CORRECTIVE_ACTION
    , CASE WHEN SPVB_PA IS NULL THEN NULL
        ELSE CONVERT(NVARCHAR(50), SPVB_PA) END     AS PREVENTIVE_ACTION
    , CASE WHEN REMARKS IS NULL THEN NULL
        ELSE CONVERT(NVARCHAR(50), REMARKS) END     AS REMARKS
    
    , CONVERT(
        NVARCHAR(200), 
        CONCAT_WS('~', ASSETSTATUSID, AS_ST.[LOCATION], 
                    DOWNTIME, CODE, AS_ST.ASSETNUM)
    )                                               AS W_INTEGRATION_ID
    , 'N'                                           AS W_DELETE_FLG
    , 1                                             AS W_DATASOURCE_NUM_ID
    , GETDATE()                                     AS W_INSERT_DT
    , GETDATE()                                     AS W_UPDATE_DT
    , NULL                                          AS W_BATCH_ID
    , 'N'                                           AS W_UPDATE_FLG
-- INTO #W_CMMS_DOWNTIME_F_tmp
FROM [FND].[W_CMMS_ASSET_STATUS_F] AS_ST
    LEFT JOIN [dbo].[W_PLANT_SAP_D] PLANT ON 1=1
        AND PLANT.PLANT_NAME_2 = LEFT(AS_ST.LOCATION, 3)
    LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
        AND LOC.[LOCATION] = LEFT(AS_ST.[LOCATION], 7)
    LEFT JOIN [dbo].[W_CMMS_ASSET_D] AST ON 1=1
        AND AST.ASSET_NUM = AS_ST.ASSETNUM
