DECLARE @p_batch_id NVARCHAR(8) = '20230318';

SELECT
    FORMAT(CONVERT(DATE, FROMDATE), 'yyyyMMdd')   AS DATE_WID
    , ISNULL(PLANT.PLANT_WID, 0)                    AS PLANT_WID
    , ISNULL(LOC.LOC_WID, 0)                        AS LOCATION_WID
    , ISNULL(A.ASSET_WID, 0)                    	AS ASSET_WID
    , ISNULL(LINE_CAT.LINE_CAT_WID, 0)              AS LINE_CAT_WID

    , A.LINE_ASSET_NUM
    , A.LINE_ASSET_DESCRIPTION
    , LINE_CAT.CATEGORY                             AS LINE_CATEGORY
    , LINE_CAT.FROM_DATE                            AS LINE_CAT_FROM_DATE
    , LINE_CAT.TO_DATE                              AS LINE_CAT_TO_DATE

    , A.MACHINE_ASSET_NUM
    , A.MACHINE_SHORT_NAME                          

    , F.ASSETSTATUS_ID							    AS ASSET_STATUS_UID
    , F.ASSET_UID                                   AS ASSET_UID
    , F.ASSET_NUM                                   AS ASSET_NUM
    , CONVERT(DECIMAL(38,20), DOWNTIME)             AS DOWNTIME

    , CONVERT(nvarchar(100), A.DESCRIPTION)     	AS [NAME]
    , F.IS_SPLIT								    AS IS_SPLIT
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
    
    , F.W_INTEGRATION_ID                                               AS W_INTEGRATION_ID
    , 'N'                                           AS W_DELETE_FLG
    , 'N' 											AS W_UPDATE_FLG
    , 8                                             AS W_DATASOURCE_NUM_ID
    , DATEADD(HH, 7, GETDATE())                     AS W_INSERT_DT
    , DATEADD(HH, 7, GETDATE())                     AS W_UPDATE_DT
    , @p_batch_id                                   AS W_BATCH_ID
INTO #W_CMMS_DOWNTIME_F_tmp
FROM [FND].[W_CMMS_ASSET_STATUS_F] F
    LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PLANT ON 1=1
        AND PLANT.PLANT_NAME_2 = LEFT(F.LOCATION, 3)
        AND PLANT.STO_LOC = ''
    LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
        AND LOC.[LOCATION] = LEFT(F.[LOCATION], 7)
    LEFT JOIN dbo.W_CMMS_ASSET_D A ON 1=1
        AND A.ASSET_UID = F.ASSET_UID
    LEFT JOIN [dbo].[W_EXCEL_SPP_LINE_CATEGORY_F] LINE_CAT ON 1=1 AND LINE_CAT.LINE_ASSET_NUM = A.LINE_ASSET_NUM 
        AND LINE_CAT.FROM_DATE <= CONVERT(DATETIME2, FROMDATE) AND LINE_CAT.TO_DATE >= CONVERT(DATETIME2, CHANGEDATE)
;

select top 10 * from #W_CMMS_DOWNTIME_F_tmp;

select top 10 * from [dbo].[W_CMMS_DOWNTIME_F];
select top 10 * from [FND].[W_CMMS_ASSET_STATUS_F];
select top 10 * from [dbo].[W_CMMS_ASSET_D];

-- ALTER TABLE [FND].[W_CMMS_WO_STATUS_D] DROP MACHINE_SHORTNAME;
ALTER TABLE [dbo].[W_CMMS_DOWNTIME_F] ADD [N_SPLIT] INT;