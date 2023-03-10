DECLARE @p_batch_id NVARCHAR(8) = '20230309';

WITH TMP_ASSET AS (
    SELECT
        F1.ASSET_WID
        , F1.ASSET_UID
        , F1.[DESCRIPTION]

        , F2.ASSET_NUM       	AS LINE_ASSET_NUM
        , F2.ASSET_UID       	AS LINE_ASSET_UID
        , F2.[DESCRIPTION]  	AS LINE_ASSET_DESCRIPTION

        , F3.ASSET_NUM       	AS MACHINE_ASSET_NUM
        , F3.ASSET_UID       	AS MACHINE_ASSET_UID
        , F3.DESCRIPTION     	AS MACHINE_ASSET_DESCRIPTION
    FROM [dbo].[W_CMMS_ASSET_D] F1
    LEFT JOIN [dbo].[W_CMMS_ASSET_D] F2 ON 1=1
        AND F1.LINE_ASSET_NUM = F2.ASSET_NUM
        AND LEFT(F1.LOCATION, 3) = LEFT(F2.LOCATION, 3)
    LEFT JOIN [dbo].[W_CMMS_ASSET_D] F3 ON 1=1
        AND F1.MACHINE_ASSET_NUM = F3.ASSET_NUM
        AND LEFT(F1.LOCATION, 3) = LEFT(F3.LOCATION, 3)
)
    SELECT
        FORMAT(CONVERT(DATE, CHANGEDATE), 'yyyyMMdd')   AS DATE_WID
        , ISNULL(PLANT.PLANT_WID, 0)                    AS PLANT_WID
        , ISNULL(LOC.LOC_WID, 0)                        AS LOCATION_WID
        , ISNULL(AST.ASSET_WID, 0)                    	AS ASSET_WID
        , ISNULL(LINE_CAT.LINE_CAT_WID, 0)              AS LINE_CAT_WID

        , AST.LINE_ASSET_NUM
        , AST.LINE_ASSET_DESCRIPTION
        , AST.LINE_ASSET_UID
        , LINE_CAT.CATEGORY                             AS LINE_CATEGORY
        , LINE_CAT.FROM_DATE                            AS LINE_CAT_FROM_DATE
        , LINE_CAT.TO_DATE                              AS LINE_CAT_TO_DATE

        , AST.MACHINE_ASSET_NUM
        , AST.MACHINE_ASSET_UID		
        , AST.MACHINE_ASSET_DESCRIPTION

        , AS_ST.ASSETSTATUSID							AS ASSET_STATUS_UID
        , AS_ST.ASSET_UID                               AS ASSET_UID
        , AS_ST.ASSETNUM                                AS ASSET_NUM
        , CONVERT(DECIMAL(38,20), DOWNTIME) * 60        AS DOWNTIME
        , CONVERT(DECIMAL(38,20), DOWNTIME_ORG) * 60    AS DOWNTIME_ORIGINAL
        , CONVERT(DATETIME2, CHANGEDATE)                AS DOWNTIME_DATETIME
        , CONVERT(DATETIME2, CHANGEDATE_ORG)			AS DOWNTIME_DATETIME_ORIGINAL
        , CONVERT(nvarchar(100), AST.DESCRIPTION)     	AS [NAME]
        , AS_ST.IS_SPLIT								AS IS_SPLIT
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
            CONCAT(ASSETSTATUSID, '~', AS_ST.ASSETNUM,
                    '~', AS_ST.IS_SPLIT)
        )                                               AS W_INTEGRATION_ID
        , 'N'                                           AS W_DELETE_FLG
        , 'N' 											AS W_UPDATE_FLG
        , 8                                             AS W_DATASOURCE_NUM_ID
        , DATEADD(HH, 7, GETDATE())                     AS W_INSERT_DT
        , DATEADD(HH, 7, GETDATE())                     AS W_UPDATE_DT
        , @p_batch_id                                   AS W_BATCH_ID
    INTO #W_CMMS_DOWNTIME_F_tmp
    FROM [FND].[W_CMMS_ASSET_STATUS_F] AS_ST
        LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PLANT ON 1=1
            AND PLANT.PLANT_NAME_2 = LEFT(AS_ST.LOCATION, 3)
            AND PLANT.STO_LOC = ''
        LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
            AND LOC.[LOCATION] = LEFT(AS_ST.[LOCATION], 7)
        LEFT JOIN [TMP_ASSET] AST ON 1=1
            AND AST.ASSET_UID = AS_ST.ASSET_UID
        LEFT JOIN [dbo].[W_EXCEL_SPP_LINE_CATEGORY_F] LINE_CAT ON 1=1 AND LINE_CAT.LINE_ASSET_NUM = AST.LINE_ASSET_NUM 
            AND LINE_CAT.FROM_DATE <= CONVERT(DATETIME2, CHANGEDATE) AND LINE_CAT.TO_DATE >= CONVERT(DATETIME2, CHANGEDATE)
;

select DISTINCT LINE_ASSET_NUM from #W_CMMS_DOWNTIME_F_tmp;