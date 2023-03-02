DECLARE @p_batch_id NVARCHAR(8) = '20230220';

SELECT
    ISNULL(LOC_X.LOC_WID, 0)                                    AS LOCATION_WID

    , CONVERT(nvarchar(50), AST.SPVB_COSTCENTER)                AS SPVB_COSTCENTER
    , CONVERT(nvarchar(50), AST.CHANGE_DATE)                    AS CHANGE_DATE
    , CONVERT(nvarchar(50), AST.SPVB_FIXEDASSETNUM)             AS SPVB_FIXEDASSETNUM
    , CONVERT(nvarchar(50), AST.TOTAL_COST)                     AS TOTAL_COST
    , CONVERT(nvarchar(50), AST.[STATUS])                       AS [STATUS]
    , CONVERT(nvarchar(50), AST.[STATUS_DESCRIPTION])           AS STATUS_DESCRIPTION
    , CONVERT(nvarchar(50), AST.TOTAL_DOWNTIME)                 AS TOTAL_DOWNTIME
    , CONVERT(nvarchar(50), AST.ASSET_NUM)                      AS ASSET_NUM
    , CONVERT(nvarchar(50), AST.ASSET_TYPE)                     AS ASSET_TYPE
    , CONVERT(nvarchar(500), AST.SPVB_COSTCENTER_DESCRIPTION)   AS SPVB_COSTCENTER_DESCRIPTION
    , CONVERT(DECIMAL(38, 20), AST.INV_COST)                    AS INV_COST
    , CASE WHEN AST.ISRUNNING = 'True' THEN 1 ELSE 0 END        AS IS_RUNNING
    , CONVERT(nvarchar(50), AST.[LOCATION])                     AS [LOCATION]
    , CONVERT(nvarchar(50), AST.SITE_ID)                        AS SITE_ID
    , CONVERT(nvarchar(50), AST.ASSET_HIERACHICAL_TYPE)         AS ASSET_HIERACHICAL_TYPE
    , CONVERT(nvarchar(50), AST.LINE_ASSET_NUM)                 AS LINE_ASSET_NUM
    , CONVERT(nvarchar(1000), AST2.[DESCRIPTION])               AS LINE_ASSET_DES
    , CONVERT(nvarchar(50), AST.MACHINE_ASSET_NUM)              AS MACHINE_ASSET_NUM
    , CONVERT(nvarchar(50), AST.COMPONENT_ASSET_NUM)            AS COMPONENT_ASSET_NUM
    , CONVERT(nvarchar(1000), AST.[DESCRIPTION])                AS [DESCRIPTION]
    , CASE WHEN AST.ASSET_HIERACHICAL_TYPE <> 'machine' THEN NULL
        WHEN SUBSTRING(AST.[LOCATION], 5, 3) = 'B02' THEN 'Building'
        WHEN SUBSTRING(AST.[LOCATION], 5, 3) = 'CIP' THEN 'CIP'
        WHEN SUBSTRING(AST.[LOCATION], 5, 3) = 'S02' THEN 'Sugar'
        WHEN SUBSTRING(AST.[LOCATION], 5, 3) = 'S03' THEN 'Syrup'
        WHEN SUBSTRING(AST.[LOCATION], 5, 3) = 'U03' THEN 'Utilities'
        WHEN SUBSTRING(AST.[LOCATION], 5, 3) = 'W03' THEN 'Wastewater'
        WHEN SUBSTRING(AST.[LOCATION], 5, 3) = 'W04' THEN 'Water treatment'
        WHEN SUBSTRING(AST.[LOCATION], 5, 3) = 'W05' THEN 'Workshop'
        WHEN SUBSTRING(AST.[LOCATION], 5, 3) = 'W06' THEN 'Warehouse'
        WHEN SUBSTRING(AST.[LOCATION], 5, 3) = 'Q01' THEN 'QC'
        WHEN CHARINDEX('Máy', AST.[DESCRIPTION]) = 1
            THEN (
                CASE WHEN CHARINDEX('Line', AST.[DESCRIPTION]) = 0
                THEN TRIM(SUBSTRING(AST.[DESCRIPTION], 5, LEN(AST.[DESCRIPTION]) - 3))
                ELSE TRIM(SUBSTRING(AST.[DESCRIPTION], 5, CHARINDEX('Line', AST.[DESCRIPTION])))
                END
            )
        WHEN CHARINDEX('Line', AST.[DESCRIPTION]) = 1
            THEN TRIM(SUBSTRING(AST.[DESCRIPTION], 6, CHARINDEX('Line', AST.[DESCRIPTION], 2) - 7))
            -- THEN NULL
        ELSE TRIM(SUBSTRING(AST.[DESCRIPTION], 0, CHARINDEX('Line', AST.[DESCRIPTION])))
        -- ELSE NULL
    END                                                         AS [MACHINE_SHORT_NAME]

    , CONVERT(
        nvarchar(200), 
        CONCAT(AST.ASSET_UID, '~', AST.SPVB_COSTCENTER, '~',
                AST.SPVB_FIXEDASSETNUM, '~', AST.[LOCATION])
    )                                                           AS W_INTEGRATION_ID
    , 'N'                                                       AS W_DELETE_FLG
    , 'N' 											            AS W_UPDATE_FLG
    , 8                                                         AS W_DATASOURCE_NUM_ID
    , DATEADD(HH, 7, GETDATE())                                 AS W_INSERT_DT
    , DATEADD(HH, 7, GETDATE())                                 AS W_UPDATE_DT
    , @p_batch_id                                               AS W_BATCH_ID
INTO #W_CMMS_ASSET_D_tmp
FROM [FND].[W_CMMS_ASSET_D] AST
    LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC_X ON 1=1
        AND LOC_X.[LOCATION] = LEFT(AST.[LOCATION], 7)
    OUTER APPLY (
        SELECT TOP 1 * FROM [FND].[W_CMMS_ASSET_D] AS AST_TMP
        WHERE 1=1
            AND AST_TMP.LINE_ASSET_NUM = AST.ASSET_NUM
            AND LEFT(AST_TMP.LOCATION, 3) = LEFT(AST.LOCATION, 3)
    ) AST2
;
