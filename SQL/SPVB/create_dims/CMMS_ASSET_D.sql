DECLARE @p_batch_id NVARCHAR(8) = '20230321';

WITH TMP AS (
    SELECT
        ASSET_UID
        , COUNT(ASSET_UID)          AS NUM_ANCESTOR
    FROM FND.W_CMMS_ASSET_ANCESTOR_D
    GROUP BY ASSET_UID
), TMP_AST_HIER_TYPE AS (
    SELECT
        ASSET_UID
        , CASE WHEN NUM_ANCESTOR = 1 THEN 'line'
            WHEN NUM_ANCESTOR = 2 THEN 'machine'
            WHEN NUM_ANCESTOR = 3 THEN 'component'
            WHEN NUM_ANCESTOR = 4 THEN 'sub_component1'
            WHEN NUM_ANCESTOR = 5 THEN 'sub_component2'
        END     AS ASSET_HIERACHICAL_TYPE
    FROM TMP
), TMP_AST_LINE_MACHINE AS (
    SELECT
        F.ASSET_UID
        , A_LINE.ANCESTOR                                   AS LINE_ASSET_NUM
        , A_MACHINE.ANCESTOR                                AS MACHINE_ASSET_NUM
        , A_LINE_INFO.[DESCRIPTION]                         AS LINE_DESC
        , A_MACHINE_INFO.[DESCRIPTION]                      AS MACHINE_DESC
        -- , CONVERT(NVARCHAR(100), 
        --         LOWER(A_MACHINE_INFO.[DESCRIPTION]), 100)   AS TMP_M_DESC
    FROM FND.W_CMMS_ASSET_D F
        LEFT JOIN TMP ON 1=1
            AND TMP.ASSET_UID = F.ASSET_UID
        LEFT JOIN FND.W_CMMS_ASSET_ANCESTOR_D A_LINE ON 1=1
            AND A_LINE.ASSET_UID = F.ASSET_UID
            AND A_LINE.HIERARCHY_LEVELS = TMP.NUM_ANCESTOR - 1
        LEFT JOIN FND.W_CMMS_ASSET_ANCESTOR_D A_MACHINE ON 1=1
            AND A_MACHINE.ASSET_UID = F.ASSET_UID
            AND A_MACHINE.HIERARCHY_LEVELS = TMP.NUM_ANCESTOR - 2
        LEFT JOIN FND.W_CMMS_ASSET_D A_LINE_INFO ON 1=1
            AND A_LINE_INFO.ASSET_NUM = A_LINE.ANCESTOR
            AND A_LINE_INFO.SITE_ID = F.SITE_ID
        LEFT JOIN FND.W_CMMS_ASSET_D A_MACHINE_INFO ON 1=1
            AND A_MACHINE_INFO.ASSET_NUM = A_MACHINE.ANCESTOR
            AND A_MACHINE_INFO.SITE_ID = F.SITE_ID

)
    SELECT
        ISNULL(LOC_X.LOC_WID, 0)                                    AS LOCATION_WID

        , CONVERT(nvarchar(50), F.ASSET_UID)                        AS ASSET_UID
        , CONVERT(nvarchar(50), F.ASSET_NUM)                        AS ASSET_NUM
        , CONVERT(nvarchar(50), F.ANCESTOR)                         AS ANCESTOR
        , CONVERT(nvarchar(50), F.[LOCATION])                       AS [LOCATION]
        , CONVERT(nvarchar(50), F.SITE_ID)                          AS SITE_ID
        , CONVERT(nvarchar(1000), F.[DESCRIPTION])                  AS [DESCRIPTION]
        , CONVERT(varchar, A_HIER.ASSET_HIERACHICAL_TYPE)           AS ASSET_HIERACHICAL_TYPE

        , CONVERT(nvarchar(50), A_LM.LINE_ASSET_NUM)                AS LINE_ASSET_NUM
        , CONVERT(nvarchar(1000), A_LM.LINE_DESC)                   AS LINE_ASSET_DESCRIPTION
        , CONVERT(nvarchar(50), A_LM.MACHINE_ASSET_NUM)             AS MACHINE_ASSET_NUM
        , CONVERT(nvarchar(1000), A_LM.MACHINE_DESC)                AS MACHINE_ASSET_DESCRIPTION
        , CASE 
            WHEN SUBSTRING(F.[LOCATION], 5, 3) = 'B02' THEN 'Building'
            WHEN SUBSTRING(F.[LOCATION], 5, 3) = 'CIP' THEN 'CIP'
            WHEN SUBSTRING(F.[LOCATION], 5, 3) = 'S02' THEN 'Sugar'
            WHEN SUBSTRING(F.[LOCATION], 5, 3) = 'S03' THEN 'Syrup'
            WHEN SUBSTRING(F.[LOCATION], 5, 3) = 'U03' THEN 'Utilities'
            WHEN SUBSTRING(F.[LOCATION], 5, 3) = 'W03' THEN 'Wastewater'
            WHEN SUBSTRING(F.[LOCATION], 5, 3) = 'W04' THEN 'Water treatment'
            WHEN SUBSTRING(F.[LOCATION], 5, 3) = 'W05' THEN 'Workshop'
            WHEN SUBSTRING(F.[LOCATION], 5, 3) = 'W06' THEN 'Warehouse'
            WHEN SUBSTRING(F.[LOCATION], 5, 3) = 'Q01' THEN 'QC'
            WHEN CHARINDEX('máy', A_LM.MACHINE_DESC) > 0 AND CHARINDEX('line', A_LM.MACHINE_DESC) > CHARINDEX('máy', A_LM.MACHINE_DESC)
                THEN TRIM(SUBSTRING(
                    A_LM.MACHINE_DESC
                    , CHARINDEX('máy', A_LM.MACHINE_DESC) + 4
                    , CHARINDEX('line', A_LM.MACHINE_DESC) - CHARINDEX('máy', A_LM.MACHINE_DESC) - 4
                ))
            WHEN CHARINDEX('line', A_LM.MACHINE_DESC) > 1 
                THEN TRIM(LEFT(A_LM.MACHINE_DESC, CHARINDEX('line', A_LM.MACHINE_DESC) - 2))
            ELSE ''
        END                                                         AS [MACHINE_SHORT_NAME]

        , CONVERT(nvarchar(50), F.ASSET_TYPE)                       AS ASSET_TYPE
        , CONVERT(nvarchar(500), F.SPVB_COSTCENTER_DESCRIPTION)     AS SPVB_COSTCENTER_DESCRIPTION
        , CONVERT(DECIMAL(38, 20), F.INV_COST)                      AS INV_COST
        , CONVERT(INT, IS_RUNNING)                                  AS IS_RUNNING
        , CONVERT(nvarchar(50), F.SPVB_COSTCENTER)                  AS SPVB_COSTCENTER
        , CONVERT(nvarchar(50), F.CHANGE_DATE)                      AS CHANGEDATE
        , CONVERT(nvarchar(50), F.SPVB_FIXEDASSETNUM)               AS SPVB_FIXEDASSETNUM
        , CONVERT(nvarchar(50), F.TOTAL_COST)                       AS TOTAL_COST
        , CONVERT(nvarchar(50), F.[STATUS_DESCRIPTION])             AS STATUS_DESCRIPTION
        , CONVERT(nvarchar(50), F.TOTAL_DOWNTIME)                   AS TOTAL_DOWNTIME

        , CONCAT(F.ASSET_UID, '~', F.ASSET_NUM, '~', F.SITE_ID)     AS W_INTEGRATION_ID
        , 'N'                                                       AS W_DELETE_FLG
        , 'N' 											            AS W_UPDATE_FLG
        , 8                                                         AS W_DATASOURCE_NUM_ID
        , DATEADD(HH, 7, GETDATE())                                 AS W_INSERT_DT
        , DATEADD(HH, 7, GETDATE())                                 AS W_UPDATE_DT
        , @p_batch_id                                               AS W_BATCH_ID
    INTO #W_CMMS_ASSET_D_tmp
    FROM FND.W_CMMS_ASSET_D F
        LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC_X ON 1=1
            AND LOC_X.[LOCATION] = LEFT(F.[LOCATION], 7)
        LEFT JOIN TMP_AST_HIER_TYPE A_HIER ON 1=1
            AND A_HIER.ASSET_UID = F.ASSET_UID
        LEFT JOIN TMP_AST_LINE_MACHINE A_LM ON 1=1
            AND A_LM.ASSET_UID = F.ASSET_UID
;

-- select distinct top 1000 MACHINE_SHORT_NAME from #W_CMMS_ASSET_D_tmp;
select * from #W_CMMS_ASSET_D_tmp where MACHINE_SHORT_NAME = 'Băng tải'
select top 1000 * from #W_CMMS_ASSET_D_tmp;