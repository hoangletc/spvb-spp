DECLARE @p_batch_id NVARCHAR(8) = '20230220';

SELECT
    ISNULL(PLANT.PLANT_WID, 0)                              AS PLANT_WID
    , ISNULL(AST.LOCATION_WID, 0)                           AS LOC_WID
    , CASE WHEN WSCHEDSTART IS NULL OR WSCHEDSTART = ''
        THEN 0
        ELSE FORMAT(
            CONVERT(DATETIME2, WSCHEDSTART, 103), 
            'yyyyMMdd'
        )
    END                                                     AS DATE_WID
    , ISNULL(AST.ASSET_WID, 0)                              AS ASSET_WID

    , CONVERT(NVARCHAR(30), WO.WONUM)                       AS WORK_ORDERS
    , CONVERT(NVARCHAR(100), WO.[DESCRIPTION])              AS [DESCRIPTION]
    , CONVERT(nvarchar(5), WORKTYPE)                        AS [TYPE]
    , CASE WHEN SPVB_OVERHAUL = 'False'
        THEN 'N' ELSE 'Y' END                               AS OVERHAUL
    , CONVERT(NVARCHAR(50), PMNUM)                          AS PM
    , CONVERT(NVARCHAR(50), JPNUM)                          AS JOB_PLAN
    -- , CONVERT(NVARCHAR( 5), SITEID)                         AS [SITE]
    , CONVERT(NVARCHAR(1000), WO.[DESCRIPTION])             AS JOB_DESCRIPTION
    , NULL                                                  AS [SITE]
    , CONVERT(NVARCHAR(50), WO.[LOCATION])                  AS [LOCATION]
    , CONVERT(NVARCHAR(30), ASSETNUM)                       AS ASSET_NUM
    , CONVERT(NVARCHAR(10), WO.[STATUS])                    AS [STATUS]
    , CONVERT(NVARCHAR(10), [SUPERVISOR])                   AS [SUPERVISOR]
    , CONVERT(NVARCHAR(100), SUPPERVISORNAME, 103)          AS SUPERVISOR_NAME
    , CONVERT(DATETIMEOFFSET, REPORTDATE, 103)              AS DATE_CREATION
    , CONVERT(DATETIMEOFFSET, TARGSTARTDATE, 103)           AS DATE_TARGET_START
    , CONVERT(DATETIMEOFFSET, SCHEDFINISH, 103)             AS DATE_TARGET_FINISH
    , CONVERT(DATETIMEOFFSET, WSCHEDSTART, 103)             AS DATE_SCHEDULE_START
    , CONVERT(DATETIMEOFFSET, SCHEDFINISH, 103)             AS DATE_SCHEDULE_FINISH
    , CONVERT(NVARCHAR(10), WO.PARENT)                	    AS PARENT
    , 0                                                     AS IS_SCHED  -- NOTE: Hiện tại đang chờ logic của Avenue cho phần resched

    , CASE WHEN WO_S_WSCH.CHANGEDATE IS NULL 
        OR WO_S_WSCH.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_WSCH.CHANGEDATE, 103)
    END                                                     AS DATE_WSCH
    , CASE WHEN WO_S_APPRV.CHANGEDATE IS NULL 
        OR WO_S_APPRV.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_APPRV.CHANGEDATE, 103)
    END                                                     AS DATE_PLANNING
    , CASE WHEN WO_S_APPRV.CHANGEDATE IS NULL 
        OR WO_S_APPRV.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_APPRV.CHANGEDATE, 103)
    END                                                     AS DATE_APPROVED
    , CASE WHEN WO_S_FINSH.CHANGEDATE IS NULL 
        OR WO_S_FINSH.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_FINSH.CHANGEDATE, 103)
    END                                                     AS DATE_FINISHED
    , CASE WHEN WO_S_CPLT.CHANGEDATE IS NULL 
        OR WO_S_CPLT.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_CPLT.CHANGEDATE, 103)
    END                                                     AS DATE_ACCEPTED
    , CASE WHEN WO_S_COMP.CHANGEDATE IS NULL 
        OR WO_S_COMP.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_COMP.CHANGEDATE, 103)
    END                                                     AS DATE_COMPLETED

    , CONVERT(
        NVARCHAR(300), 
        CONCAT(WO.WONUM, '~', ASSETNUM, '~',
                PMNUM, '~', SUPERVISOR, '~', JPNUM)
    )                                                       AS W_INTEGRATION_ID
    , 'N'                                                   AS W_DELETE_FLG
    , 'N' 											        AS W_UPDATE_FLG
    , 8                                                     AS W_DATASOURCE_NUM_ID
    , DATEADD(HH, 7, GETDATE())                             AS W_INSERT_DT
    , DATEADD(HH, 7, GETDATE())                             AS W_UPDATE_DT
    , @p_batch_id                                           AS WATCH_ID
INTO #W_CMMS_WO_F_tmp
FROM [FND].[W_CMMS_WO_F] WO
    LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PLANT ON 1=1
        AND PLANT.PLANT_NAME_2 = LEFT(WO.LOCATION, 3) 
        AND STo_LOC = ''
    LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC_X ON 1=1
        AND LOC_X.[LOCATION] = LEFT(WO.[LOCATION], 7)
    LEFT JOIN [dbo].[W_CMMS_ASSET_D] AST ON 1=1
        AND AST.[ASSET_NUM] = WO.[ASSETNUM]
        AND LEFT(AST.LOCATION, 3) = LEFT(WO.LOCATION, 3)
    
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_WSCH
        WHERE 1=1
            AND TMP_WSCH.WONUM = WO.WONUM
            
            AND TMP_WSCH.STATUS = 'WSCH'
        ORDER BY CHANGE_DATE DESC
    ) WO_S_WSCH
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_PLN
        WHERE 1=1
            AND TMP_PLN.WONUM = WO.WONUM
            AND TMP_PLN.GLACCOUNT = WO.GLACCOUNT
            AND TMP_PLN.STATUS = 'PLANNING'
        ORDER BY CHANGE_DATE DESC
    ) WO_S_PLN
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_APPRV
        WHERE 1=1
            AND TMP_APPRV.WONUM = WO.WONUM
            AND TMP_APPRV.GLACCOUNT = WO.GLACCOUNT
            AND TMP_APPRV.STATUS = 'APPR'
        ORDER BY CHANGE_DATE DESC
    ) WO_S_APPRV
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_FINSH
        WHERE 1=1
            AND TMP_FINSH.WONUM = WO.WONUM
            AND TMP_FINSH.GLACCOUNT = WO.GLACCOUNT
            AND TMP_FINSH.STATUS = 'FINISHED'
        ORDER BY CHANGE_DATE DESC
    ) WO_S_FINSH
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_CPLT
        WHERE 1=1
            AND TMP_CPLT.WONUM = WO.WONUM
            AND TMP_CPLT.GLACCOUNT = WO.GLACCOUNT
            AND TMP_CPLT.STATUS = 'COMPLETED'
        ORDER BY CHANGE_DATE DESC
    ) WO_S_CPLT
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_COMP
        WHERE 1=1
            AND TMP_COMP.WONUM = WO.WONUM
            AND TMP_COMP.GLACCOUNT = WO.GLACCOUNT
            AND TMP_COMP.STATUS = 'COMP'
        ORDER BY CHANGE_DATE DESC
    ) WO_S_COMP

WHERE 1=1
    AND wo.ISTASK = 0
    AND wo.WORKTYPE IN ('PM', 'CM')
;


SELECT * FROM [dbo].[W_CMMS_ASSET_D]
WHERE ASSET_NUM = '170072010000'

-- DROP TABLE #W_CMMS_WO_F_tmp;
select top 30 * from #W_CMMS_WO_F_tmp;
SELECT * FROM FND.W_CMMS_WO_F
WHERE 1=1
    AND WONUM = 'WO7000043279'

SELECT count(*)
FROM [FND].[W_CMMS_WO_F] WO
WHERE 1=1
    AND wo.ISTASK = 0
    AND wo.WORKTYPE IN ('PM', 'CM')
;

select count(*) from #W_CMMS_WO_F_tmp;


-- 01-06-2021 08:00:00

-- select WSCHEDSTART
-- from [FND].[W_CMMS_WO_F] WO
-- where 1=1
--     AND try_cast(WSCHEDSTART AS DATETIME2) is null
--     AND wo.ISTASK = 0
--     AND wo.WORKTYPE IN ('PM', 'CM')
-- ;
    