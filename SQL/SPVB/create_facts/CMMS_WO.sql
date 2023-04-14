DECLARE @p_batch_id NVARCHAR(8) = '20230220';

SELECT
    ISNULL(PLANT.PLANT_WID, 0)                              AS PLANT_WID
    , ISNULL(AST.LOCATION_WID, 0)                           AS LOC_WID
    , CASE WHEN SCHEDFINISH IS NULL OR SCHEDFINISH = ''
        THEN 0
        ELSE FORMAT(
            CONVERT(DATETIME2, SCHEDFINISH, 103), 
            'yyyyMMdd'
        )
    END                                                     AS DATE_WID
    , ISNULL(AST.ASSET_WID, 0)                              AS ASSET_WID

    , CONVERT(NVARCHAR(30), WO.WORKORDER_ID)                AS WORKORDER_ID
    , CONVERT(NVARCHAR(30), WO.WONUM)                       AS WORK_ORDERS
    , CONVERT(NVARCHAR(100), WO.[DESCRIPTION])              AS [DESCRIPTION]
    , CONVERT(nvarchar(5), WORKTYPE)                        AS [TYPE]
    , CASE WHEN SPVB_OVERHAUL = '0'
        THEN 'N' ELSE 'Y' END                               AS OVERHAUL
    , CONVERT(NVARCHAR(50), PMNUM)                          AS PM
    , CONVERT(NVARCHAR(50), JPNUM)                          AS JOB_PLAN
    , CONVERT(NVARCHAR(1000), WO.[DESCRIPTION])             AS JOB_DESCRIPTION
    , CONVERT(NVARCHAR(10), WO.[SITE_ID])                   AS [SITE]
    , CONVERT(NVARCHAR(50), WO.[LOCATION])                  AS [LOCATION]
    , CONVERT(NVARCHAR(30), ASSETNUM)                       AS ASSET_NUM
    , CONVERT(NVARCHAR(10), WO.[STATUS])                    AS [STATUS]
    , CONVERT(NVARCHAR(10), [SUPERVISOR])                   AS [SUPERVISOR]
    , CONVERT(NVARCHAR(100), SUPPERVISORNAME, 103)          AS SUPERVISOR_NAME
    , CASE WHEN REPORTDATE IS NULL OR REPORTDATE = ''
        THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, REPORTDATE, 103)
    END                                                     AS DATE_CREATION
    , CASE WHEN TARGSTARTDATE IS NULL OR TARGSTARTDATE = ''
        THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, TARGSTARTDATE, 103)
    END                                                     AS DATE_TARGET_START
    , CASE WHEN SCHEDSTART IS NULL OR SCHEDSTART = ''
        THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, SCHEDSTART, 103)
    END                                                     AS DATE_SCHEDULE_START
    , CASE WHEN TARGCOMPDATE IS NULL OR TARGCOMPDATE = ''
        THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, TARGCOMPDATE, 103)
    END                                                     AS DATE_TARGET_FINISH
    , CASE WHEN SCHEDFINISH IS NULL OR SCHEDFINISH = ''
        THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, SCHEDFINISH, 103)
    END                                                     AS DATE_SCHEDULE_FINISH
    , CONVERT(NVARCHAR(10), WO.PARENT)                	    AS PARENT
    , 0                                                     AS IS_SCHED  -- NOTE: Hiện tại đang chờ logic của Avenue cho phần resched

    CASE WHEN WO_S_WSCH.CHANGEDATE IS NULL 
        OR WO_S_WSCH.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_WSCH.CHANGEDATE, 103)
    END                                                     AS DATE_WSCH
    , CASE WHEN WO_S_APPRV.CHANGEDATE IS NULL 
        OR WO_S_APPRV.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_APPRV.CHANGEDATE, 103)
    END                                                     AS DATE_APPROVED
    , CASE WHEN WO_S_PLN.CHANGEDATE IS NULL 
        OR WO_S_PLN.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_PLN.CHANGEDATE, 103)
    END                                                     AS DATE_PLANNING
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
    , CASE WHEN WO_S_REJ.CHANGEDATE IS NULL 
        OR WO_S_REJ.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_REJ.CHANGEDATE, 103)
    END                                                     AS DATE_REJECTED
    , CASE WHEN WO_S_WAP.CHANGEDATE IS NULL 
        OR WO_S_WAP.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_WAP.CHANGEDATE, 103)
    END                                                     AS DATE_WAIT_APPR
    , CASE WHEN WO_S_INPRG.CHANGEDATE IS NULL 
        OR WO_S_INPRG.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_INPRG.CHANGEDATE, 103)
    END                                                     AS DATE_INPROGRESS
    , CASE WHEN WO_S_CANC.CHANGEDATE IS NULL 
        OR WO_S_CANC.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_CANC.CHANGEDATE, 103)
    END                                                     AS DATE_CANCEL
    , CASE WHEN WO_S_CLOSE.CHANGEDATE IS NULL 
        OR WO_S_CLOSE.CHANGEDATE = '' THEN NULL
        ELSE CONVERT(DATETIMEOFFSET, WO_S_CLOSE.CHANGEDATE, 103)
    END                                                     AS DATE_CLOSED

    , CONVERT(
        NVARCHAR(300), 
        CONCAT(WO.WONUM, '~', WO.WORKORDER_ID)
    )                                                       AS W_INTEGRATION_ID
    , 'N'                                                   AS W_DELETE_FLG
    , 'N' 											        AS W_UPDATE_FLG
    , 8                                                     AS W_DATASOURCE_NUM_ID
    , DATEADD(HH, 7, GETDATE())                             AS W_INSERT_DT
    , DATEADD(HH, 7, GETDATE())                             AS W_UPDATE_DT
    , @p_batch_id                                           AS W_BATCH_ID
INTO #W_CMMS_WO_F_tmp
FROM [FND].[W_CMMS_WO_F] WO
LEFT JOIN (SELECT DISTINCT [CMMS Plant (SIDEID)] as [SITE_ID],[SAP Plant (WERKS)] AS PLANT_CODE FROM [STG].[W_EXCEL_SLOC_MAPPING_CMMS_VS_SAP_DS]) MP
    ON MP.SITE_ID = WO.[SITE_ID]
    LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PLANT ON 1=1
        AND PLANT.PLANT = MP.PLANT_CODE 
        AND STo_LOC = ''
    LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC_X ON 1=1
        AND LOC_X.[LOCATION] = LEFT(WO.[LOCATION], 7)
    LEFT JOIN [dbo].[W_CMMS_ASSET_D] AST ON 1=1
        AND AST.[ASSET_NUM] = WO.[ASSETNUM]
        AND LEFT(AST.LOCATION, 3) = LEFT(WO.LOCATION, 3)
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_WSCH
        WHERE 1=1
            AND TMP_WSCH.WORKORDER_ID = WO.WORKORDER_ID
            AND TMP_WSCH.STATUS = 'WSCH'
        ORDER BY WOSTATUS_ID DESC
    ) WO_S_WSCH
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_PLN
        WHERE 1=1
            AND TMP_PLN.WORKORDER_ID = WO.WORKORDER_ID
            AND TMP_PLN.STATUS = 'PLANNING'
        ORDER BY WOSTATUS_ID DESC
    ) WO_S_PLN
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_APPRV
        WHERE 1=1
            AND TMP_APPRV.WORKORDER_ID = WO.WORKORDER_ID
            AND TMP_APPRV.STATUS = 'APPR'
        ORDER BY WOSTATUS_ID DESC
    ) WO_S_APPRV
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_FINSH
        WHERE 1=1
            AND TMP_FINSH.WORKORDER_ID = WO.WORKORDER_ID
            AND TMP_FINSH.STATUS = 'FINISHED'
        ORDER BY WOSTATUS_ID DESC
    ) WO_S_FINSH
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_CPLT
        WHERE 1=1
            AND TMP_CPLT.WORKORDER_ID = WO.WORKORDER_ID
            AND TMP_CPLT.STATUS = 'COMPLETED'
        ORDER BY WOSTATUS_ID DESC
    ) WO_S_CPLT
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_COMP
        WHERE 1=1
            AND TMP_COMP.WORKORDER_ID = WO.WORKORDER_ID
            AND TMP_COMP.STATUS = 'COMP'
        ORDER BY WOSTATUS_ID DESC
    ) WO_S_COMP
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_COMP
        WHERE 1=1
            AND TMP_COMP.WORKORDER_ID = WO.WORKORDER_ID
            AND TMP_COMP.STATUS = 'REJECTED'
        ORDER BY WOSTATUS_ID DESC
    ) WO_S_REJ
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_COMP
        WHERE 1=1
            AND TMP_COMP.WORKORDER_ID = WO.WORKORDER_ID
            AND TMP_COMP.STATUS = 'WAPPR'
        ORDER BY WOSTATUS_ID DESC
    ) WO_S_WAP
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_COMP
        WHERE 1=1
            AND TMP_COMP.WORKORDER_ID = WO.WORKORDER_ID
            AND TMP_COMP.STATUS = 'INPRG'
        ORDER BY WOSTATUS_ID DESC
    ) WO_S_INPRG
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_COMP
        WHERE 1=1
            AND TMP_COMP.WORKORDER_ID = WO.WORKORDER_ID
            AND TMP_COMP.STATUS = 'CAN'
        ORDER BY WOSTATUS_ID DESC
    ) WO_S_CANC
    OUTER APPLY ( 
        SELECT TOP 1 * FROM [FND].[W_CMMS_WO_STATUS_D] TMP_COMP
        WHERE 1=1
            AND TMP_COMP.WORKORDER_ID = WO.WORKORDER_ID
            AND TMP_COMP.STATUS = 'CLOSE'
        ORDER BY WOSTATUS_ID DESC
    ) WO_S_CLOSE

WHERE 1=1
    AND wo.ISTASK = 0
    AND wo.WORKTYPE IN ('PM', 'CM')
    -- and WO.WONUM = 'WO4000188909'
    AND WO.WORKORDER_ID = 2173229
;

-- SELECT * FROM [dbo].[W_CMMS_ASSET_D]
-- WHERE ASSET_NUM = '170072010000'

-- -- DROP TABLE #W_CMMS_WO_F_tmp;
-- select top 30 * from #W_CMMS_WO_F_tmp;
-- SELECT * FROM FND.W_CMMS_WO_F
-- WHERE 1=1
--     AND WONUM = 'WO7000043279'

-- SELECT count(*)
-- FROM [FND].[W_CMMS_WO_F] WO
-- WHERE 1=1
--     AND wo.ISTASK = 0
--     AND wo.WORKTYPE IN ('PM', 'CM')
-- ;



-- SELECT TOP 10 * FROM [STG].[W_CMMS_WO_FS];
-- 01-06-2021 08:00:00

-- select SCHEDSTART
-- from [FND].[W_CMMS_WO_F] WO
-- where 1=1
--     AND try_cast(SCHEDSTART AS DATETIME2) is null
--     AND wo.ISTASK = 0
--     AND wo.WORKTYPE IN ('PM', 'CM')
-- ;
    
select top 50 * from #W_CMMS_WO_F_tmp where ASSET_NUM <> '' AND ASSET_WID = 0;
SELECT TOP 10 * FROM FND.W_CMMS_WO_F WHERE WONUM = 'WO6000034378'