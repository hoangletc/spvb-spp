SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
alter PROC [dbo].[CMMS_proc_load_w_spp_wo_f] @p_batch_id [bigint] AS 
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_CMMS_WO_F',
			@sql nvarchar(max),
	        @column_name varchar(4000),
	        @no_row bigint	,
			@v_batch_id bigint = @p_batch_id,
			@v_job_id bigint = null,
			@v_jobinstance_id bigint,
			@v_logtype_id bigint,
			@p_src_chk_column varchar(32),
			@p_tgt_chk_column varchar(32),
			@p_return_code bigint,
			@p_return_msg varchar(4000),
			@src_rownum	bigint,
			@tgt_rownum bigint,
			@tgt_chk_value float,
			@src_chk_value float,			
			@v_src_tablename varchar(100),
			@v_tgt_tablename varchar(100),
			@v_return_msg varchar(4000),
			@v_return_status varchar(100),
			@isExistSSAS char(1),
	        @isFullload char(1),
			@PartitionCol nvarchar(100),
			@v_message varchar(max),
			@p_error_code varchar(4000),
			@p_error_message varchar(4000),
			@frequencyPartition nvarchar(10),
			@p_g_job_status_running varchar(100),
			@p_g_job_status_success varchar(100),
			@p_g_job_status_failed  varchar(100),
			@p_g_job_status_aborted varchar(100),
			@p_job_status varchar(100) = 'SUCCESS'

    set @v_job_id= (
        select top 1
            JOB_ID
        from [dbo].[SAP_ETL_JOB]
        where 1=1 and ACTIVE_FLG='Y' and TGT_TABLE = @tgt_TableName
    )
    set @v_jobinstance_id = convert(
        bigint, 
        convert(varchar, @v_batch_id) + convert(varchar, @v_job_id)
    )
    set @v_src_tablename = (
        select top 1
            SRC_TABLE
        from [dbo].[SAP_ETL_JOB]
        where 1=1 
            and ACTIVE_FLG = 'Y' 
            and TGT_TABLE = @tgt_TableName
    )

    print '@v_jobinstance_id is ' + cast(@v_jobinstance_id as varchar)
    print '@v_src_tablename ' + @v_src_tablename

    execute	 [dbo].[SAP_proc_etl_util_start_job_instance]
		@p_tgt_table_name 	= @tgt_TableName,
		@p_batch_id 		= @v_batch_id,
		@p_job_instance_id  = @v_jobinstance_id OUTPUT,
		@p_src_chk_column 	= @p_src_chk_column OUTPUT,
		@p_tgt_chk_column 	= @p_tgt_chk_column OUTPUT,
		@p_return_code 		= @p_return_code OUTPUT,
		@p_return_msg 		= @p_return_msg OUTPUT


    BEGIN TRY
		-- 1. Check existence and remove of temp table
        PRINT '1. Check existence and remove of temp table'

        IF OBJECT_ID(N'tempdb..#W_CMMS_WO_F_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_CMMS_WO_F_tmp'
            DROP Table #W_CMMS_WO_F_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

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

			, CASE WHEN WO_S_WSCH.CHANGEDATE IS NULL 
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
			--and WO.WONUM = 'WO4000188909'
		;


		-- 3. Update main table using W_INTEGRATION_ID
		PRINT '3. Update main table using W_INTEGRATION_ID'

		-- 3.1. Mark existing records by flag 'Y'
		PRINT '3.1. Mark existing records by flag ''Y'''

		UPDATE #W_CMMS_WO_F_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_CMMS_WO_F_tmp tg
		INNER JOIN [dbo].[W_CMMS_WO_F] sc 
		ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

		-- 3.2. Start updating
		PRINT '3.2. Start updating'

		UPDATE [dbo].[W_CMMS_WO_F]
		SET 
			PLANT_WID = src.PLANT_WID
			, LOC_WID = src.LOC_WID
			, DATE_WID = src.DATE_WID
			, ASSET_WID = src.ASSET_WID

			, WORKORDER_ID = src.WORKORDER_ID
			, WORK_ORDERS = src.WORK_ORDERS
			, [DESCRIPTION] = src.DESCRIPTION
			, [TYPE] = src.TYPE
			, OVERHAUL = src.OVERHAUL
			, PM = src.PM
			, JOB_PLAN = src.JOB_PLAN
			, [SITE] = src.SITE
			, [LOCATION] = src.LOCATION
			, ASSET_NUM = src.ASSET_NUM
			, [STATUS] = src.STATUS
			, SUPERVISOR = src.SUPERVISOR
			, DATE_CREATION = src.DATE_CREATION
			, DATE_TARGET_START = src.DATE_TARGET_START
			, DATE_TARGET_FINISH = src.DATE_TARGET_FINISH
			, DATE_SCHEDULE_START = src.DATE_SCHEDULE_START
			, DATE_SCHEDULE_FINISH = src.DATE_SCHEDULE_FINISH
			, PARENT = src.PARENT
			, IS_SCHED = src.IS_SCHED

			, DATE_WSCH = src.DATE_WSCH
			, DATE_PLANNING = src.DATE_PLANNING
			, DATE_APPROVED = src.DATE_APPROVED
			, DATE_FINISHED = src.DATE_FINISHED
			, DATE_ACCEPTED = src.DATE_ACCEPTED
			, DATE_COMPLETED = src.DATE_COMPLETED
			, DATE_REJECTED = src.DATE_REJECTED
			, DATE_WAIT_APPR = src.DATE_WAIT_APPR
			, DATE_INPROGRESS = src.DATE_INPROGRESS
			, DATE_CANCEL = src.DATE_CANCEL
			, DATE_CLOSED = src.DATE_CLOSED

	

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = DATEADD(HH, 7, GETDATE())
		FROM [dbo].[W_CMMS_WO_F] tgt
		INNER JOIN #W_CMMS_WO_F_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


		-- 4. Insert non-existed records to main table from temp table
		PRINT '4. Insert non-existed records to main table from temp table'

		INSERT INTO [dbo].[W_CMMS_WO_F](
			PLANT_WID
			, LOC_WID
			, DATE_WID
			, ASSET_WID

			, WORKORDER_ID
			, WORK_ORDERS
			, [DESCRIPTION]
			, [TYPE]
			, OVERHAUL
			, PM
			, JOB_PLAN
			, [SITE]
			, [LOCATION]
			, ASSET_NUM
			, [STATUS]
			, SUPERVISOR
			, DATE_CREATION
			, DATE_TARGET_START
			, DATE_TARGET_FINISH
			, DATE_SCHEDULE_START
			, DATE_SCHEDULE_FINISH
			, PARENT
			, IS_SCHED

			, DATE_WSCH
			, DATE_PLANNING
			, DATE_APPROVED
			, DATE_FINISHED
			, DATE_ACCEPTED
			, DATE_COMPLETED
			, DATE_REJECTED
			, DATE_WAIT_APPR
			, DATE_INPROGRESS
			, DATE_CANCEL
			, DATE_CLOSED

			, W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
		)
		SELECT
			PLANT_WID
			, LOC_WID
			, DATE_WID
			, ASSET_WID

			, WORKORDER_ID
			, WORK_ORDERS
			, [DESCRIPTION]
			, [TYPE]
			, OVERHAUL
			, PM
			, JOB_PLAN
			, [SITE]
			, [LOCATION]
			, ASSET_NUM
			, [STATUS]
			, SUPERVISOR
			, DATE_CREATION
			, DATE_TARGET_START
			, DATE_TARGET_FINISH
			, DATE_SCHEDULE_START
			, DATE_SCHEDULE_FINISH
			, PARENT
			, IS_SCHED

			, DATE_WSCH
			, DATE_PLANNING
			, DATE_APPROVED
			, DATE_FINISHED
			, DATE_ACCEPTED
			, DATE_COMPLETED
			, DATE_REJECTED
			, DATE_WAIT_APPR
			, DATE_INPROGRESS
			, DATE_CANCEL
			, DATE_CLOSED

			, W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
		FROM #W_CMMS_WO_F_tmp
		where W_UPDATE_FLG = 'N'
		

		/*delete & re-insert data refresh*/
		DELETE FROM [dbo].[SAP_ETL_DATAFRESH_CONF] WHERE UPPER(TABLE_NAME) = UPPER(@tgt_TableName)

		INSERT INTO [dbo].[SAP_ETL_DATAFRESH_CONF]
        (
            TABLE_NAME, 
            REFRESH_DATE, 
            IS_FULLLOAD, 
            IS_EXIST_SSAS, 
            LAST_UPDATE_DATE
        )
        SELECT DISTINCT 
            @tgt_TableName, 
            NULL, 
            'Y', 
            'Y', 
            DATEADD(HH, 7, GETDATE())
        FROM (
            SELECT *
            FROM W_CMMS_WO_F
        ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

		SET @src_rownum = ( SELECT COUNT(1) FROM #W_CMMS_WO_F_tmp );
		SET @tgt_rownum = ( 
            SELECT 
                COUNT(DISTINCT W_INTEGRATION_ID)
            FROM W_CMMS_WO_F
            WHERE 1=1
                AND W_DELETE_FLG = 'N' 
                AND W_BATCH_ID = @p_batch_id
        );

	END TRY


	BEGIN CATCH
		set @p_job_status = 'FAILED'
		set @p_error_message = ERROR_MESSAGE()
		print @p_error_message
	END CATCH

    execute	[dbo].[SAP_proc_etl_util_end_job_instance]
			@p_job_instance_id 	= @v_jobinstance_id,
			@p_return_code 		= @p_return_code OUTPUT,
			@p_return_msg 		= @p_return_msg OUTPUT,
			@p_status_code 		= @p_job_status,
			@p_error_message 	= @p_error_message,
			@src_rownum 		= @src_rownum,
			@tgt_rownum 		= @tgt_rownum,
			@src_chk_value 		= @src_chk_value,
			@tgt_chk_value 		= @tgt_chk_value
END
GO
