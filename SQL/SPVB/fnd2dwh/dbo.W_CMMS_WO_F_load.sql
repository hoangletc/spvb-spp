SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (OBJECT_ID('[dbo].[proc_load_w_cmms_wo_f]') is not null)
BEGIN
    DROP PROCEDURE [dbo].[proc_load_w_cmms_wo_f]
END;
GO

CREATE PROC [dbo].[proc_load_w_cmms_wo_f]
    @p_batch_id [bigint]
AS 
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
			ISNULL(PLANT.PLANT_WID, 0)                          AS PLANT_WID
			, ISNULL(AST.LOCATION_WID, 0)                       AS LOC_WID
			, FORMAT(
				CONVERT(DATETIME2, SCHEDSTART), 
				'yyyyMMdd'
			) 													AS DATE_WID
			, ISNULL(AST.ASSET_WID, 0)                          AS ASSET_WID

			, CONVERT(NVARCHAR(30), WONUM)                      AS WORK_ORDERS
			, CONVERT(NVARCHAR(100), WO.[DESCRIPTION])          AS [DESCRIPTION]
			, CONVERT(nvarchar(5), WORKTYPE)                    AS [TYPE]
			, CASE WHEN SPVB_OVERHAUL = 'False'
				THEN 'N' ELSE 'Y' END                           AS OVERHAUL
			, CONVERT(NVARCHAR(50), PMNUM)                      AS PM
			, CONVERT(NVARCHAR(50), JPNUM)                      AS JOB_PLAN
			, CONVERT(NVARCHAR( 5), SITEID)                     AS [SITE]
			, CONVERT(NVARCHAR(50), WO.[LOCATION])              AS [LOCATION]
			, CONVERT(NVARCHAR(30), ASSETNUM)                   AS ASSET_NUM
			, CONVERT(NVARCHAR(10), WO.[STATUS])                AS [STATUS]
			, CONVERT(NVARCHAR(10), [SUPERVISOR])               AS [SUPERVISOR]
			, CONVERT(DATETIMEOFFSET, REPORTDATE)               AS DATE_CREATION
			, CONVERT(DATETIMEOFFSET, TARGSTARTDATE)            AS DATE_TARGET_START
			, CONVERT(DATETIMEOFFSET, SCHEDFINISH)              AS DATE_TARGET_FINISH
			, CONVERT(DATETIMEOFFSET, SCHEDSTART)               AS DATE_SCHEDULE_START
			, CONVERT(DATETIMEOFFSET, SCHEDFINISH)              AS DATE_SCHEDULE_FINISH
			, NULL AS JOB_DESCRIPTION   -- NOTE: cần join với table JOBPLAN nhưng khôngv có
			, NULL AS SUPERVISOR_NAME   -- NOTE: Cần join với table Person nhưng không có table
			, CONVERT(NVARCHAR(10), WO.PARENT)                	AS PARENT
			, CONVERT(NVARCHAR(5), WO.IS_SCHED)					AS IS_SCHED

			, CONVERT(DATETIMEOFFSET, WO_S_WSCH_B.CHANGEDATE)       AS DATE_WSCH_BFR
			, CONVERT(DATETIMEOFFSET, WO_S_PLN_B.CHANGEDATE)        AS DATE_PLANNING_BFR
			, CONVERT(DATETIMEOFFSET, WO_S_APPRV_B.CHANGEDATE)      AS DATE_APPROVED_BFR
			, CONVERT(DATETIMEOFFSET, WO_S_FINSH_B.CHANGEDATE)      AS DATE_FINISHED_BFR
			, CONVERT(DATETIMEOFFSET, WO_S_CPLT_B.CHANGEDATE)       AS DATE_ACCEPTED_BFR
			, CONVERT(DATETIMEOFFSET, WO_S_COMP_B.CHANGEDATE)       AS DATE_COMPLETED_BFR

			, CASE WHEN WO.IS_SCHED = 'True'
				THEN CONVERT(DATETIMEOFFSET, WO_S_WSCH_A.CHANGEDATE)
				ELSE NULL
			END                                                     AS DATE_WSCH_AFT
			, CASE WHEN WO.IS_SCHED = 'True'
				THEN CONVERT(DATETIMEOFFSET, WO_S_PLN_A.CHANGEDATE)
				ELSE NULL
			END                                                     AS DATE_PLANNING_AFT
			, CASE WHEN WO.IS_SCHED = 'True'
				THEN CONVERT(DATETIMEOFFSET, WO_S_APPRV_A.CHANGEDATE)
				ELSE NULL
			END                                                     AS DATE_APPROVED_AFT
			, CASE WHEN WO.IS_SCHED = 'True'
				THEN CONVERT(DATETIMEOFFSET, WO_S_FINSH_A.CHANGEDATE)
				ELSE NULL
			END                                                     AS DATE_FINISHED_AFT
			, CASE WHEN WO.IS_SCHED = 'True'
				THEN CONVERT(DATETIMEOFFSET, WO_S_CPLT_A.CHANGEDATE)
				ELSE NULL
			END                                                     AS DATE_ACCEPTED_AFT
			, CASE WHEN WO.IS_SCHED = 'True'
				THEN CONVERT(DATETIMEOFFSET, WO_S_COMP_A.CHANGEDATE)
				ELSE NULL
			END                                                     AS DATE_COMPLETED_AFT

			, CONVERT(
				NVARCHAR(300), 
				CONCAT_WS('~', WONUM, ASSETNUM, 
						PMNUM, SUPERVISOR, JPNUM)
			)                                               AS W_INTEGRATION_ID
			, 'N'                                           AS W_DELETE_FLG
			, 'N' 											AS W_UPDATE_FLG
			, 1                                             AS W_DATASOURCE_NUM_ID
			, GETDATE()                                     AS W_INSERT_DT
			, GETDATE()                                     AS W_UPDATE_DT
			, NULL                                          AS W_BATCH_ID
		INTO #W_CMMS_WO_F_tmp
		FROM [FND].[W_CMMS_WO_F] WO
			LEFT JOIN [dbo].[W_PLANT_SAP_D] PLANT ON 1=1
				AND PLANT.PLANT_NAME_2 = LEFT(WO.LOCATION, 3)
			LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC_X ON 1=1
				AND LOC_X.[LOCATION] = LEFT(WO.[LOCATION], 7)
			LEFT JOIN [dbo].[W_CMMS_ASSET_D] AST ON 1=1
				AND AST.[ASSET_NUM] = WO.[ASSETNUM]

			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_S_WSCH_B ON 1=1
				AND WO_S_WSCH_B.PARENT = WO.WONUM
				AND WO_S_WSCH_B.STATUS = 'WSCH_BEFORE'
			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_S_PLN_B ON 1=1
				AND WO_S_PLN_B.PARENT = WO.WONUM
				AND WO_S_PLN_B.STATUS = 'PLANNING_BEFORE'
			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_S_APPRV_B ON 1=1
				AND WO_S_APPRV_B.PARENT = WO.WONUM
				AND WO_S_APPRV_B.STATUS = 'APPR_BEFORE'
			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_S_FINSH_B ON 1=1
				AND WO_S_FINSH_B.PARENT = WO.WONUM
				AND WO_S_FINSH_B.STATUS = 'FINISHED_BEFORE'
			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_S_CPLT_B ON 1=1
				AND WO_S_CPLT_B.PARENT = WO.WONUM
				AND WO_S_CPLT_B.STATUS = 'COMPLETED_BEFORE'
			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_S_COMP_B ON 1=1
				AND WO_S_COMP_B.PARENT = WO.WONUM
				AND WO_S_COMP_B.STATUS = 'COMP_BEFORE'

			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_S_WSCH_A ON 1=1
				AND WO_S_WSCH_A.PARENT = WO.WONUM
				AND WO_S_WSCH_A.STATUS = 'WSCH_AFTER'
			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_S_PLN_A ON 1=1
				AND WO_S_PLN_A.PARENT = WO.WONUM
				AND WO_S_PLN_A.STATUS = 'PLANNING_AFTER'
			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_S_APPRV_A ON 1=1
				AND WO_S_APPRV_A.PARENT = WO.WONUM
				AND WO_S_APPRV_A.STATUS = 'APPR_AFTER'
			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_S_FINSH_A ON 1=1
				AND WO_S_FINSH_A.PARENT = WO.WONUM
				AND WO_S_FINSH_A.STATUS = 'FINISHED_AFTER'
			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_S_CPLT_A ON 1=1
				AND WO_S_CPLT_A.PARENT = WO.WONUM
				AND WO_S_CPLT_A.STATUS = 'COMPLETED_AFTER'
			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_S_COMP_A ON 1=1
				AND WO_S_COMP_A.PARENT = WO.WONUM
				AND WO_S_COMP_A.STATUS = 'COMP_AFTER'

		WHERE 1=1
			AND wo.ISTASK = 'False'
			AND wo.WORKTYPE IN ('PM', 'CM') 


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

		UPDATE  [dbo].[W_CMMS_WO_F]
		SET 
			PLANT_WID = src.PLANT_WID
			, LOC_WID = src.LOC_WID
			, DATE_WID = src.DATE_WID
			, ASSET_WID = src.ASSET_WID

			, WORK_ORDERS = src.WORK_ORDERS
			, DESCRIPTION = src.DESCRIPTION
			, TYPE = src.TYPE
			, OVERHAUL = src.OVERHAUL
			, PM = src.PM
			, JOB_PLAN = src.JOB_PLAN
			, SITE = src.SITE
			, LOCATION = src.LOCATION
			, ASSET_NUM = src.ASSET_NUM
			, STATUS = src.STATUS
			, SUPERVISOR = src.SUPERVISOR
			, DATE_CREATION = src.DATE_CREATION
			, DATE_TARGET_START = src.DATE_TARGET_START
			, DATE_TARGET_FINISH = src.DATE_TARGET_FINISH
			, DATE_SCHEDULE_START = src.DATE_SCHEDULE_START
			, DATE_SCHEDULE_FINISH = src.DATE_SCHEDULE_FINISH
			, PARENT = src.PARENT
			, IS_SCHED = src.IS_SCHED

			, DATE_WSCH_BFR = src.DATE_WSCH_BFR
			, DATE_PLANNING_BFR = src.DATE_PLANNING_BFR
			, DATE_APPROVED_BFR = src.DATE_APPROVED_BFR
			, DATE_FINISHED_BFR = src.DATE_FINISHED_BFR
			, DATE_ACCEPTED_BFR = src.DATE_ACCEPTED_BFR
			, DATE_COMPLETED_BFR = src.DATE_COMPLETED_BFR

			, DATE_WSCH_AFT = src.DATE_WSCH_AFT
			, DATE_PLANNING_AFT = src.DATE_PLANNING_AFT
			, DATE_APPROVED_AFT = src.DATE_APPROVED_AFT
			, DATE_FINISHED_AFT = src.DATE_FINISHED_AFT
			, DATE_ACCEPTED_AFT = src.DATE_ACCEPTED_AFT
			, DATE_COMPLETED_AFT = src.DATE_COMPLETED_AFT

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = getdate()
		FROM [dbo].[W_CMMS_WO_F] tgt
		INNER JOIN #W_CMMS_WO_F_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


		-- 4. Insert non-existed records to main table from temp table
		PRINT '4. Insert non-existed records to main table from temp table'

		INSERT INTO [dbo].[W_CMMS_WO_F](
			PLANT_WID
			, LOC_WID
			, DATE_WID
			, ASSET_WID

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

			, DATE_WSCH_BFR
			, DATE_PLANNING_BFR
			, DATE_APPROVED_BFR
			, DATE_FINISHED_BFR
			, DATE_ACCEPTED_BFR
			, DATE_COMPLETED_BFR

			, DATE_WSCH_AFT
			, DATE_PLANNING_AFT
			, DATE_APPROVED_AFT
			, DATE_FINISHED_AFT
			, DATE_ACCEPTED_AFT
			, DATE_COMPLETED_AFT

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

			, DATE_WSCH_BFR
			, DATE_PLANNING_BFR
			, DATE_APPROVED_BFR
			, DATE_FINISHED_BFR
			, DATE_ACCEPTED_BFR
			, DATE_COMPLETED_BFR

			, DATE_WSCH_AFT
			, DATE_PLANNING_AFT
			, DATE_APPROVED_AFT
			, DATE_FINISHED_AFT
			, DATE_ACCEPTED_AFT
			, DATE_COMPLETED_AFT

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