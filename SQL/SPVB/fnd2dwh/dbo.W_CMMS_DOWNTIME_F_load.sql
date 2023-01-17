SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (OBJECT_ID('[dbo].[proc_load_w_cmms_downtime_f]') is not null)
BEGIN
    DROP PROCEDURE [dbo].[proc_load_w_cmms_downtime_f]
END;
GO

CREATE PROC [dbo].[proc_load_w_cmms_downtime_f]
    @p_batch_id [bigint]
AS 
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_CMMS_DOWNTIME_F',
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

        IF OBJECT_ID(N'tempdb..#W_CMMS_DOWNTIME_F_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_CMMS_DOWNTIME_F_tmp'
            DROP Table #W_CMMS_DOWNTIME_F_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

		;WITH TMP_ASSET_LINE_INFO AS (
			-- NOTE: Nên để phần tìm Line info của mỗi asset vào Python script thay vì phải chạy lại mỗi lần query như vầy
			select
				T1.ASSET_WID,
				T1.ASSET_NUM AS ASSET_NUM,
				T2.ASSET_NUM AS LINE_ASSET_NUM,
				T2.[DESCRIPTION] AS LINE_DESCRIPTION
			from [dbo].[W_CMMS_ASSET_D] T1, [dbo].[W_CMMS_ASSET_D] T2
			where 1=1
				AND T2.ASSET_NUM = CASE 
					WHEN T1.ASSET_HIERACHICAL_TYPE = 'machine' THEN T1.[PARENT]
					WHEN T1.ASSET_HIERACHICAL_TYPE = 'component' THEN T1.[GRANDPARENT]
					ELSE T1.[ASSET_NUM]
				END
		)
			SELECT
				CONVERT(DATE, CHANGEDATE) AS DATE_WID
				, ISNULL(PLANT.PLANT_WID, 0) AS PLANT_WID
				, ISNULL(LOC.ASSET_WID, 0) AS LOCATION_WID

				, TMP_ASST_L_INF.LINE_ASSET_NUM AS LINE_ASSET_NUM
				, TMP_ASST_L_INF.LINE_DESCRIPTION AS LINE_ASSET_DESCRIPTION
				, F.ASSET_NUM AS ASSET_NUM
				, CONVERT(DECIMAL(38,20), DOWNTIME) * 60 AS DOWNTIME
				, CONVERT(nvarchar(100), ASST.DESCRIPTION) AS NAME
				, NULL AS ANALYSIS_1 -- NOTE: Không biết là cột nào
				, NULL AS ANALYSIS_2 -- NOTE: Không biết là cột nào
				, CONVERT(nvarchar(5), CODE) AS ANALYSIS_3
				, CONVERT(nvarchar(50), CODE_DESCRIPTION) AS DOWNTIME_CODE
				, NULL AS ISSUE -- NOTE: Không biết là cột nào
				, CONVERT(nvarchar(50), F.LOCATION) AS LOCATION
				
				, CONCAT_WS('~', ASSET_STATUS_ID, F.LOCATION, CHANGEDATE, CODE) AS W_INTEGRATION_ID
				, 'N' AS W_DELETE_FLG
				, 1 AS W_DATASOURCE_NUM_ID
				, GETDATE() AS W_INSERT_DT
				, GETDATE() AS W_UPDATE_DT
				, NULL W_BATCH_ID
				, 'N' AS W_UPDATE_FLG
			INTO #W_CMMS_DOWNTIME_F_tmp
			FROM [FND].[W_CMMS_ASSET_STATUS_F] F
				LEFT JOIN [dbo].[W_PLANT_SAP_D] PLANT ON 1=1
					AND PLANT.PLANT_NAME_2 = LEFT(F.LOCATION, 3)
				LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
					AND LOC.LOCATION = F.LOCATION
				LEFT JOIN [dbo].[W_CMMS_ASSET_D] ASST ON 1=1
					AND ASST.ASSET_NUM = F.ASSET_NUM
				LEFT JOIN TMP_ASSET_LINE_INFO AS TMP_ASST_L_INF ON 1=1
					AND F.ASSET_NUM = TMP_ASST_L_INF.ASSET_NUM


			-- 3. Update main table using W_INTEGRATION_ID
			PRINT '3. Update main table using W_INTEGRATION_ID'

			-- 3.1. Mark existing records by flag 'Y'
			PRINT '3.1. Mark existing records by flag ''Y'''

			UPDATE #W_CMMS_DOWNTIME_F_tmp
			SET W_UPDATE_FLG = 'Y'
			FROM #W_CMMS_DOWNTIME_F_tmp tg
			INNER JOIN [dbo].[W_CMMS_DOWNTIME_F] sc 
			ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

			-- 3.2. Start updating
			PRINT '3.2. Start updating'

			UPDATE  [dbo].[W_CMMS_DOWNTIME_F]
			SET 
				DATE_WID = src.DATE_WID
				, PLANT_WID = src.PLANT_WID
				, LOCATION_WID = src.LOCATION_WID

				, LINE_ASSET_NUM = src.LINE_ASSET_NUM
				, LINE_ASSET_DESCRIPTION = src.LINE_ASSET_DESCRIPTION
				, ASSET_NUM = src.ASSET_NUM
				, DOWNTIME = src.DOWNTIME
				, [NAME] = src.NAME
				, ANALYSIS_1 = src.ANALYSIS_1
				, ANALYSIS_2 = src.ANALYSIS_2
				, ANALYSIS_3 = src.ANALYSIS_3
				, DOWNTIME_CODE = src.DOWNTIME_CODE
				, ISSUE = src.ISSUE
				, [LOCATION] = src.LOCATION

				, W_DELETE_FLG = src.W_DELETE_FLG
				, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
				, W_INSERT_DT = src.W_INSERT_DT
				, W_BATCH_ID = src.W_BATCH_ID
				, W_INTEGRATION_ID = src.W_INTEGRATION_ID
				, W_UPDATE_DT = getdate()
			FROM [dbo].[W_CMMS_DOWNTIME_F] tgt
			INNER JOIN #W_CMMS_DOWNTIME_F_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


			-- 4. Insert non-existed records to main table from temp table
			PRINT '4. Insert non-existed records to main table from temp table'

			INSERT INTO [dbo].[W_CMMS_DOWNTIME_F](
				DATE_WID
				, PLANT_WID
				, LOCATION_WID

				, LINE_ASSET_NUM
				, LINE_ASSET_DESCRIPTION
				, ASSET_NUM
				, DOWNTIME
				, [NAME]
				, ANALYSIS_1
				, ANALYSIS_2
				, ANALYSIS_3
				, DOWNTIME_CODE
				, ISSUE
				, [LOCATION]

				, W_DELETE_FLG
				, W_DATASOURCE_NUM_ID
				, W_INSERT_DT
				, W_UPDATE_DT
				, W_BATCH_ID
				, W_INTEGRATION_ID
			)
			SELECT
				DATE_WID
				, PLANT_WID
				, LOCATION_WID

				, LINE_ASSET_NUM
				, LINE_ASSET_DESCRIPTION
				, ASSET_NUM
				, DOWNTIME
				, [NAME]
				, ANALYSIS_1
				, ANALYSIS_2
				, ANALYSIS_3
				, DOWNTIME_CODE
				, ISSUE
				, [LOCATION]

				, W_DELETE_FLG
				, W_DATASOURCE_NUM_ID
				, W_INSERT_DT
				, W_UPDATE_DT
				, W_BATCH_ID
				, W_INTEGRATION_ID
			FROM #W_CMMS_DOWNTIME_F_tmp
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
            FROM W_CMMS_DOWNTIME_F
        ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

		SET @src_rownum = ( SELECT COUNT(1) FROM #W_CMMS_DOWNTIME_F_tmp );
		SET @tgt_rownum = ( 
            SELECT 
                COUNT(DISTINCT W_INTEGRATION_ID)
            FROM W_CMMS_DOWNTIME_F
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