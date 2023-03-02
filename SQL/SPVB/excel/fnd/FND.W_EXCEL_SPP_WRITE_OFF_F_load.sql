SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[SAP_proc_load_fnd_w_excel_spp_write_off_f] @p_batch_id [bigint] AS

BEGIN
	--DECLARE @p_batch_id bigint = 2020111601
	DECLARE	@tgt_TableName nvarchar(200) = N'FND.W_EXCEL_SPP_WRITE_OFF_F',
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

    set @v_job_id= (select top 1 JOB_ID from [dbo].[SAP_ETL_JOB] where 1=1 and ACTIVE_FLG='Y' and TGT_TABLE = @tgt_TableName)
	set @v_jobinstance_id = convert(bigint, convert(varchar,@v_batch_id)+convert(varchar,@v_job_id))
	set @v_src_tablename = (select top 1 SRC_TABLE from [dbo].[SAP_ETL_JOB] where 1=1 and ACTIVE_FLG='Y' and TGT_TABLE = @tgt_TableName /*+'T' */) 

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
	
		/*Update soft delete flg for old data*/
		UPDATE FND.W_EXCEL_SPP_WRITE_OFF_F SET W_DELETE_FLG = 'Y', W_UPDATE_DT = DATEADD(HH, 7, GETDATE()), W_BATCH_ID = @p_batch_id
		WHERE W_DELETE_FLG = 'N'
			AND FILE_NAME IN (SELECT DISTINCT FILE_NAME FROM STG.W_EXCEL_SPP_WRITE_OFF_FS /*WHERE W_BATCH_ID = @p_batch_id*/)
	;
	WITH A AS (																	
		SELECT 
			  Prop_1 AS [MATERIAL_NUMBER]	
			, Prop_2 AS MATERIAL_NAME	
			, Prop_3 AS BASE_UNIT	
			, SUM(CASE WHEN Prop_4 = '0' THEN 0.0 ELSE CONVERT(DECIMAL(38,20), Prop_4) END) AS REQUEST_WO_QTY_TM
			, SUM(CASE WHEN Prop_5 = '0' then 0.0 ELSE CONVERT(DECIMAL(38,20), Prop_5) END) AS REQUEST_WO_QTY_LM
			, SUM(CASE WHEN Prop_6 = '0' THEN 0.0 ELSE CONVERT(DECIMAL(38,20), Prop_6) END) AS REQUEST_WO_QTY_RWH
			, SUM(CASE WHEN Prop_7 = '0' THEN 0.0 ELSE CONVERT(DECIMAL(38,20), Prop_7) END) AS TOTAL_REQUEST_WO_QTY
			, SUM(CASE WHEN Prop_9 = '#VALUE!' then null WHEN Prop_9 = '0' THEN 0.0 ELSE CONVERT(DECIMAL(38,20), Prop_9) END) AS TOTAL_REQUEST_WO_AMT
			, MIN(CASE WHEN Prop_8 = '0' THEN 0.0 ELSE CONVERT(DECIMAL(38,20), Prop_8) END) AS UNIT_PRICE
			, LEFT(REPLACE(FILE_NAME, 'Write-off_', ''),3) AS PLANT_NAME
			, FILE_NAME
			, SUBSTRING(REPLACE(FILE_NAME, 'Write-off_', ''), 5,6) AS PERIOD
			, CONCAT(Prop_1, '~', SUBSTRING(REPLACE(FILE_NAME, 'Write-off_', ''), 5,6), '~', LEFT(REPLACE(FILE_NAME, 'Write-off_', ''),3)) AS W_INTEGRATION_ID
		FROM STG.W_EXCEL_SPP_WRITE_OFF_FS
		WHERE Prop_1 IS NOT NULL
		GROUP BY Prop_1, Prop_2, Prop_3, LEFT(REPLACE(FILE_NAME, 'Write-off_', ''),3)
		,  FILE_NAME,SUBSTRING(REPLACE(FILE_NAME, 'Write-off_', ''), 5,6)
		,  CONCAT(Prop_1, '~', SUBSTRING(REPLACE(FILE_NAME, 'Write-off_', ''), 5,6), '~', LEFT(REPLACE(FILE_NAME, 'Write-off_', ''),3))
	)
	INSERT INTO FND.W_EXCEL_SPP_WRITE_OFF_F
	(
		  [MATERIAL_NUMBER]
		, MATERIAL_NAME
		, BASE_UNIT
		, REQUEST_WO_QTY_TM
		, REQUEST_WO_QTY_LM
		, REQUEST_WO_QTY_RWH
		, TOTAL_REQUEST_WO_QTY
		, TOTAL_REQUEST_WO_AMT
		, UNIT_PRICE
		, PERIOD
		, PLANT_NAME
		, [W_DELETE_FLG]
		, W_DATASOURCE_NUM_ID
		, W_INSERT_DT
		, W_UPDATE_DT
		, W_BATCH_ID
		, W_INTEGRATION_ID
		, FILE_NAME
	)
	select 
		  [MATERIAL_NUMBER]
		, MATERIAL_NAME
		, BASE_UNIT
		, REQUEST_WO_QTY_TM
		, REQUEST_WO_QTY_LM
		, REQUEST_WO_QTY_RWH
		, TOTAL_REQUEST_WO_QTY
		, TOTAL_REQUEST_WO_AMT
		, UNIT_PRICE
		, PERIOD
		, PLANT_NAME
		, 'N' AS [W_DELETE_FLG]
		, 3 AS [W_DATASOURCE_NUM_ID]
		, GETDATE() AS [W_INSERT_DT]
		, GETDATE() AS [W_UPDATE_DT]
		, @p_batch_id [W_BATCH_ID]
		, W_INTEGRATION_ID
		, FILE_NAME
	from A
	--WHERE ROW_NUM = 1
	;

/*
		/*delete & re-insert data refresh*/
		DELETE FROM [dbo].[SAP_ETL_DATAFRESH_CONF] WHERE UPPER(TABLE_NAME) = UPPER(@tgt_TableName)

		INSERT INTO [dbo].[SAP_ETL_DATAFRESH_CONF] (TABLE_NAME, REFRESH_DATE, IS_FULLLOAD, IS_EXIST_SSAS, LAST_UPDATE_DATE)
		SELECT DISTINCT @tgt_TableName, NULL, 'Y', 'Y', DATEADD(HH, 7, GETDATE())
		FROM
			( 
			SELECT * FROM W_PROGRAM_TARGET_RCS_F
			) M
		WHERE W_BATCH_ID = @p_batch_id
			AND W_DELETE_FLG = 'N'
*/
		SET @src_rownum = (SELECT COUNT(1) FROM [STG].[W_EXCEL_SPP_WRITE_OFF_FS]  WHERE W_BATCH_ID = @p_batch_id);
		SET @tgt_rownum = (SELECT COUNT(1) FROM FND.W_EXCEL_SPP_WRITE_OFF_F  WHERE W_DELETE_FLG = 'N' AND  W_BATCH_ID = @p_batch_id);

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
