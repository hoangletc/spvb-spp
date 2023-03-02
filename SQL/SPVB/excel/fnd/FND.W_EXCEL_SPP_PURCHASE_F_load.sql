SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[EXCEL_proc_load_fnd_w_spp_purchase_f] @p_batch_id [bigint] AS

BEGIN
	--DECLARE @p_batch_id bigint = 2020111601
	DECLARE	@tgt_TableName nvarchar(200) = N'FND.W_EXCEL_SPP_PURCHASE_F',
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
		UPDATE FND.W_EXCEL_SPP_PURCHASE_F SET 
			W_DELETE_FLG = 'Y'
			, W_UPDATE_DT = DATEADD(HH, 7, GETDATE())
			, W_BATCH_ID = @p_batch_id
		WHERE W_DELETE_FLG = 'N'
			AND FILE_PATH IN (SELECT DISTINCT FILE_PATH FROM STG.W_EXCEL_SPP_PURCHASE_FS /*WHERE W_BATCH_ID = @p_batch_id*/)
	;
		WITH A AS (																	
			SELECT 
				CONVERT(NVARCHAR(20), Prop_2)							AS [LINE]
				, CONVERT(NVARCHAR(30), Prop_3)							AS [MACHINE]
				, CONVERT(NVARCHAR(30), Prop_4)							AS [ITEM_CODE]
				, CONVERT(NVARCHAR(1000), Prop_5)							AS [DESCRIPTION]
				, CONVERT(NVARCHAR(100), Prop_6)							AS [CODE]
				, CONVERT(NVARCHAR(100), Prop_7)							AS [MANUFACTURER]
				, CONVERT(NVARCHAR(100), Prop_8)							AS [UOM]
				, CASE WHEN Prop_9 IS NULL OR ISNUMERIC(Prop_9) = 0 
						OR TRIM(Prop_9) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_9)
				END 														AS [QUANTITY]
				, CASE WHEN Prop_10 IS NULL OR ISNUMERIC(Prop_10) = 0 
						OR TRIM(Prop_10) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_10)
				END 														AS [OVH]
				, CASE WHEN Prop_11 IS NULL OR ISNUMERIC(Prop_11) = 0 
						OR TRIM(Prop_11) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_11)
				END 														AS [PM]
				, CASE WHEN Prop_12 IS NULL OR ISNUMERIC(Prop_12) = 0 
						OR TRIM(Prop_12) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_12)
				END 														AS [CM]
				, CASE WHEN Prop_13 IS NULL OR ISNUMERIC(Prop_13) = 0 
						OR TRIM(Prop_13) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_13)
				END 														AS [CRITICAL_SPARE]
				, CASE WHEN Prop_14 IS NULL OR ISNUMERIC(Prop_14) = 0 
						OR TRIM(Prop_14) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_14)
				END 														AS [NEEDED_QTY]
				, CASE WHEN Prop_15 IS NULL OR ISNUMERIC(Prop_15) = 0 
						OR TRIM(Prop_15) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_15)
				END 														AS [MIN_LEVEL]
				, CASE WHEN Prop_16 IS NULL OR ISNUMERIC(Prop_16) = 0 
						OR TRIM(Prop_16) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_16)
				END 														AS [MAX_LEVEL]
				, CASE WHEN Prop_17 IS NULL OR ISNUMERIC(Prop_17) = 0 
						OR TRIM(Prop_17) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_17)
				END 														AS [AVG_LEVEL]
				, CASE WHEN Prop_18 IS NULL OR ISNUMERIC(Prop_18) = 0 
						OR TRIM(Prop_18) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_18)
				END 														AS [JANUARY]
				, CASE WHEN Prop_19 IS NULL OR ISNUMERIC(Prop_19) = 0 
						OR TRIM(Prop_19) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_19)
				END 														AS [FEBRUARY]
				, CASE WHEN Prop_20 IS NULL OR ISNUMERIC(Prop_20) = 0 
						OR TRIM(Prop_20) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_20)
				END 														AS [MARCH]
				, CASE WHEN Prop_21 IS NULL OR ISNUMERIC(Prop_21) = 0 
						OR TRIM(Prop_21) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_21)
				END 														AS [APRIL]
				, CASE WHEN Prop_22 IS NULL OR ISNUMERIC(Prop_22) = 0 
						OR TRIM(Prop_22) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_22)
				END 														AS [MAY]
				, CASE WHEN Prop_23 IS NULL OR ISNUMERIC(Prop_23) = 0 
						OR TRIM(Prop_23) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_23)
				END 														AS [JUNE]
				, CASE WHEN Prop_24 IS NULL OR ISNUMERIC(Prop_24) = 0 
						OR TRIM(Prop_24) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_24)
				END 														AS [JULY]
				, CASE WHEN Prop_25 IS NULL OR ISNUMERIC(Prop_25) = 0 
						OR TRIM(Prop_25) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_25)
				END 														AS [AUGUST]
				, CASE WHEN Prop_26 IS NULL OR ISNUMERIC(Prop_26) = 0 
						OR TRIM(Prop_26) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_26)
				END 														AS [SEPTEMBER]
				, CASE WHEN Prop_27 IS NULL OR ISNUMERIC(Prop_27) = 0 
						OR TRIM(Prop_27) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_27)
				END 														AS [OCTOBER]
				, CASE WHEN Prop_28 IS NULL OR ISNUMERIC(Prop_28) = 0 
						OR TRIM(Prop_28) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_28)
				END 														AS [NOVEMBER]
				, CASE WHEN Prop_29 IS NULL OR ISNUMERIC(Prop_29) = 0 
						OR TRIM(Prop_29) = '-' THEN 0.0 		
					ELSE CONVERT(FLOAT, Prop_29)
				END 														AS [DECEMBER]


				, SUBSTRING(FILE_PATH, 19, 3)								AS PLANT_NAME
				, FILE_PATH

				, CONCAT(
					Prop_3
					, '~'
					, LEFT(REPLACE(FILE_PATH, 'Write-off_', ''), 3)
					, '~'
					, SUBSTRING(REPLACE(FILE_PATH, 'Write-off_', ''), 5,6)
				) 															AS W_INTEGRATION_ID
			FROM STG.W_EXCEL_SPP_PURCHASE_FS
		), TMP_UNPIVOT AS (
			SELECT
				TMP.*
				, 2023 * 100 + MONTH([MONTH] + ' 1 2023') 					AS [PERIOD]
			FROM A
			UNPIVOT (
				PURCHASING
				FOR [MONTH] IN (
					[JANUARY]
					, [FEBRUARY]
					, [MARCH]
					, [APRIL]
					, [MAY]
					, [JUNE]
					, [JULY]
					, [AUGUST]
					, [SEPTEMBER]
					, [OCTOBER]
					, [NOVEMBER]
					, [DECEMBER]
				)
			) AS TMP
		)
			INSERT INTO FND.W_EXCEL_SPP_PURCHASE_F(
				, [LINE]
				, [MACHINE]
				, [ITEM_CODE]
				, [DESCRIPTION]
				, [CODE]
				, [MANUFACTURER]
				, [UOM]

				, [QUANTITY]
				, [OVH]
				, [PM]
				, [CM]
				, [CRITICAL_SPARE]
				, [NEEDED_QTY]
				, [MIN_LEVEL]
				, [MAX_LEVEL]
				, [AVG_LEVEL]
				, [PERIOD]
				, [PURCHASING]

				, PLANT_NAME
				, FILE_PATH

				, [W_DELETE_FLG]
				, W_DATASOURCE_NUM_ID
				, W_INSERT_DT
				, W_UPDATE_DT
				, W_BATCH_ID
				, W_INTEGRATION_ID
			)
			select 
				[LINE]
				, [MACHINE]
				, [ITEM_CODE]
				, [DESCRIPTION]
				, [CODE]
				, [MANUFACTURER]
				, [UOM]

				, [QUANTITY]
				, [OVH]
				, [PM]
				, [CM]
				, [CRITICAL_SPARE]
				, [NEEDED_QTY]
				, [MIN_LEVEL]
				, [MAX_LEVEL]
				, [AVG_LEVEL]
				, [PERIOD]
				, [PURCHASING]

				, PLANT_NAME
				, FILE_PATH

				, 'N' 						AS [W_DELETE_FLG]
				, 3 						AS [W_DATASOURCE_NUM_ID]
				, DATEADD(HH, 7, GETDATE()) AS [W_INSERT_DT]
				, DATEADD(HH, 7, GETDATE()) AS [W_UPDATE_DT]
				, @p_batch_id				AS [W_BATCH_ID]
				, W_INTEGRATION_ID
			from TMP_UNPIVOT
		;

		/*delete & re-insert data refresh*/
		DELETE FROM [dbo].[SAP_ETL_DATAFRESH_CONF] WHERE UPPER(TABLE_NAME) = UPPER(@tgt_TableName)

		INSERT INTO [dbo].[SAP_ETL_DATAFRESH_CONF] (
			TABLE_NAME
			, REFRESH_DATE
			, IS_FULLLOAD
			, IS_EXIST_SSAS
			, LAST_UPDATE_DATE
		)
		SELECT DISTINCT @tgt_TableName, NULL, 'Y', 'Y', DATEADD(HH, 7, GETDATE())
		FROM
			( 
			SELECT * FROM W_PROGRAM_TARGET_RCS_F
			) M
		WHERE W_BATCH_ID = @p_batch_id
			AND W_DELETE_FLG = 'N'

		SET @src_rownum = (SELECT COUNT(1) FROM [STG].[W_EXCEL_SPP_PURCHASE_FS] WHERE W_BATCH_ID = @p_batch_id);
		SET @tgt_rownum = (SELECT COUNT(1) FROM FND.W_EXCEL_SPP_PURCHASE_F WHERE W_DELETE_FLG = 'N' AND  W_BATCH_ID = @p_batch_id);

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
