SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[EXCEL_proc_load_fnd_w_spp_spending_aop_f] @p_batch_id [bigint] AS

BEGIN
	--DECLARE @p_batch_id bigint = 2020111601
	DECLARE	@tgt_TableName nvarchar(200) = N'FND.W_EXCEL_SPP_SPENDING_AOP_F',
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
		UPDATE FND.W_EXCEL_SPP_SPENDING_AOP_F
		SET W_DELETE_FLG = 'Y'
			, W_UPDATE_DT = DATEADD(HH, 7, GETDATE())
			, W_BATCH_ID = @p_batch_id
		WHERE 1=1
			AND W_DELETE_FLG = 'N'
			AND FILE_PATH IN (
				SELECT DISTINCT FILE_PATH 
				FROM STG.W_EXCEL_SPP_SPENDING_AOP_FS /*WHERE W_BATCH_ID = @p_batch_id*/
			)
		;

		WITH A AS (																	
			SELECT 
				Prop_1 											    AS OPEX_LINE
				, Prop_4 											AS AP_ACCOUNT
				, Prop_5 											AS SAP_ACCOUNT_NAME
				, Prop_6 											AS [DESCRIPTION]
				, CASE WHEN Prop_7 = '' THEN 0.0 		
					ELSE CONVERT(DECIMAL(38, 20), Prop_7)
				END 												AS AOP_AMOUNT
				, Prop_8											AS NORM_ONE_OFF
				, CASE WHEN Prop_9 IS NULL OR Prop_9 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_9)
				END													AS JULF_22
				, CASE WHEN Prop_10 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_10) * 100
				END													AS INC_DEC_PERCENT
				, Prop_11 											AS [NOTE]
				, CASE WHEN Prop_12 IS NULL OR Prop_12 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_12)
				END													AS [JANUARY]
				, CASE WHEN Prop_13 IS NULL OR Prop_13 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_13)
				END													AS [FEBRUARY]
				, CASE WHEN Prop_14 IS NULL OR Prop_14 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_14)
				END													AS [MARCH]
				, CASE WHEN Prop_15 IS NULL OR Prop_15 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_15)
				END													AS [APRIL]
				, CASE WHEN Prop_16 IS NULL OR Prop_16 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_16)
				END													AS [MAY]
				, CASE WHEN Prop_17 IS NULL OR Prop_17 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_17)
				END													AS [JUNE]
				, CASE WHEN Prop_18 IS NULL OR Prop_18 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_18)
				END													AS [JULY]
				, CASE WHEN Prop_19 IS NULL OR Prop_19 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_19)
				END													AS [AUGUST]
				, CASE WHEN Prop_20 IS NULL OR Prop_20 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_20)
				END													AS [SEPTEMBER]
				, CASE WHEN Prop_21 IS NULL OR Prop_21 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_21)
				END													AS [OCTOBER]
				, CASE WHEN Prop_22 IS NULL OR Prop_22 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_22)
				END													AS [NOVEMBER]
				, CASE WHEN Prop_23 IS NULL OR Prop_23 = '' THEN 0.0
					ELSE CONVERT(DECIMAL(38, 20), Prop_23)
				END													AS [DECEMBER]
				, Prop_25											AS [RM_TYPE]
				, Prop_26											AS [LINE_FUNCTION_NAME]
				, Prop_27											AS [MACHINE]
				, Prop_28											AS [PM_CM_OVH]
				, Prop_29											AS [SPP_SERVICE]
				, SUBSTRING(FILE_PATH, 24, 3) 						AS PLANT_NAME

				, FILE_PATH
				, CONCAT(Prop_4, '~', SUBSTRING(FILE_PATH, 24, 3)) 	AS W_INTEGRATION_ID
			FROM STG.W_EXCEL_SPP_SPENDING_AOP_FS
		), TMP_UNPIVOT AS (		
			SELECT
				TMP.*
				, 2023 * 100 + MONTH([MONTH] + ' 1 2023') 			AS [PERIOD]
			FROM A
			UNPIVOT (
				SPENDING_AMOUNT
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
			INSERT INTO FND.W_EXCEL_SPP_SPENDING_AOP_F (
				OPEX_LINE
				, AP_ACCOUNT
				, SAP_ACCOUNT_NAME
				, [DESCRIPTION]
				, AOP_AMOUNT
				, NORM_ONE_OFF
				, JULF_22
				, INC_DEC_PERCENT
				, [NOTE]
				, [PERIOD]
				, SPENDING_AMOUNT
				, RM_TYPE
				, LINE_FUNCTION_NAME
				, MACHINE
				, PM_CM_OVH
				, SPP_SERVICE
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
				OPEX_LINE
				, AP_ACCOUNT
				, SAP_ACCOUNT_NAME
				, [DESCRIPTION]
				, AOP_AMOUNT
				, NORM_ONE_OFF
				, JULF_22
				, INC_DEC_PERCENT
				, [NOTE]
				, [PERIOD]
				, SPENDING_AMOUNT
				, RM_TYPE
				, LINE_FUNCTION_NAME
				, MACHINE
				, PM_CM_OVH
				, SPP_SERVICE
				, PLANT_NAME
				, FILE_PATH

				, 'N' 						AS [W_DELETE_FLG]
				, 3 						AS [W_DATASOURCE_NUM_ID]
				, DATEADD(HH, 7, GETDATE()) AS [W_INSERT_DT]
				, DATEADD(HH, 7, GETDATE()) AS [W_UPDATE_DT]
				, @p_batch_id				AS [W_BATCH_ID]
				, W_INTEGRATION_ID
			FROM TMP_UNPIVOT
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
		SET @src_rownum = (
			SELECT COUNT(1) FROM [STG].[W_EXCEL_SPP_SPENDING_AOP_FS]
			WHERE W_BATCH_ID = @p_batch_id
		);
		SET @tgt_rownum = (
			SELECT COUNT(1) FROM FND.W_EXCEL_SPP_SPENDING_AOP_F
			WHERE W_DELETE_FLG = 'N' AND  W_BATCH_ID = @p_batch_id
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