SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[EXCEL_proc_load_fnd_w_spp_purchase_oracle_d] @p_batch_id [bigint] AS
BEGIN
	DECLARE	@tgt_TableName nvarchar(200) = N'FND.W_EXCEL_SPP_PURCHASE_ORACLE_D',
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
		UPDATE FND.W_EXCEL_SPP_PURCHASE_ORACLE_D SET 
			W_DELETE_FLG = 'Y'
			, W_UPDATE_DT = DATEADD(HH, 7, GETDATE())
			, W_BATCH_ID = @p_batch_id
		WHERE W_DELETE_FLG = 'N'
			AND FILE_PATH IN (SELECT DISTINCT FILE_PATH FROM STG.W_EXCEL_SPP_PURCHASE_ORACLE_DS /*WHERE W_BATCH_ID = @p_batch_id*/)
		;

		INSERT INTO FND.W_EXCEL_SPP_PURCHASE_ORACLE_D(
			  MATERIAL
			, VALUATION_TYPE
			, PLANT
			, STORAGE_LOCATION
			, BATCH
			, VALUATION_AREA
			, VALUATION_CLASS
			, TYPE_OF_WAREHOUSE

			, MATERIAL_TYPE
			, MATERIAL_GROUP
			, PURCHASING_GROUP
			, MATERIAL_DESCRIPTION
			, BASE_UNIT_OF_MEASURE
			, CURRENCY
			, PRICE_CONTROL
			, STOCK_QTY_ON_PERIOD_START
			, VALUE_ON_PERIOD_START
			, TOTAL_GOODS_RECEIPT_QTY
			, TOTAL_GOODS_RECEIPT_VALUE
			, TOTAL_GOODS_ISSUE_QTY
			, TOTAL_GOODS_ISSUE_VALUE
			, STOCK_QTY_ON_PERIOD_END
			, STOCK_VALUE_ON_PERIOD_END
			, SOURCE_OF_INFORMATION
			, OB_QTY
			, OB_ORACLE_PRICE
			, OB_BALANCE
			, TRANSFER_QTY
			, TRANSFER_AMOUNT
			, OB_AMOUNT
			, GR_QTY
			, GR_AMOUNT
			, GI_QTY
			, GI_PRICE
			, GI_AMOUNT
			, OTHER_TRANSFER_QTY
			, OTHER_TRANSFER_AMOUNT
			, CB_QTY
			, CB_AMOUNT

			, [FILE_PATH]
			, [PERIOD]

			, [W_DELETE_FLG]
			, [W_DATASOURCE_NUM_ID]
			, [W_INSERT_DT]
			, [W_UPDATE_DT]
			, [W_BATCH_ID]
			, [W_INTEGRATION_ID]
		)
		SELECT 
			  CONVERT(nvarchar(100), Prop_0) 						AS MATERIAL
			, CONVERT(nvarchar(100), Prop_1) 						AS VALUATION_TYPE
			, CONVERT(nvarchar(100), Prop_2) 						AS PLANT
			, CONVERT(nvarchar(100), Prop_3) 						AS STORAGE_LOCATION
			, CONVERT(nvarchar(100), Prop_4) 						AS BATCH
			, CONVERT(nvarchar(100), Prop_5) 						AS VALUATION_AREA
			, CONVERT(nvarchar(100), Prop_6) 						AS VALUATION_CLASS
			, CONVERT(nvarchar(100), Prop_7) 						AS TYPE_OF_WAREHOUSE

			, CONVERT(nvarchar(100), Prop_10) 						AS MATERIAL_TYPE
			, CONVERT(nvarchar(100), Prop_11) 						AS MATERIAL_GROUP
			, CONVERT(nvarchar(100), Prop_12) 						AS PURCHASING_GROUP
			, CONVERT(nvarchar(1000), Prop_13) 						AS MATERIAL_DESCRIPTION
			, CONVERT(nvarchar(100), Prop_14) 						AS BASE_UNIT_OF_MEASURE
			, CONVERT(nvarchar(100), Prop_15) 						AS CURRENCY
			, CONVERT(nvarchar(100), Prop_16) 						AS PRICE_CONTROL
			, CONVERT(DECIMAL(38, 20), Prop_17) 					AS STOCK_QTY_ON_PERIOD_START
			, CONVERT(DECIMAL(38, 20), Prop_18) 					AS VALUE_ON_PERIOD_START
			, CONVERT(DECIMAL(38, 20), Prop_19) 					AS TOTAL_GOODS_RECEIPT_QTY
			, CONVERT(DECIMAL(38, 20), Prop_20) 					AS TOTAL_GOODS_RECEIPT_VALUE
			, CONVERT(DECIMAL(38, 20), Prop_21) 					AS TOTAL_GOODS_ISSUE_QTY
			, CONVERT(DECIMAL(38, 20), Prop_22) 					AS TOTAL_GOODS_ISSUE_VALUE
			, CONVERT(DECIMAL(38, 20), Prop_23) 					AS STOCK_QTY_ON_PERIOD_END
			, CONVERT(DECIMAL(38, 20), Prop_24) 					AS STOCK_VALUE_ON_PERIOD_END
			, CONVERT(nvarchar(100), Prop_25)   					AS SOURCE_OF_INFORMATION
			, CONVERT(DECIMAL(38, 20), Prop_26) 					AS OB_QTY
			, CONVERT(DECIMAL(38, 20), Prop_27) 					AS OB_ORACLE_PRICE
			, CONVERT(DECIMAL(38, 20), Prop_28) 					AS OB_BALANCE
			, CONVERT(DECIMAL(38, 20), Prop_29) 					AS TRANSFER_QTY
			, CONVERT(DECIMAL(38, 20), Prop_30) 					AS TRANSFER_AMOUNT

			, CONVERT(DECIMAL(38, 20), Prop_32) 					AS OB_AMOUNT
			, CONVERT(DECIMAL(38, 20), Prop_33) 					AS GR_QTY
			, CONVERT(DECIMAL(38, 20), Prop_34) 					AS GR_AMOUNT
			, CONVERT(DECIMAL(38, 20), Prop_35) 					AS GI_QTY
			, CONVERT(DECIMAL(38, 20), Prop_36) 					AS GI_PRICE
			, CONVERT(DECIMAL(38, 20), Prop_37) 					AS GI_AMOUNT
			, CONVERT(DECIMAL(38, 20), Prop_38) 					AS OTHER_TRANSFER_QTY
			, CONVERT(DECIMAL(38, 20), Prop_39) 					AS OTHER_TRANSFER_AMOUNT
			, CONVERT(DECIMAL(38, 20), Prop_40) 					AS CB_QTY
			, CONVERT(DECIMAL(38, 20), Prop_41) 					AS CB_AMOUNT

			, [FILE_PATH]
			, CONVERT(INT, SUBSTRING([FILE_PATH], 17, 6))			AS [PERIOD]

			, 'N' 													AS [W_DELETE_FLG]
			, 3 													AS [W_DATASOURCE_NUM_ID]
			, DATEADD(HH, 7, GETDATE()) 							AS [W_INSERT_DT]
			, DATEADD(HH, 7, GETDATE()) 							AS [W_UPDATE_DT]
			, @p_batch_id											AS [W_BATCH_ID]
			, CONCAT([Prop_0], '~', [Prop_1], '~',
					[Prop_2], '~', SUBSTRING([FILE_PATH], 17, 6))	AS W_INTEGRATION_ID
		FROM STG.W_EXCEL_SPP_PURCHASE_ORACLE_DS
		WHERE 1=1
			AND [Prop_0] IS NOT NULL
			AND [Prop_1] IS NOT NULL
			AND [Prop_2] IS NOT NULL
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

		SET @src_rownum = (SELECT COUNT(1) FROM [STG].[W_EXCEL_SPP_PURCHASE_ORACLE_DS] WHERE W_BATCH_ID = @p_batch_id);
		SET @tgt_rownum = (SELECT COUNT(1) FROM FND.W_EXCEL_SPP_PURCHASE_ORACLE_D WHERE W_DELETE_FLG = 'N' AND  W_BATCH_ID = @p_batch_id);

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
