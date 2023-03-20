SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[EXCEL_proc_load_fnd_w_spp_j3rflvmobvedh_f] @p_batch_id [bigint] AS
BEGIN
	DECLARE	@tgt_TableName nvarchar(200) = N'FND.W_EXCEL_SPP_J3RFLVMOBVEDH_F',
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
		UPDATE FND.W_EXCEL_SPP_J3RFLVMOBVEDH_F SET 
			W_DELETE_FLG = 'Y'
			, W_UPDATE_DT = DATEADD(HH, 7, GETDATE())
			, W_BATCH_ID = @p_batch_id
		WHERE W_DELETE_FLG = 'N'
			AND FILE_PATH IN (SELECT DISTINCT FILE_PATH FROM STG.W_EXCEL_SPP_J3RFLVMOBVEDH_FS /*WHERE W_BATCH_ID = @p_batch_id*/)
		;

		INSERT INTO FND.W_EXCEL_SPP_J3RFLVMOBVEDH_F(
			  [MATERIAL]
			, [VALUATION_TYPE]
			, [PLANT]
			, [STORAGE_LOCATION]
			, [BATCH]
			, [COMPANY_CODE]
			, [VALUATION_AREA]
			, [VALUATION_CLASS]
			, [GL_ACCOUNT]
			, [MATERIAL_TYPE]
			, [MATERIAL_GROUP]
			, [EXT_MATERIAL_GROUP]
			, [OLD_MATERIAL_NUMBER]
			, [PURCHASING_GROUP]
			, [MATERIAL_DESCRIPTION]
			, [BASE_UNIT_OF_MEASURE]
			, [CURRENCY]
			, [PRICE_CONTROL]
			, [STOCK_QUANTITY_ON_PERIOD_START]
			, [VALUE_ON_PERIOD_START]
			, [TOTAL_GOODS_RECEIPT_QUANTITY]
			, [TOTAL_GOODS_RECEIPT_VALUE]
			, [TOTAL_GOODS_ISSUE_QUANTITY]
			, [TOTAL_GOODS_ISSUE_VALUE]
			, [STOCK_QUANTITY_ON_PERIOD_END]
			, [STOCK_VALUE_ON_PERIOD_END]
			, [RECEIPT_FROM_PURCHASE_QUANTITY]
			, [RECEIPT_FROM_PURCHASE_VALUE]
			, [ISSUE_TO_SALES_QUANTITY]
			, [ISSUE_TO_SALES_VALUE]
			, [RECEIPT_FROM_MANUFACT_QUANTITY]
			, [RECEIPT_FROM_MANUFACT_VALUE]
			, [ISSUE_TO_MANUFACT_QUANTITY]
			, [ISSUE_TO_MANUFACT_VALUE]
			, [RECEIPT_FROM_TRANSFER_QUANTITY]
			, [RECEIPT_FROM_TRANSFER_VALUE]
			, [ISSUE_TO_TRANSFER_QUANTITY]
			, [ISSUE_TO_TRANSFER_VALUE]
			, [QUANTITY_OF_OTHER_RECEIVED_GOODS]
			, [VALUE_OF_OTHER_RECEIVED_GOODS]
			, [QUANTITY_OF_OTHER_ISSUED_GOODS]
			, [VALUE_OF_OTHER_ISSUED_GOODS]
			, [ISSUE_TO_INTERNAL_PURPOSES_QTY]
			, [ISSUE_TO_INTERNAL_PURPOSES_VAL]
			, [DEBIT_REVALUATION]
			, [CREDIT_REVALUATION]

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
			  CONVERT(nvarchar(10), [Material]) 							AS MATERIAL                                             
			, CONVERT(nvarchar(10), [Valuation Type]) 						AS VALUATION_TYPE                                 
			, CONVERT(nvarchar(5), [Plant]) 								AS PLANT                                                   
			, CONVERT(nvarchar(5), [Storage location])						AS STORAGE_LOCATION                             
			, CONVERT(nvarchar(10), [Batch]) 								AS BATCH                                                   
			, CONVERT(nvarchar(5), [Company Code]) 							AS COMPANY_CODE                                     
			, CONVERT(nvarchar(5), [Valuation area]) 						AS VALUATION_AREA                                 
			, CONVERT(nvarchar(5), [Valuation Class]) 						AS VALUATION_CLASS                               
			, CONVERT(nvarchar(100), [G/L Account]) 						AS GL_ACCOUNT                                       
			, CONVERT(nvarchar(5), [Material type]) 						AS MATERIAL_TYPE
			, CONVERT(nvarchar(8), [Material Group]) 						AS MATERIAL_GROUP
			, CONVERT(nvarchar(100), [Ext. Material Group]) 				AS EXT_MATERIAL_GROUP
			, CONVERT(nvarchar(10), [Old material number]) 					AS OLD_MATERIAL_NUMBER
			, CONVERT(nvarchar(5), [Purchasing Group]) 						AS PURCHASING_GROUP
			, CONVERT(nvarchar(1000), [Material description]) 				AS MATERIAL_DESCRIPTION
			, CONVERT(nvarchar(5), [Base Unit of Measure]) 					AS BASE_UNIT_OF_MEASURE
			, CONVERT(nvarchar(5), [Currency]) 			 					AS CURRENCY
			, CONVERT(nvarchar(3), [Price control]) 		 				AS PRICE_CONTROL
			, CONVERT(decimal(38, 20), [Stock Quantity on Period Start]) 	AS STOCK_QUANTITY_ON_PERIOD_START 
			, CONVERT(decimal(38, 20), [Value on Period Start]) 			AS VALUE_ON_PERIOD_START
			, CONVERT(decimal(38, 20), [Total Goods Receipt Quantity]) 		AS TOTAL_GOODS_RECEIPT_QUANTITY     
			, CONVERT(decimal(38, 20), [Total Goods Receipt Value]) 		AS TOTAL_GOODS_RECEIPT_VALUE
			, CONVERT(decimal(38, 20), [Total Goods Issue Quantity]) 		AS TOTAL_GOODS_ISSUE_QUANTITY
			, CONVERT(decimal(38, 20), [Total Goods Issue Value]) 			AS TOTAL_GOODS_ISSUE_VALUE
			, CONVERT(decimal(38, 20), [Stock Quantity on Period End]) 		AS STOCK_QUANTITY_ON_PERIOD_END     
			, CONVERT(decimal(38, 20), [Stock Value on Period End]) 		AS STOCK_VALUE_ON_PERIOD_END
			, CONVERT(decimal(38, 20), [Receipt From Purchase Quantity]) 	AS RECEIPT_FROM_PURCHASE_QUANTITY 
			, CONVERT(decimal(38, 20), [Receipt From Purchase Value]) 		AS RECEIPT_FROM_PURCHASE_VALUE       
			, CONVERT(decimal(38, 20), [Issue To Sales Quantity]) 			AS ISSUE_TO_SALES_QUANTITY
			, CONVERT(decimal(38, 20), [Issue To Sales Value]) 				AS ISSUE_TO_SALES_VALUE
			, CONVERT(decimal(38, 20), [Receipt From Manuf. Quantity]) 		AS RECEIPT_FROM_MANUFACT_QUANTITY     
			, CONVERT(decimal(38, 20), [Receipt From Manuf. Value]) 		AS RECEIPT_FROM_MANUFACT_VALUE
			, CONVERT(decimal(38, 20), [Issue To Manuf. Quantity]) 			AS ISSUE_TO_MANUFACT_QUANTITY
			, CONVERT(decimal(38, 20), [Issue To Manuf. Value]) 			AS ISSUE_TO_MANUFACT_VALUE
			, CONVERT(decimal(38, 20), [Receipt From Transfer Quantity]) 	AS RECEIPT_FROM_TRANSFER_QUANTITY 
			, CONVERT(decimal(38, 20), [Receipt From Transfer Value]) 		AS RECEIPT_FROM_TRANSFER_VALUE       
			, CONVERT(decimal(38, 20), [Issue To Transfer Quantity]) 		AS ISSUE_TO_TRANSFER_QUANTITY
			, CONVERT(decimal(38, 20), [Issue To Transfer Value]) 			AS ISSUE_TO_TRANSFER_VALUE
			, CONVERT(decimal(38, 20), [Quantity of Other Received Goods])	AS QUANTITY_OF_OTHER_RECEIVED_GOODS
			, CONVERT(decimal(38, 20), [Value of Other Received Goods]) 	AS VALUE_OF_OTHER_RECEIVED_GOODS   
			, CONVERT(decimal(38, 20), [Quantity of Other Issued Goods]) 	AS QUANTITY_OF_OTHER_ISSUED_GOODS 
			, CONVERT(decimal(38, 20), [Value of Other Issued Goods]) 		AS VALUE_OF_OTHER_ISSUED_GOODS       
			, CONVERT(decimal(38, 20), [Issue To Internal Purposes Qty]) 	AS ISSUE_TO_INTERNAL_PURPOSES_QTY 
			, CONVERT(decimal(38, 20), [Issue To Internal Purposes Val]) 	AS ISSUE_TO_INTERNAL_PURPOSES_VAL 
			, CONVERT(decimal(38, 20), [Debit Revaluation]) 				AS DEBIT_REVALUATION
			, CONVERT(decimal(38, 20), [Credit Revaluation]) 				AS CREDIT_REVALUATION

			, [FILE_PATH]
			, CONVERT(INT, SUBSTRING([FILE_PATH], 15, 6))					AS [PERIOD]

			, 'N' 															AS [W_DELETE_FLG]
			, 3 															AS [W_DATASOURCE_NUM_ID]
			, DATEADD(HH, 7, GETDATE()) 									AS [W_INSERT_DT]
			, DATEADD(HH, 7, GETDATE()) 									AS [W_UPDATE_DT]
			, @p_batch_id													AS [W_BATCH_ID]
			, CONCAT([Material], '~', [Plant], '~',
					[Storage location], '~', SUBSTRING([FILE_PATH], 15, 6))	AS W_INTEGRATION_ID
		FROM STG.W_EXCEL_SPP_J3RFLVMOBVEDH_FS
		WHERE 1=1
			AND [Material] IS NOT NULL
			AND [Plant] IS NOT NULL
			AND [Storage location] IS NOT NULL
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

		SET @src_rownum = (SELECT COUNT(1) FROM [STG].[W_EXCEL_SPP_J3RFLVMOBVEDH_FS] WHERE W_BATCH_ID = @p_batch_id);
		SET @tgt_rownum = (SELECT COUNT(1) FROM FND.W_EXCEL_SPP_J3RFLVMOBVEDH_F WHERE W_DELETE_FLG = 'N' AND  W_BATCH_ID = @p_batch_id);

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
