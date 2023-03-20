SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[EXCEL_proc_load_w_spp_j3rflvmobvedh_f] @p_batch_id [bigint] AS 
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_EXCEL_SPP_J3RFLVMOBVEDH_F',
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

        IF OBJECT_ID(N'tempdb..#W_EXCEL_SPP_J3RFLVMOBVEDH_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_EXCEL_SPP_J3RFLVMOBVEDH_tmp'
            DROP Table #W_EXCEL_SPP_J3RFLVMOBVEDH_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

		SELECT
			  CONVERT(VARCHAR, 
			  	EOMONTH(CONVERT(VARCHAR, [PERIOD]) + '01'),
				112
			)										AS DATE_WID
			, ISNULL(P.PRODUCT_WID,0)				AS MATERIAL_WID
			, ISNULL(PL.PLANT_WID, 0)				AS PLANT_WID

			, [MATERIAL]
			, [VALUATION_TYPE]
			, F.[PLANT]
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
			
			, F.W_INTEGRATION_ID                    AS W_INTEGRATION_ID
			, 'N'                                   AS W_DELETE_FLG
			, 'N' 									AS W_UPDATE_FLG
			, 3                                     AS W_DATASOURCE_NUM_ID
			, DATEADD(HH, 7, GETDATE())             AS W_INSERT_DT
			, DATEADD(HH, 7, GETDATE())             AS W_UPDATE_DT
			, @p_batch_id                           AS W_BATCH_ID
		INTO #W_EXCEL_SPP_J3RFLVMOBVEDH_tmp
		FROM FND.W_EXCEL_SPP_J3RFLVMOBVEDH_F F
			LEFT JOIN [dbo].[W_PRODUCT_D] P ON 1=1
				AND P.PRODUCT_CODE = F.MATERIAL
				AND P.W_DATASOURCE_NUM_ID = 1
			LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL ON 1=1
				AND PL.STO_LOC = F.STORAGE_LOCATION
				AND PL.PLANT = F.PLANT
		;


		-- 3. Update main table using W_INTEGRATION_ID
		PRINT '3. Update main table using W_INTEGRATION_ID'

		-- 3.1. Mark existing records by flag 'Y'
		PRINT '3.1. Mark existing records by flag ''Y'''

		UPDATE #W_EXCEL_SPP_J3RFLVMOBVEDH_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_EXCEL_SPP_J3RFLVMOBVEDH_tmp tg
		INNER JOIN [dbo].[W_EXCEL_SPP_J3RFLVMOBVEDH_F] sc 
		ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

		-- 3.2. Start updating
		PRINT '3.2. Start updating'

		UPDATE  [dbo].[W_EXCEL_SPP_J3RFLVMOBVEDH_F]
		SET 
			 [PLANT_WID] = src.[PLANT_WID]
			, [MATERIAL_WID] = src.[MATERIAL_WID]
			, [DATE_WID] = src.[DATE_WID]

			, [MATERIAL] = src.[MATERIAL]
			, [VALUATION_TYPE] = src.[VALUATION_TYPE]
			, [PLANT] = src.[PLANT]
			, [STORAGE_LOCATION] = src.[STORAGE_LOCATION]
			, [BATCH] = src.[BATCH]
			, [COMPANY_CODE] = src.[COMPANY_CODE]
			, [VALUATION_AREA] = src.[VALUATION_AREA]
			, [VALUATION_CLASS] = src.[VALUATION_CLASS]
			, [GL_ACCOUNT] = src.[GL_ACCOUNT]
			, [MATERIAL_TYPE] = src.[MATERIAL_TYPE]
			, [MATERIAL_GROUP] = src.[MATERIAL_GROUP]
			, [EXT_MATERIAL_GROUP] = src.[EXT_MATERIAL_GROUP]
			, [OLD_MATERIAL_NUMBER] = src.[OLD_MATERIAL_NUMBER]
			, [PURCHASING_GROUP] = src.[PURCHASING_GROUP]
			, [MATERIAL_DESCRIPTION] = src.[MATERIAL_DESCRIPTION]
			, [BASE_UNIT_OF_MEASURE] = src.[BASE_UNIT_OF_MEASURE]
			, [CURRENCY] = src.[CURRENCY]
			, [PRICE_CONTROL] = src.[PRICE_CONTROL]
			, [STOCK_QUANTITY_ON_PERIOD_START] = src.[STOCK_QUANTITY_ON_PERIOD_START]
			, [VALUE_ON_PERIOD_START] = src.[VALUE_ON_PERIOD_START]
			, [TOTAL_GOODS_RECEIPT_QUANTITY] = src.[TOTAL_GOODS_RECEIPT_QUANTITY]
			, [TOTAL_GOODS_RECEIPT_VALUE] = src.[TOTAL_GOODS_RECEIPT_VALUE]
			, [TOTAL_GOODS_ISSUE_QUANTITY] = src.[TOTAL_GOODS_ISSUE_QUANTITY]
			, [TOTAL_GOODS_ISSUE_VALUE] = src.[TOTAL_GOODS_ISSUE_VALUE]
			, [STOCK_QUANTITY_ON_PERIOD_END] = src.[STOCK_QUANTITY_ON_PERIOD_END]
			, [STOCK_VALUE_ON_PERIOD_END] = src.[STOCK_VALUE_ON_PERIOD_END]
			, [RECEIPT_FROM_PURCHASE_QUANTITY] = src.[RECEIPT_FROM_PURCHASE_QUANTITY]
			, [RECEIPT_FROM_PURCHASE_VALUE] = src.[RECEIPT_FROM_PURCHASE_VALUE]
			, [ISSUE_TO_SALES_QUANTITY] = src.[ISSUE_TO_SALES_QUANTITY]
			, [ISSUE_TO_SALES_VALUE] = src.[ISSUE_TO_SALES_VALUE]
			, [RECEIPT_FROM_MANUFACT_QUANTITY] = src.[RECEIPT_FROM_MANUFACT_QUANTITY]
			, [RECEIPT_FROM_MANUFACT_VALUE] = src.[RECEIPT_FROM_MANUFACT_VALUE]
			, [ISSUE_TO_MANUFACT_QUANTITY] = src.[ISSUE_TO_MANUFACT_QUANTITY]
			, [ISSUE_TO_MANUFACT_VALUE] = src.[ISSUE_TO_MANUFACT_VALUE]
			, [RECEIPT_FROM_TRANSFER_QUANTITY] = src.[RECEIPT_FROM_TRANSFER_QUANTITY]
			, [RECEIPT_FROM_TRANSFER_VALUE] = src.[RECEIPT_FROM_TRANSFER_VALUE]
			, [ISSUE_TO_TRANSFER_QUANTITY] = src.[ISSUE_TO_TRANSFER_QUANTITY]
			, [ISSUE_TO_TRANSFER_VALUE] = src.[ISSUE_TO_TRANSFER_VALUE]
			, [QUANTITY_OF_OTHER_RECEIVED_GOODS] = src.[QUANTITY_OF_OTHER_RECEIVED_GOODS]
			, [VALUE_OF_OTHER_RECEIVED_GOODS] = src.[VALUE_OF_OTHER_RECEIVED_GOODS]
			, [QUANTITY_OF_OTHER_ISSUED_GOODS] = src.[QUANTITY_OF_OTHER_ISSUED_GOODS]
			, [VALUE_OF_OTHER_ISSUED_GOODS] = src.[VALUE_OF_OTHER_ISSUED_GOODS]
			, [ISSUE_TO_INTERNAL_PURPOSES_QTY] = src.[ISSUE_TO_INTERNAL_PURPOSES_QTY]
			, [ISSUE_TO_INTERNAL_PURPOSES_VAL] = src.[ISSUE_TO_INTERNAL_PURPOSES_VAL]
			, [DEBIT_REVALUATION] = src.[DEBIT_REVALUATION]
			, [CREDIT_REVALUATION] = src.[CREDIT_REVALUATION]

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = DATEADD(HH, 7, GETDATE())
		FROM [dbo].[W_EXCEL_SPP_J3RFLVMOBVEDH_F] tgt
		INNER JOIN #W_EXCEL_SPP_J3RFLVMOBVEDH_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


		-- 4. Insert non-existed records to main table from temp table
		PRINT '4. Insert non-existed records to main table from temp table'

		INSERT INTO [dbo].[W_EXCEL_SPP_J3RFLVMOBVEDH_F](
			[PLANT_WID]
			, [MATERIAL_WID]
			, [DATE_WID]

			, [MATERIAL]
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

			, W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
		)
		SELECT
			[PLANT_WID]
			, [MATERIAL_WID]
			, [DATE_WID]

			, [MATERIAL]
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

			, W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
		FROM #W_EXCEL_SPP_J3RFLVMOBVEDH_tmp
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
            FROM W_EXCEL_SPP_J3RFLVMOBVEDH_F
        ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

		SET @src_rownum = ( SELECT COUNT(1) FROM #W_EXCEL_SPP_J3RFLVMOBVEDH_tmp );
		SET @tgt_rownum = ( 
            SELECT 
                COUNT(DISTINCT W_INTEGRATION_ID)
            FROM W_EXCEL_SPP_J3RFLVMOBVEDH_F
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
