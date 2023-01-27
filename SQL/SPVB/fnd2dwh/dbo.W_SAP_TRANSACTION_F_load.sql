SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (OBJECT_ID('[dbo].[proc_load_w_sap_transaction_f]') is not null)
BEGIN
    DROP PROCEDURE [dbo].[proc_load_w_sap_transaction_f]
END;
GO

CREATE PROC [dbo].[proc_load_w_sap_transaction_f]
    @p_batch_id [bigint]
AS 
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_SAP_TRANSACTION_F',
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

        IF OBJECT_ID(N'tempdb..#W_SAP_TRANSACTION_F_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_SAP_TRANSACTION_F_tmp'
            DROP Table #W_SAP_TRANSACTION_F_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

		SELECT
            CONVERT(nvarchar(100), BUDAT) AS DATE_WID
			, ISNULL(P.PRODUCT_WID, 0) AS PRODUCT_WID
			, ISNULL(PL_X.PLANT_WID, 0) AS PLANT_WID
			, ISNULL(WO.WO_WID, 0) AS WO_WID

			, CONVERT(nvarchar(100), LIFNR) AS VENDOR_CODE
			, CONVERT(nvarchar(100), MD.WERKS) AS PLANT_CODE
			, CONVERT(nvarchar(100), BUKRS) AS COMPANY_CODE
			, CONVERT(nvarchar(100), BSTAUS_SG) AS STOCK_STATUS
			, CONVERT(nvarchar(100), WAERS) AS CURRENCY_CODE
			, CONVERT(nvarchar(100), MEINS) AS BASE_UNIT
			, CASE WHEN WAERS IN ('VND', 'JPY') THEN CONVERT(DECIMAL(38, 20), DMBTR_STOCK) * 100 ELSE CONVERT(DECIMAL(38, 20), DMBTR_STOCK) END AS STOCK_VALUE
			, CONVERT(DECIMAL(38,20), STOCK_QTY) AS STOCK_QUANTITY_IN_BASE_UNIT
			, CONVERT(nvarchar(100), ERFME) AS ENTRY_UNIT
			, CONVERT(DECIMAL(38,20), ERFMG) AS STOCK_QUANTITY_IN_ENTRY_UNIT
			, CONVERT(DATE, BUDAT) AS POSTING_DATE
			, CONVERT(DATE, CPUDT) AS CREATE_DATE
			, CONVERT(DATE, BLDAT) AS DOCUMENT_DATE
			, CASE WHEN AEDAT = '00000000' THEN NULL ELSE CONVERT(DATE, AEDAT) END AS UPDATE_DATE
			, CONVERT(nvarchar(100), MBLNR) AS DOCUMENT_NUMBER
			, CONVERT(nvarchar(100), ZEILE) AS DOCUMENT_LINE_ITEM
			, CONVERT(nvarchar(100), LINE_ID) AS LINE_ID
			, CONVERT(nvarchar(100), EBELN) AS PURCHASE_DOCUMENT
			, CONVERT(nvarchar(100), EBELP) AS PURCHASE_LINE_ITEM
			, CONVERT(nvarchar(100), SMBLN) AS ORIGINAL_DOCUMENT_NUM
			, CONVERT(nvarchar(100), SMBLP) AS ORIGINAL_DOCUMENT_LINE
			, CONVERT(nvarchar(100), XBLNR) AS REFERENCE_DOCUMENT
			, CONVERT(nvarchar(100), VBELN_IM) AS DELIVERY_DOCUMENT
			, CONVERT(nvarchar(100), VBELP_IM) AS DELIVER_LINE_ITEM
			, CONVERT(nvarchar(100), XAUTO) AS IS_AUTO_FLG
			, CONVERT(nvarchar(100), REPLACE(LTRIM(REPLACE(MD.MATNR, '0', ' ')), ' ', '0')) AS MATERIAL_NUMBER
			, CONVERT(nvarchar(100), MD.LGORT) AS STORAGE_LOCATION
			, CONVERT(nvarchar(100), CHARG) AS BATCH_NUMBER
			, CONVERT(nvarchar(100), BWTAR) AS VALUATION_TYPE
			, CONVERT(nvarchar(100), SHKZG) AS CR_DR_FLG
			, CONVERT(nvarchar(100), SGTXT) AS [TEXT]
			, CONVERT(nvarchar(100), MD.VPRSV) AS PRICE_INDICATOR
			, CONVERT(nvarchar(100), BLART) AS DOCUMENT_TYPE
			, CONVERT(nvarchar(100), PRCTR) AS PROFIT_CENTER
			, CONVERT(nvarchar(100), KOSTL) AS COST_CENTER
			, CONVERT(nvarchar(100), ABLAD) AS UPLOADING_POINT
			, CONVERT(nvarchar(100), MD.BWART) AS MOVEMENT_TYPE

			, CONVERT([varbinary](500), CONCAT(KEY1, '~', KEY2, '~', KEY3, '~' , KEY4, '~', KEY5, '~', KEY6)) AS W_INTEGRATION_ID
			, 'N' AS W_DELETE_FLG
			, 1 AS W_DATASOURCE_NUM_ID
			, GETDATE() AS W_INSERT_DT
			, GETDATE() AS W_UPDATE_DT
			, NULL AS W_BATCH_ID
            , 'N' AS W_UPDATE_FLG
        INTO #W_SAP_TRANSACTION_F_tmp
        FROM [FND].[W_SAP_MATDOC_F_temp] MD
			LEFT JOIN [dbo].[W_PRODUCT_D] P ON 1=1
				AND REPLACE(LTRIM(REPLACE(MD.MATNR, '0', ' ')), ' ', '0') = P.PRODUCT_CODE
				AND P.W_DATASOURCE_NUM_ID = 1
			LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL_X ON 1=1
				AND PL_X.LGORT = MD.LGORT
			LEFT JOIN [FND].[W_CMMS_WO_F] WO ON 1=1
				AND WO.WONUM =  MD.ABLAD
			LEFT JOIN [dbo].[W_VENDOR_D] as V on 1=1
				AND V.VENDOR_CODE = MD.LIFNR 
				AND V.CLIENT_CODE = '300'
		WHERE  1=1
			AND MD.MANDT = '300'
			AND (
				REPLACE(LTRIM(REPLACE(MD.MATNR, '0', ' ')), ' ', '0') LIKE '6%'
				OR REPLACE(LTRIM(REPLACE(MD.MATNR, '0', ' ')), ' ', '0') LIKE '9%'
			)


        -- 3. Update main table using W_INTEGRATION_ID
        PRINT '3. Update main table using W_INTEGRATION_ID'

        -- 3.1. Mark existing records by flag 'Y'
        PRINT '3.1. Mark existing records by flag ''Y'''

        UPDATE #W_SAP_TRANSACTION_F_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_SAP_TRANSACTION_F_tmp tg
        INNER JOIN [dbo].[W_SAP_TRANSACTION_F] sc 
        ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

        -- 3.2. Start updating
        PRINT '3.2. Start updating'

		UPDATE  [dbo].[W_SAP_TRANSACTION_F]
		SET DATE_WID = src.DATE_WID
			, PRODUCT_WID = src.PRODUCT_WID
			, PLANT_WID = src.PLANT_WID
			, WO_WID = src.WO_WID

			, VENDOR_CODE = src.VENDOR_CODE
			, PLANT_CODE = src.PLANT_CODE
			, COMPANY_CODE = src.COMPANY_CODE
			, STOCK_STATUS = src.STOCK_STATUS
			, CURRENCY_CODE = src.CURRENCY_CODE
			, STOCK_VALUE = src.STOCK_VALUE
			, BASE_UNIT = src.BASE_UNIT
			, STOCK_QUANTITY_IN_BASE_UNIT = src.STOCK_QUANTITY_IN_BASE_UNIT
			, ENTRY_UNIT = src.ENTRY_UNIT
			, STOCK_QUANTITY_IN_ENTRY_UNIT = src.STOCK_QUANTITY_IN_ENTRY_UNIT
			, POSTING_DATE = src.POSTING_DATE
			, CREATE_DATE = src.CREATE_DATE
			, DOCUMENT_DATE = src.DOCUMENT_DATE
			, UPDATE_DATE = src.UPDATE_DATE
			, DOCUMENT_NUMBER = src.DOCUMENT_NUMBER
			, DOCUMENT_LINE_ITEM = src.DOCUMENT_LINE_ITEM
			, LINE_ID = src.LINE_ID
			, PURCHASE_DOCUMENT = src.PURCHASE_DOCUMENT
			, PURCHASE_LINE_ITEM = src.PURCHASE_LINE_ITEM
			, ORIGINAL_DOCUMENT_NUM = src.ORIGINAL_DOCUMENT_NUM
			, ORIGINAL_DOCUMENT_LINE = src.ORIGINAL_DOCUMENT_LINE
			, REFERENCE_DOCUMENT = src.REFERENCE_DOCUMENT
			, DELIVERY_DOCUMENT = src.DELIVERY_DOCUMENT
			, DELIVER_LINE_ITEM = src.DELIVER_LINE_ITEM
			, IS_AUTO_FLG = src.IS_AUTO_FLG
			, MATERIAL_NUMBER = src.MATERIAL_NUMBER
			, STORAGE_LOCATION = src.STORAGE_LOCATION
			, BATCH_NUMBER = src.BATCH_NUMBER
			, VALUATION_TYPE = src.VALUATION_TYPE
			, CR_DR_FLG = src.CR_DR_FLG
			, [TEXT] = src.[TEXT]
			, PRICE_INDICATOR = src.PRICE_INDICATOR
			, DOCUMENT_TYPE = src.DOCUMENT_TYPE
			, PROFIT_CENTER = src.PROFIT_CENTER
			, COST_CENTER = src.COST_CENTER
			, UPLOADING_POINT = src.UPLOADING_POINT
			, MOVEMENT_TYPE = src.MOVEMENT_TYPE

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = getdate()
        FROM [dbo].[W_SAP_TRANSACTION_F] tgt
        INNER JOIN #W_SAP_TRANSACTION_F_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


	    -- 4. Insert non-existed records to main table from temp table
        PRINT '4. Insert non-existed records to main table from temp table'

        INSERT INTO [dbo].[W_SAP_TRANSACTION_F](
            DATE_WID
			, PRODUCT_WID
			, PLANT_WID
			, WO_WID

			, VENDOR_CODE
			, PLANT_CODE
			, COMPANY_CODE
			, STOCK_STATUS
			, CURRENCY_CODE
			, STOCK_VALUE
			, BASE_UNIT
			, STOCK_QUANTITY_IN_BASE_UNIT
			, ENTRY_UNIT
			, STOCK_QUANTITY_IN_ENTRY_UNIT
			, POSTING_DATE
			, CREATE_DATE
			, DOCUMENT_DATE
			, UPDATE_DATE
			, DOCUMENT_NUMBER
			, DOCUMENT_LINE_ITEM
			, LINE_ID
			, PURCHASE_DOCUMENT
			, PURCHASE_LINE_ITEM
			, ORIGINAL_DOCUMENT_NUM
			, ORIGINAL_DOCUMENT_LINE
			, REFERENCE_DOCUMENT
			, DELIVERY_DOCUMENT
			, DELIVER_LINE_ITEM
			, IS_AUTO_FLG
			, MATERIAL_NUMBER
			, STORAGE_LOCATION
			, BATCH_NUMBER
			, VALUATION_TYPE
			, CR_DR_FLG
			, [TEXT]
			, PRICE_INDICATOR
			, DOCUMENT_TYPE
			, PROFIT_CENTER
			, COST_CENTER
			, UPLOADING_POINT
			, MOVEMENT_TYPE

            , W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
        )
        SELECT
            DATE_WID
			, PRODUCT_WID
			, PLANT_WID
			, WO_WID

			, VENDOR_CODE
			, PLANT_CODE
			, COMPANY_CODE
			, STOCK_STATUS
			, CURRENCY_CODE
			, STOCK_VALUE
			, BASE_UNIT
			, STOCK_QUANTITY_IN_BASE_UNIT
			, ENTRY_UNIT
			, STOCK_QUANTITY_IN_ENTRY_UNIT
			, POSTING_DATE
			, CREATE_DATE
			, DOCUMENT_DATE
			, UPDATE_DATE
			, DOCUMENT_NUMBER
			, DOCUMENT_LINE_ITEM
			, LINE_ID
			, PURCHASE_DOCUMENT
			, PURCHASE_LINE_ITEM
			, ORIGINAL_DOCUMENT_NUM
			, ORIGINAL_DOCUMENT_LINE
			, REFERENCE_DOCUMENT
			, DELIVERY_DOCUMENT
			, DELIVER_LINE_ITEM
			, IS_AUTO_FLG
			, MATERIAL_NUMBER
			, STORAGE_LOCATION
			, BATCH_NUMBER
			, VALUATION_TYPE
			, CR_DR_FLG
			, [TEXT]
			, PRICE_INDICATOR
			, DOCUMENT_TYPE
			, PROFIT_CENTER
			, COST_CENTER
			, UPLOADING_POINT
			, MOVEMENT_TYPE

            , W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
        FROM #W_SAP_TRANSACTION_F_tmp
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
            FROM W_SAP_TRANSACTION_F
        ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

		SET @src_rownum = ( SELECT COUNT(1) FROM #W_SAP_TRANSACTION_F_tmp );
		SET @tgt_rownum = ( 
            SELECT 
                COUNT(DISTINCT W_INTEGRATION_ID)
            FROM W_SAP_TRANSACTION_F
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