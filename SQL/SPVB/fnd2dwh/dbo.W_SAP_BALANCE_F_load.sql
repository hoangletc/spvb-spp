SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (OBJECT_ID('[dbo].[proc_load_w_sap_balance_f]') is not null)
BEGIN
    DROP PROCEDURE [dbo].[proc_load_w_sap_balance_f]
END;
GO

CREATE PROC [dbo].[proc_load_w_sap_balance_f]
    @p_batch_id [bigint]
AS 
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_SAP_BALANCE_F',
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

        IF OBJECT_ID(N'tempdb..#W_SAP_BALANCE_F_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_SAP_BALANCE_F_tmp'
            DROP Table #W_SAP_BALANCE_F_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

		;WITH MATDOC_EXTENDED AS (
			SELECT
				PL_X.PLANT_WID
				, M.LGORT
				, MANDT
				, MBLNR
				, ZEILE
			FROM [FND].[W_SAP_MATDOC_F_TEMP] M
			LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL_X ON 1=1
				AND PL_X.LGORT = M.LGORT
		)

			SELECT
				ACDOCA.BUDAT                                    AS DATE_WID
				, ISNULL(PRODUCT.PRODUCT_WID, 0)                AS PRODUCT_WID
				, ISNULL(M_X.PLANT_WID, 0)                      AS PLANT_WID
				, ISNULL(CC.ASSET_WID, 0)                       AS COST_CENTER_WID
				
				, CONVERT(NVARCHAR(20), ACDOCA.WERKS)           AS PLANT_CODE
				, CONVERT(NVARCHAR(20), ACDOCA.BWTAR)           AS VALUATION_TYPE
				, CONVERT(nvarchar(100), REPLACE(LTRIM(
					REPLACE(ACDOCA.MATNR, '0', ' ')), ' ', '0')
				)                                               AS MATERIAL_NUMBER
				, CONVERT(NVARCHAR(20), RCLNT)                  AS CLIENT_CODE
				, CONVERT(NVARCHAR(20), RLDNR)                  AS LEDGER_CODE
				, CONVERT(NVARCHAR(20), M_X.LGORT)              AS STORAGE_LOCATION
				, CONVERT(NVARCHAR(20), RBUKRS)                 AS COMPANY_CODE
				, CONVERT(NVARCHAR(20), ACDOCA.BWKEY)           AS VALUATION_AREA
				, NULL                                          AS VALUATION_CLASS
				, CONVERT(NVARCHAR(8), MAT_DAT.MTART)           AS MATERIAL_TYPE
				, CONVERT(NVARCHAR(8), MAT_DAT.MATKL)           AS MATERIAL_GROUP
				, NULL                                          AS PURCHASING_GROUP
				-- , CONVERT(nvarchar(100), MAT_DES.MAKTX) AS MATERIAL_DESCRIPTION
				, CONVERT(NVARCHAR(5), RRUNIT)                  AS BASE_UNIT_OF_MEASURE
				, CONVERT(NVARCHAR(5), ACDOCA.VPRSV)            AS PRICE_CONTROL
				, CASE WHEN ACDOCA.BLDAT = '00000000' THEN NULL 
					ELSE CONVERT(DATE, ACDOCA.BLDAT) END 		AS DOCUMENT_DATE
				, CONVERT(NVARCHAR(15), ACDOCA.BELNR)           AS DOCUMENT_NUMBER
				, CONVERT(NVARCHAR(10), DOCLN)                  AS LINE_ITEM
				, CONVERT(NVARCHAR(5), RUNIT)                   AS UNIT
				, CONVERT(NVARCHAR(5), RVUNIT)                  AS BASE_UNIT
				, CONVERT(NVARCHAR(5), RTCUR)                   AS CURRENCY
				, CONVERT(NVARCHAR(20), RACCT)                  AS ACCOUNT_NUMBER
				, CONVERT(NVARCHAR(5), DRCRK)                   AS DEBIT_INDICATOR

				, CONVERT(NVARCHAR(20), RCNTR)                  AS COST_CENTER
				, CONVERT(NVARCHAR(50), CC.KTEXT)               AS COST_CENTER_DESC

				, CONVERT(INT, MSL)								AS QUANTITY
				, CONVERT(DECIMAL(38, 20), HSL)					AS LOCAL_AMOUNT

				, CONCAT_WS('~', RCLNT, RLDNR, RBUKRS, 
							ACDOCA.GJAHR, ACDOCA.BELNR, DOCLN)  AS W_INTEGRATION_ID
				, 'N'                                           AS W_DELETE_FLG
				, 1                                             AS W_DATASOURCE_NUM_ID
				, GETDATE()                                     AS W_INSERT_DT
				, GETDATE()                                     AS W_UPDATE_DT
				, NULL                                          AS W_BATCH_ID
				, 'N'                                           AS W_UPDATE_FLG
			INTO #W_SAP_BALANCE_F_tmp
			FROM [FND].[W_SAP_ACDOCA_SPP_F] ACDOCA

				LEFT JOIN [dbo].[W_PRODUCT_D] PRODUCT ON 1=1
					AND REPLACE(LTRIM(REPLACE(ACDOCA.MATNR, '0', ' ')), ' ', '0') = PRODUCT.PRODUCT_CODE
					AND PRODUCT.W_DATASOURCE_NUM_ID = 1
				LEFT JOIN [FND].[W_SAP_MARA_D] MAT_DAT ON 1=1
					AND MAT_DAT.MATNR = ACDOCA.MATNR
				LEFT JOIN [dbo].[W_SAP_CSKS_D] CV ON 1=1
					AND CV.MANDT = ACDOCA.RCLNT
					AND CV.BUKRS = ACDOCA.RBUKRS
					AND CV.WERKS = ACDOCA.WERKS
				LEFT JOIN [dbo].[W_SAP_CSKT_D] CC ON 1=1
					AND CC.KOSTL = ACDOCA.RCNTR
				LEFT JOIN MATDOC_EXTENDED M_X ON 1=1
					AND M_X.MANDT = ACDOCA.RCLNT
					AND M_X.MBLNR = ACDOCA.AWREF
					AND CONCAT('00', M_X.ZEILE) = ACDOCA.AWITEM

			WHERE 1=1
				AND ACDOCA.RCLNT = '300'
				AND RLDNR = '0L'
				AND ACDOCA.RACCT in ('0000120050', '0000530302','0000530301','0000530303','0000530304','0000530305' ,'0000530300')
				AND ACDOCA.VPRSV = 'V'
				AND (
					REPLACE(LTRIM(REPLACE(ACDOCA.MATNR, '0', ' ')), ' ', '0') LIKE '6%'
					OR REPLACE(LTRIM(REPLACE(ACDOCA.MATNR, '0', ' ')), ' ', '0') LIKE '9%'
				)



        -- 3. Update main table using W_INTEGRATION_ID
        PRINT '3. Update main table using W_INTEGRATION_ID'

        -- 3.1. Mark existing records by flag 'Y'
        PRINT '3.1. Mark existing records by flag ''Y'''

        UPDATE #W_SAP_BALANCE_F_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_SAP_BALANCE_F_tmp tg
        INNER JOIN [dbo].[W_SAP_BALANCE_F] sc 
        ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

        -- 3.2. Start updating
        PRINT '3.2. Start updating'

		UPDATE  [dbo].[W_SAP_BALANCE_F]
		SET 
			DATE_WID = src.DATE_WID
			, PRODUCT_WID = src.PRODUCT_WID
			, PLANT_WID = src.PLANT_WID
			, COST_CENTER_WID = src.COST_CENTER_WID

			, PLANT_CODE = src.PLANT_CODE
			, VALUATION_TYPE = src.VALUATION_TYPE
			, MATERIAL_NUMBER = src.MATERIAL_NUMBER
			, CLIENT_CODE = src.CLIENT_CODE
			, LEDGER_CODE = src.LEDGER_CODE
			, STORAGE_LOCATION = src.STORAGE_LOCATION
			, COMPANY_CODE = src.COMPANY_CODE
			, VALUATION_AREA = src.VALUATION_AREA
			, VALUATION_CLASS = src.VALUATION_CLASS
			, MATERIAL_TYPE = src.MATERIAL_TYPE
			, MATERIAL_GROUP = src.MATERIAL_GROUP
			, PURCHASING_GROUP = src.PURCHASING_GROUP
			, BASE_UNIT_OF_MEASURE = src.BASE_UNIT_OF_MEASURE
			, PRICE_CONTROL = src.PRICE_CONTROL
			, DOCUMENT_DATE = src.DOCUMENT_DATE
			, DOCUMENT_NUMBER = src.DOCUMENT_NUMBER
			, LINE_ITEM = src.LINE_ITEM
			, UNIT = src.UNIT
			, BASE_UNIT = src.BASE_UNIT
			, CURRENCY = src.CURRENCY
			, ACCOUNT_NUMBER = src.ACCOUNT_NUMBER
			, DEBIT_INDICATOR = src.DEBIT_INDICATOR
			, COST_CENTER = src.COST_CENTER
			, COST_CENTER_DESC = src.COST_CENTER_DESC
			, QUANTITY = src.QUANTITY
			, LOCAL_AMOUNT = src.LOCAL_AMOUNT

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = getdate()
        FROM [dbo].[W_SAP_BALANCE_F] tgt
        INNER JOIN #W_SAP_BALANCE_F_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


	    -- 4. Insert non-existed records to main table from temp table
        PRINT '4. Insert non-existed records to main table from temp table'

        INSERT INTO [dbo].[W_SAP_BALANCE_F](
            DATE_WID
			, PRODUCT_WID
			, PLANT_WID
			, COST_CENTER_WID

			, PLANT_CODE
			, VALUATION_TYPE
			, MATERIAL_NUMBER
			, CLIENT_CODE
			, LEDGER_CODE
			, STORAGE_LOCATION
			, COMPANY_CODE
			, VALUATION_AREA
			, VALUATION_CLASS
			, MATERIAL_TYPE
			, MATERIAL_GROUP
			, PURCHASING_GROUP
			, BASE_UNIT_OF_MEASURE
			, PRICE_CONTROL
			, DOCUMENT_DATE
			, DOCUMENT_NUMBER
			, LINE_ITEM
			, UNIT
			, BASE_UNIT
			, CURRENCY
			, ACCOUNT_NUMBER
			, DEBIT_INDICATOR
			, COST_CENTER
			, COST_CENTER_DESC
			, QUANTITY
			, LOCAL_AMOUNT

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
			, COST_CENTER_WID

			, PLANT_CODE
			, VALUATION_TYPE
			, MATERIAL_NUMBER
			, CLIENT_CODE
			, LEDGER_CODE
			, STORAGE_LOCATION
			, COMPANY_CODE
			, VALUATION_AREA
			, VALUATION_CLASS
			, MATERIAL_TYPE
			, MATERIAL_GROUP
			, PURCHASING_GROUP
			, BASE_UNIT_OF_MEASURE
			, PRICE_CONTROL
			, DOCUMENT_DATE
			, DOCUMENT_NUMBER
			, LINE_ITEM
			, UNIT
			, BASE_UNIT
			, CURRENCY
			, ACCOUNT_NUMBER
			, DEBIT_INDICATOR
			, COST_CENTER
			, COST_CENTER_DESC
			, QUANTITY
			, LOCAL_AMOUNT

            , W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
        FROM #W_SAP_BALANCE_F_tmp
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
            FROM W_SAP_BALANCE_F
        ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

		SET @src_rownum = ( SELECT COUNT(1) FROM #W_SAP_BALANCE_F_tmp );
		SET @tgt_rownum = ( 
            SELECT 
                COUNT(DISTINCT W_INTEGRATION_ID)
            FROM W_SAP_BALANCE_F
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