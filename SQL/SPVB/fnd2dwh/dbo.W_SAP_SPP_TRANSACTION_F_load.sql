SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[SAP_proc_load_w_spp_transaction_f] @p_batch_id [bigint] AS BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_SAP_SPP_TRANSACTION_F',
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

        IF OBJECT_ID(N'tempdb..#W_SAP_SPP_TRANSACTION_F_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_SAP_SPP_TRANSACTION_F_tmp'
            DROP Table #W_SAP_SPP_TRANSACTION_F_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

		;WITH MATDOC_EXTENDED AS (
			SELECT 
				PL_X.PLANT_WID
				, M.LGORT
				, M.DMBTR_STOCK
				, MANDT
				, MBLNR
				, ZEILE
				, BWART
				, ABLAD
				, WAERS
				, UMBAR
				, XAUTO
				, SMBLN
				, SMBLP
				, LINE_ID
				, PARENT_ID
				, KOSTL
				, XBLNR
				, SALK3
				, LbkUM
				, BNBTR
				, KZBEW
				, KZZUG
				, KZVBR
				, SOBKZ
			FROM [FND].[W_SAP_MATDOC_F_TEMP] M
			LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL_X ON 1=1
				AND PL_X.STo_LOC = CASE WHEN M.LGORT IS NULL THEN '' ELSE M.LGORT END
				AND M.WERKS = PL_X.PLANT
			--WHERE M.LGORT = ''
		), Reverse_doc AS (
				select F.RCLNT, RLDNR, RBUKRS, GJAHR, BELNR, DOCLN, R.BUDAT AS ORIGINAL_POSTING_DATE
				from [FND].[W_SAP_ACDOCA_SPP_F] F
					LEFT JOIN MATDOC_EXTENDED M ON M.MANDT = F.RCLNT AND M.MBLNR = F.AWREF AND CONCAT('00', ZEILE) = F.AWITEM
					LEFT JOIN 
						(select F.AWREF_REV,AWITEM, BUDAT,AWREF  from [FND].[W_SAP_ACDOCA_SPP_F] F where XREVERSING <> 'X' AND RACCT = '0000120050') R 
						ON F.AWREF = R.AWREF_REV AND F.AWITEM = R.AWITEM AND F.AWREF_REV = R.AWREF 
				where  F.XREVERSING = 'X'
				and F.RACCT = '0000120050' 
				AND M.BWART IN ('102')
		),
		TMP_WO AS (
			SELECT
				ISNULL(AST.ASSET_WID, 0)                        AS ASSET_WID
				, CONVERT(NVARCHAR(30), WO.WONUM)               AS WONUM
			FROM [FND].[W_CMMS_WO_F] WO
			OUTER APPLY (
				SELECT TOP 1
					LOCATION_WID, ASSET_WID
				FROM [dbo].[W_CMMS_ASSET_D] TMP_AST
				WHERE 1=1
					AND TMP_AST.[ASSET_NUM] = WO.[ASSETNUM]
					AND LEFT(TMP_AST.LOCATION, 3) = LEFT(WO.LOCATION, 3)
			) AST
			WHERE 1=1
				AND wo.ISTASK = 'False'
				AND wo.WORKTYPE IN ('PM', 'CM')
		)
			SELECT 
				F.BUDAT                                    		AS DATE_WID
				, ISNULL(PRODUCT.PRODUCT_WID, 0)                AS PRODUCT_WID
				, CASE WHEN M_X.PLANT_WID = 0 OR M_X.PLANT_WID IS NULL 
					THEN PL_X.PLANT_WID 
					ELSE M_X.PLANT_WID 
				END												AS PLANT_WID

				, ISNULL(COST_CENTER.COST_CENTER_WID, 0)        AS COST_CENTER_WID	
				, TMP_WO.ASSET_WID                              AS ASSET_WID
				, CONVERT(DATE, R.ORIGINAL_POSTING_DATE)		AS ORIGINAL_POSTING_DATE
				, CONVERT(NVARCHAR(20), F.WERKS)           		AS PLANT_CODE
				, CONVERT(NVARCHAR(20), F.BWTAR)           		AS VALUATION_TYPE
				, CONVERT(VARCHAR, REPLACE(LTRIM(REPLACE(F.MATNR, '0', ' ')), ' ', '0')) AS MATERIAL_NUMBER
				, CONVERT(NVARCHAR(20), F.RCLNT)                AS CLIENT_CODE
				, CONVERT(NVARCHAR(20), F.RLDNR)                AS LEDGER_CODE
				, CONVERT(NVARCHAR(20), M_X.LGORT)              AS STORAGE_LOCATION
				, CONVERT(NVARCHAR(20), F.RBUKRS)               AS COMPANY_CODE
				, CONVERT(NVARCHAR(20), F.BWKEY)           		AS VALUATION_AREA
				, '6000'                                        AS VALUATION_CLASS
				, CONVERT(NVARCHAR(8), MAT_DAT.MTART)           AS MATERIAL_TYPE
				, CONVERT(NVARCHAR(8), MAT_DAT.MATKL)           AS MATERIAL_GROUP
				, NULL                                          AS PURCHASING_GROUP
				, CONVERT(NVARCHAR(5), RRUNIT)                  AS BASE_UNIT_OF_MEASURE
				, CONVERT(NVARCHAR(5), F.VPRSV)            		AS PRICE_CONTROL
				, CASE WHEN F.BLDAT = '00000000' THEN NULL ELSE CONVERT(DATE, F.BLDAT) END  AS DOCUMENT_DATE
				, CONVERT(NVARCHAR(15), F.BELNR)           		AS DOCUMENT_NUMBER
				, CONVERT(NVARCHAR(10), F.DOCLN)                AS LINE_ITEM
				, CONVERT(NVARCHAR(5), RUNIT)                   AS UNIT
				, CONVERT(NVARCHAR(5), RVUNIT)                  AS BASE_UNIT
				, CONVERT(NVARCHAR(5), RTCUR)                   AS CURRENCY
				, CONVERT(NVARCHAR(20), RACCT)                  AS ACCOUNT_NUMBER
				, CONVERT(NVARCHAR(5), DRCRK)                   AS DEBIT_INDICATOR
				, CASE WHEN CONVERT(NVARCHAR(20), RCNTR) = '' THEN CONVERT(VARCHAR(50), KOSTL) ELSE CONVERT(NVARCHAR(20), RCNTR) END  AS COST_CENTER
				, CONVERT(NVARCHAR(50), COST_CENTER.COST_CENTER_DESC)   AS COST_CENTER_DESC
				, CONVERT(DECIMAL(38, 20), MSL)					AS QUANTITY
				, CONVERT(DECIMAL(38,20), LbkUM) 				AS OPENING_VOLUMN
				, CONVERT(NVARCHAR(50), AWREF) 					AS MATERIAL_DOCUMENT
				, CONVERT(NVARCHAR(50), AWITEM) 				AS MATERIAL_LINE
				, CASE 
					WHEN RHCUR IN ('VND', 'JPY') THEN CONVERT(DECIMAL(38, 20), M_X.DMBTR_STOCK) * 100
					ELSE CONVERT(DECIMAL(38, 20), M_X.DMBTR_STOCK)
				END	AS STOCK_VALUE
				, CASE 
					WHEN RHCUR IN ('VND', 'JPY') THEN CONVERT(DECIMAL(38, 20), M_X.SALK3) * 100
					ELSE CONVERT(DECIMAL(38, 20), M_X.SALK3)
				END	AS OPENING_VALUE
				, CASE 
					WHEN RHCUR IN ('VND', 'JPY') THEN CONVERT(DECIMAL(38, 20), HSL) * 100 
					ELSE CONVERT(DECIMAL(38, 20), HSL) 
				END	AS LOCAL_AMOUNT
				, CASE WHEN RHCUR IN ('VND', 'JPY') THEN CONVERT(DECIMAL(38,20), BNBTR) * 100 ELSE CONVERT(DECIMAL(38,20), BNBTR) END AS DELIVERY_COST 
				, CONVERT(VARCHAR(10), BWART) 					AS MOVEMENT_TYPE
				, CONVERT(VARCHAR(100), ABLAD) 					AS UPLOADING_POINT
				, CONVERT(VARCHAR(10),UMBAR) 					AS RECEIPT_STORAGE_LOCATION
				, CONVERT(VARCHAR(10), LINE_ID) 				AS LINE_ID
				, CONVERT(VARCHAR(10), PARENT_ID) 				AS PARENT_ID
				, CONVERT(varchar(20), F.EBELN)         		AS PURCHASE_DOCUMENT
				, CONVERT(varchar(20), F.EBELP) 		    	AS PURCHASE_LINE_ITEM
				, CONVERT(nvarchar(500), XREVERSING)            AS FLG_RESERVING
				, CONVERT(nvarchar(500), XREVERSED)             AS FLG_RESERVED
				, CONVERT(nvarchar(500), AWORG_REV)             AS FISCAL_YEAR_RESERVED
				, CONVERT(nvarchar(500), AWREF_REV)             AS REVERTED_DOCUMENT
				, CONVERT(NVARCHAR(500), SMBLN) 				AS ORIGINAL_DOCUMENT
				, CONVERT(NVARCHAR(500), SMBLP) 				AS ORIGINAL_LINE_ITEM
				, CONVERT(VARCHAR(200), XBLNR)  				AS REFERENCE_DOCUMENT
				, CONVERT(VARCHAR(20), KZBEW)					AS MOVEMENT_IND
				, CONVERT(VARCHAR(20), KZZUG)					AS RECEIPT_IND
				, CONVERT(VARCHAR(20), KZVBR)					AS CONSUMPTION_POSTING
				, CONVERT(VARCHAR(20), SOBKZ)					AS SPECIAL_IND

				, CONCAT( F.RCLNT,'~', F.RLDNR,'~', F.RBUKRS, '~',F.GJAHR, '~', F.BELNR, '~', F.DOCLN) AS W_INTEGRATION_ID
				, 'N'                                           AS W_DELETE_FLG
				, 1                                             AS W_DATASOURCE_NUM_ID
				, DATEADD(HH, 7, GETDATE())                     AS W_INSERT_DT
				, DATEADD(HH, 7, GETDATE())                     AS W_UPDATE_DT
				, @p_batch_id                                   AS W_BATCH_ID
				, 'N'                                           AS W_UPDATE_FLG
			INTO #W_SAP_SPP_TRANSACTION_F_tmp
			FROM [FND].[W_SAP_ACDOCA_SPP_F] F
				LEFT JOIN [dbo].[W_PRODUCT_D] PRODUCT ON 1=1
					AND REPLACE(LTRIM(REPLACE(F.MATNR, '0', ' ')), ' ', '0') = PRODUCT.PRODUCT_CODE
					AND PRODUCT.W_DATASOURCE_NUM_ID = 1

				LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL_X ON 1=1
					AND PL_X.STo_LOC = ''
					AND PL_X.PLANT = F.WERKS

				LEFT JOIN [FND].[W_SAP_MARA_D] MAT_DAT ON 1=1
					AND MAT_DAT.MATNR = F.MATNR
				LEFT JOIN MATDOC_EXTENDED M_X ON 1=1
					AND M_X.MANDT = F.RCLNT
					AND M_X.MBLNR = F.AWREF
					AND CONCAT('00', M_X.ZEILE) = F.AWITEM
				LEFT JOIN Reverse_doc R ON 1=1
					AND F.GJAHR = R.GJAHR AND F.BELNR = R.BELNR AND F.DOCLN = R.DOCLN
				LEFT JOIN [dbo].[W_SAP_COST_CENTER_D] COST_CENTER ON 1=1
					AND COST_CENTER.COST_CENTER = CASE WHEN CONVERT(NVARCHAR(20), RCNTR) = '' THEN CONVERT(VARCHAR(50), KOSTL) ELSE CONVERT(NVARCHAR(20), RCNTR) END
					AND COST_CENTER.CLIENT = '300' 
				LEFT JOIN TMP_WO ON 1=1
            		AND TMP_WO.WONUM = M_X.ABLAD
			WHERE 1=1
				AND F.RCLNT = '300'
				AND F.RLDNR = '0L'
				AND (
					(F.RACCT = '0000120050' AND REPLACE(LTRIM(REPLACE(F.MATNR, '0', ' ')), ' ', '0') LIKE '6%')
					OR (F.RACCT in ( '0000530302','0000530301','0000530303','0000530304','0000530305' ,'0000530300' ) 
						AND REPLACE(LTRIM(REPLACE(F.MATNR, '0', ' ')), ' ', '0') LIKE '9%'
						)
					)
				AND F.VPRSV = 'V'


        -- 3. Update main table using W_INTEGRATION_ID
        PRINT '3. Update main table using W_INTEGRATION_ID'

        -- 3.1. Mark existing records by flag 'Y'
        PRINT '3.1. Mark existing records by flag ''Y'''

        UPDATE #W_SAP_SPP_TRANSACTION_F_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_SAP_SPP_TRANSACTION_F_tmp tg
        INNER JOIN [dbo].[W_SAP_SPP_TRANSACTION_F] sc  ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

        -- 3.2. Start updating
        PRINT '3.2. Start updating'

		UPDATE  [dbo].[W_SAP_SPP_TRANSACTION_F]
		SET 
			  DATE_WID = src.DATE_WID
			, PRODUCT_WID = src.PRODUCT_WID
			, PLANT_WID = src.PLANT_WID
			, COST_CENTER_WID = src.COST_CENTER_WID
			, ASSET_WID = src.ASSET_WID
			, PLANT_CODE = src.PLANT_CODE
			, VALUATION_TYPE = src.VALUATION_TYPE
			, ORIGINAL_POSTING_DATE= SRC.ORIGINAL_POSTING_DATE
			, MATERIAL_NUMBER = src.MATERIAL_NUMBER
			, CLIENT_CODE = src.CLIENT_CODE
			, LEDGER_CODE = src.LEDGER_CODE
			, STORAGE_LOCATION = src.STORAGE_LOCATION
			, RECEIPT_STORAGE_LOCATION = SRC.RECEIPT_STORAGE_LOCATION
			, LINE_ID =SRC.LINE_ID
			, PARENT_ID = SRC.PARENT_ID
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
			, OPENING_VOLUMN = src.OPENING_VOLUMN
			, MATERIAL_DOCUMENT = src.MATERIAL_DOCUMENT
			, MATERIAL_LINE = src.MATERIAL_LINE
			, STOCK_VALUE = src.STOCK_VALUE
			, OPENING_VALUE = src.OPENING_VALUE
			, LOCAL_AMOUNT = src.LOCAL_AMOUNT
			, DELIVERY_COST = src.DELIVERY_COST
			, MOVEMENT_TYPE = src.MOVEMENT_TYPE
			, UPLOADING_POINT = src.UPLOADING_POINT
			, PURCHASE_DOCUMENT = src.PURCHASE_DOCUMENT
			, PURCHASE_LINE_ITEM = src.PURCHASE_LINE_ITEM
			, FLG_RESERVING = src.FLG_RESERVING
			, FLG_RESERVED = src.FLG_RESERVED
			, FISCAL_YEAR_RESERVED = src.FISCAL_YEAR_RESERVED
			, REVERTED_DOCUMENT = src.REVERTED_DOCUMENT
			, ORIGINAL_DOCUMENT = src.ORIGINAL_DOCUMENT
			, ORIGINAL_LINE_ITEM = src.ORIGINAL_LINE_ITEM
			, REFERENCE_DOCUMENT = src.REFERENCE_DOCUMENT
			, MOVEMENT_IND = src.MOVEMENT_IND
			, RECEIPT_IND = src.RECEIPT_IND
			, CONSUMPTION_POSTING = src.CONSUMPTION_POSTING
			, SPECIAL_IND = src.SPECIAL_IND

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = DATEADD(HH, 7, getdate())
        FROM [dbo].[W_SAP_SPP_TRANSACTION_F] tgt
        INNER JOIN #W_SAP_SPP_TRANSACTION_F_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID

--alter table [dbo].[W_SAP_SPP_TRANSACTION_F]
--add REFERENCE_DOCUMENT varchar(200)

	    -- 4. Insert non-existed records to main table from temp table
        PRINT '4. Insert non-existed records to main table from temp table'

        INSERT INTO [dbo].[W_SAP_SPP_TRANSACTION_F]
		(
              DATE_WID
			, PRODUCT_WID
			, PLANT_WID
			, COST_CENTER_WID
			, ASSET_WID
			, ORIGINAL_POSTING_DATE
			, PLANT_CODE
			, VALUATION_TYPE
			, MATERIAL_NUMBER
			, CLIENT_CODE
			, LEDGER_CODE
			, STORAGE_LOCATION
			, RECEIPT_STORAGE_LOCATION
			, LINE_ID
			, PARENT_ID
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
			, OPENING_VOLUMN
			, MATERIAL_DOCUMENT
			, MATERIAL_LINE
			, STOCK_VALUE
			, OPENING_VALUE
			, LOCAL_AMOUNT
			, DELIVERY_COST
			, MOVEMENT_TYPE
			, UPLOADING_POINT
			, PURCHASE_DOCUMENT
			, PURCHASE_LINE_ITEM
			, FLG_RESERVING
			, FLG_RESERVED
			, FISCAL_YEAR_RESERVED
			, REVERTED_DOCUMENT
			, ORIGINAL_DOCUMENT
			, ORIGINAL_LINE_ITEM
			, REFERENCE_DOCUMENT
			, MOVEMENT_IND
			, RECEIPT_IND
			, CONSUMPTION_POSTING
			, SPECIAL_IND

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
			, ASSET_WID
			, ORIGINAL_POSTING_DATE
			, PLANT_CODE
			, VALUATION_TYPE
			, MATERIAL_NUMBER
			, CLIENT_CODE
			, LEDGER_CODE
			, STORAGE_LOCATION
			, RECEIPT_STORAGE_LOCATION
			, LINE_ID
			, PARENT_ID
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
			, OPENING_VOLUMN
			, MATERIAL_DOCUMENT
			, MATERIAL_LINE
			, STOCK_VALUE
			, OPENING_VALUE
			, LOCAL_AMOUNT
			, DELIVERY_COST
			, MOVEMENT_TYPE
			, UPLOADING_POINT
			, PURCHASE_DOCUMENT
			, PURCHASE_LINE_ITEM
			, FLG_RESERVING
			, FLG_RESERVED
			, FISCAL_YEAR_RESERVED
			, REVERTED_DOCUMENT
			, ORIGINAL_DOCUMENT
			, ORIGINAL_LINE_ITEM
			, REFERENCE_DOCUMENT
			, MOVEMENT_IND
			, RECEIPT_IND
			, CONSUMPTION_POSTING
			, SPECIAL_IND

            , W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
        FROM #W_SAP_SPP_TRANSACTION_F_tmp
        where W_UPDATE_FLG = 'N'

--alter table W_SAP_SPP_TRANSACTION_F
--add DELIVERY_COST decimal(38,20)

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
            FROM W_SAP_SPP_TRANSACTION_F
        ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

		SET @src_rownum = ( SELECT COUNT(1) FROM #W_SAP_SPP_TRANSACTION_F_tmp );
		SET @tgt_rownum = ( 
            SELECT 
                COUNT(DISTINCT W_INTEGRATION_ID)
            FROM W_SAP_SPP_TRANSACTION_F
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
