SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [dbo].[SAP_proc_load_w_spp_trans_f] @p_batch_id [bigint] AS BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_SAP_SPP_TRANS_F',
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

        IF OBJECT_ID(N'tempdb..#W_SAP_SPP_TRANS_F_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_SAP_SPP_TRANS_F_tmp'
            DROP Table #W_SAP_SPP_TRANS_F_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table';

        -- DECLARE @p_batch_id INT = 20230413;
        WITH MDOC AS ( 
            SELECT 
                MBLNR
                , ZEILE
                , MATNR
            from [FND].[W_SAP_MATDOC_F_TEMP] 
            WHERE 1=1
                AND BWART IN ('101', '102') 
                AND MATNR LIKE '00000000006%' 
                -- AND BUDAT <= @Lastdayofmonth 
            GROUP BY MBLNR, ZEILE, MATNR
        ), BDOC AS ( 
            SELECT 
                MATERIAL_DOCUMENT
                , MATERIAL_LINE
                , MATERIAL_NUMBER 
            FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
            WHERE 1=1
                -- AND DATE_WID <= convert(bigint, convert(varchar,@Lastdayofmonth,112)) --- @dateLastMonth 
                AND ACCOUNT_NUMBER = '0000120050'
            GROUP BY MATERIAL_DOCUMENT, MATERIAL_LINE, MATERIAL_NUMBER
        ), d as (
            SELECT
                D.MBLNR
                , D.ZEILE
                , D.MATNR
            FROM MDOC D 
                LEFT JOIN BDOC F ON 1=1
                    AND D.MBLNR = F.MATERIAL_DOCUMENT
                    AND CONCAT('00', D.ZEILE) = F.MATERIAL_LINE
                    AND F.MATERIAL_NUMBER = RIGHT(D.MATNR,8)
            WHERE 1=1
                AND F.MATERIAL_LINE IS NULL
            ), R AS (
                SELECT 
                    MBLNR
                    , ZEILE
                    , MATNR
                    , BUDAT--, COUNT(1) 
                FROM [FND].[W_SAP_MATDOC_F_TEMP] 
                where 1=1
                    AND CANCELLED = 'X' 
                    AND MATNR LIKE '00000000006%'
                    -- AND BUDAT <= @Lastdayofmonth
                GROUP BY MBLNR, ZEILE, MATNR, BUDAT
            )
                SELECT
                    CONVERT(INT, ISNULL(F.BUDAT, 0))                AS DATE_WID
                    , ISNULL(PL_X.PLANT_WID, 0)                     AS PLANT_WID
                    , ISNULL(P.PRODUCT_WID, 0)                      AS MATERIAL_WID

                    , F.BUDAT                                       AS POSTING_DATE
                    , F.WERKS                                       AS PLANT_CODE
                    , F.LGORT                                       AS STORAGE_LOCATION
                    , F.BWTAR                                       AS VALUATION_TYPE
                    , F.VPRSV                                       AS PRICE_CONTROL
                    , REPLACE(
                        LTRIM(REPLACE(F.MATNR, '0', ' ')),
                        ' ',
                        '0'
                    )                                               AS MATERIAL_NUMBER
                    , F.STOCK_QTY                                   AS QUANTITY
                    , F.MBLNR                                       AS MATERIAL_DOCUMENT
                    , CONCAT('00', F.ZEILE)                         AS MATERIAL_LINE
                    , F.BWART                                       AS MOVEMENT_TYPE
                    , CASE WHEN F.SMBLN = '' THEN NULL 
                        ELSE F.SMBLN END                            AS [ORGINAL_DOCUMENT]
                    , CASE WHEN F.SMBLP = '0000' THEN NULL
                        ELSE F.SMBLP END                            AS ORIGINAL_LINE_ITEM
                    , R.BUDAT                                       AS ORG_POSTING_DATE
                    , F.EBELN                                       AS PURCHASE_DOCUMENT
                    , F.EBELP                                       AS PURCHASE_LINE_ITEM
                    , F.LBKUM                                       AS OB_QUANTITY
                    , CASE WHEN F.WAERS IN ('VND', 'JPY') THEN F.SALK3 * 100
                        ELSE F.SALK3 END                            AS OB_VALUE
                    , CASE WHEN F.WAERS IN ('VND', 'JPY') THEN F.DMBTR_STOCK * 100
                        ELSE F.DMBTR_STOCK END                      AS STOCK_VALUE
                    , F.SHKZG                                       AS DEBIT_IND
                    , 0                                             AS LOCAL_AMT
                    , CASE WHEN F.WAERS IN ('VND', 'JPY') THEN F.BNBTR * 100
                        ELSE F.DMBTR_STOCK END                      AS DELIVERY_COST
                    , XBLNR                                         AS REFERENCE_DOCUMENT

                    , CONVERT(VARCHAR(20), F.KZBEW)                 AS MOVEMENT_IND
                    , CONVERT(VARCHAR(20), F.KZZUG)                 AS RECEIPT_IND
                    , CONVERT(VARCHAR(20), F.KZVBR)                 AS CONSUMPTION_POSTING
                    , CONVERT(VARCHAR(20), F.SOBKZ)                 AS SPECIAL_IND

                    , CONVERT(VARCHAR(50), KOSTL)                   AS COST_CENTER
                    , CONVERT(VARCHAR(50), ABLAD)                   AS WORK_ORDER
                    , CONVERT(
                        VARCHAR,
                        REPLACE(LTRIM(REPLACE(F.LIFNR, '0', ' ')), 
                                ' ', '0')
                    )                                               AS VENDOR_CODE

                    , CONCAT(F.MANDT, '~', F.BUKRS, '~', F.MBLNR, 
                            '~', F.MJAHR, '~', F.ZEILE, '~', ISNULL(F.MATNR,''),
                            '~', F.WERKS, '~', ISNULL(F.CHARG,''),
                            '~', ISNULL(F.LGORT,''), '~', F.RECORD_TYPE,
                            '~', F.HEADER_COUNTER
                    )                                               AS W_INTEGRATION_ID
                    , 'N'                                           AS W_DELETE_FLG
                    , 1                                             AS W_DATASOURCE_NUM_ID
                    , DATEADD(HH, 7, GETDATE())                     AS W_INSERT_DT
                    , DATEADD(HH, 7, GETDATE())                     AS W_UPDATE_DT
                    , @p_batch_id                                   AS W_BATCH_ID
                    , 'N'                                           AS W_UPDATE_FLG
                INTO #W_SAP_SPP_TRANS_F_tmp
                FROM [FND].[W_SAP_MATDOC_F_TEMP] F
                    LEFT JOIN R ON 1=1
                        AND R.MBLNR = F.SMBLN
                        AND R.ZEILE = F.SMBLP
                        AND F.BWART = '312'
                    LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL_X ON 1=1
                        AND PL_X.STO_LOC = CASE WHEN F.LGORT IS NULL THEN '' ELSE F.LGORT END
                        AND F.WERKS = PL_X.PLANT
                    LEFT JOIN [dbo].[W_PRODUCT_D] P ON 1=1
                        AND P.PRODUCT_CODE = REPLACE(LTRIM(REPLACE(F.MATNR, '0', ' ')), ' ', '0')
                        AND P.W_DATASOURCE_NUM_ID = 1
                WHERE 1=1
                    AND F.BWART IN ('311', '312')
                    AND F.MATNR LIKE '00000000006%'
                    --AND ACCOUNT_NUMBER = '0000120050'
                    -- AND F.BUDAT <= @Lastdayofmonth--@dateLastMonth

                UNION ALL
                
                SELECT
                    F.DATE_WID
                    , F.PLANT_WID
                    , F.PRODUCT_WID                                 AS MATERIAL_WID

                    , CONVERT(DATE, CONVERT(VARCHAR, F.DATE_WID))   AS POSTING_DATE
                    , F.PLANT_CODE
                    , F.STORAGE_LOCATION
                    , F.VALUATION_TYPE
                    , F.PRICE_CONTROL
                    , F.MATERIAL_NUMBER
                    , F.QUANTITY
                    , F.MATERIAL_DOCUMENT
                    , F.MATERIAL_LINE
                    , F.MOVEMENT_TYPE
                    , F.ORIGINAL_DOCUMENT
                    , F.ORIGINAL_LINE_ITEM
                    , R.BUDAT
                    , PURCHASE_DOCUMENT
                    , PURCHASE_LINE_ITEM
                    , OPENING_VOLUMN
                    , OPENING_VALUE
                    , STOCK_VALUE
                    , F.DEBIT_INDICATOR
                    , F.LOCAL_AMOUNT                                AS LOCAL_AMT
                    , DELIVERY_COST
                    , F.REFERENCE_DOCUMENT
                    , MOVEMENT_IND
                    , RECEIPT_IND
                    , CONSUMPTION_POSTING
                    , SPECIAL_IND

                    , F.COST_CENTER
                    , F.UPLOADING_POINT                             AS WORK_ORDER
                    , F.VENDOR_CODE

                    , F.W_INTEGRATION_ID
                    , 'N'                                           AS W_DELETE_FLG
                    , 1                                             AS W_DATASOURCE_NUM_ID
                    , DATEADD(HH, 7, GETDATE())                     AS W_INSERT_DT
                    , DATEADD(HH, 7, GETDATE())                     AS W_UPDATE_DT
                    , @p_batch_id                                   AS W_BATCH_ID
                    , 'N'                                           AS W_UPDATE_FLG
                FROM [dbo].[W_SAP_SPP_TRANSACTION_F] F 
                    left join R ON 1=1
                        AND R.MBLNR = F.ORIGINAL_DOCUMENT
                        AND R.ZEILE = F.ORIGINAL_LINE_ITEM
                WHERE 1=1
                    AND F.QUANTITY <> 0
                    AND ACCOUNT_NUMBER = '0000120050'
                    -- AND F.date_wid <= convert(bigint, convert(varchar, @Lastdayofmonth,112))
                    AND F.MOVEMENT_TYPE NOT IN ('311', '312')
                    
                UNION ALL
                
                SELECT
                    CONVERT(INT, ISNULL(F.BUDAT, 0))                AS DATE_WID
                    , ISNULL(PL_X.PLANT_WID, 0)                     AS PLANT_WID
                    , ISNULL(P.PRODUCT_WID, 0)                      AS MATERIAL_WID

                    , F.BUDAT                                       AS POSTING_DATE
                    , F.WERKS                                       AS PLANT_CODE
                    , F.LGORT                                       AS STORAGE_LOCATION
                    , F.BWTAR                                       AS VALUATION_TYPE
                    , F.VPRSV                                       AS PRICE_CONTROL
                    , REPLACE(
                        LTRIM(REPLACE(F.MATNR, '0', ' ')),
                        ' ',
                        '0'
                    )                                               AS MATERIAL_NUMBER
                    , F.STOCK_QTY                                   AS QUANTITY
                    , F.MBLNR                                       AS MATERIAL_DOCUMENT
                    , CONCAT('00', F.ZEILE)                         AS MATERIAL_LINE
                    , F.BWART                                       AS MOVEMENT_TYPE
                    , CASE WHEN F.SMBLN = '' THEN NULL
                        ELSE F.SMBLN END                            AS ORGINAL_DOCUMENT
                    , CASE WHEN F.SMBLP = '0000' THEN NULL
                        ELSE F.SMBLP END                            AS ORIGINAL_LINE_ITEM
                    , R.BUDAT                                       AS ORG_POSTING_DATE
                    , F.EBELN                                       AS PURCHASE_DOCUMENT
                    , F.EBELP                                       AS PURCHASE_LINE_ITEM
                    , F.LBKUM                                       AS OB_QUANTITY
                    , CASE WHEN F.WAERS IN ('VND', 'JPY') THEN F.SALK3 * 100
                        ELSE F.SALK3 END                            AS OB_VALUE
                    , CASE WHEN F.WAERS IN ('VND', 'JPY') 
                            THEN (F.DMBTR_STOCK + F.BNBTR) * 100
                        ELSE F.DMBTR_STOCK END                      AS STOCK_VALUE
                    , F.SHKZG                                       AS DEBIT_IND
                    , 0                                             AS LOCAL_AMOUNT
                    , CASE WHEN F.WAERS IN ('VND', 'JPY') THEN F.BNBTR * 100
                        ELSE F.DMBTR_STOCK END                      AS DELIVERY_COST
                    , XBLNR                                         AS REFERENCE_DOCUMENT
                    , CONVERT(VARCHAR(20), F.KZBEW)                 AS MOVEMENT_IND
                    , CONVERT(VARCHAR(20), F.KZZUG)                 AS RECEIPT_IND
                    , CONVERT(VARCHAR(20), F.KZVBR)                 AS CONSUMPTION_POSTING
                    , CONVERT(VARCHAR(20), F.SOBKZ)                 AS SPECIAL_IND

                    , CONVERT(VARCHAR(50), KOSTL)                   AS COST_CENTER
                    , CONVERT(VARCHAR(50), ABLAD)                   AS WORK_ORDER
                    , CONVERT(
                        VARCHAR,
                        REPLACE(LTRIM(REPLACE(F.LIFNR, '0', ' ')), 
                                ' ', '0')
                    )                                               AS VENDOR_CODE

                    , CONCAT(F.MANDT, '~', F.BUKRS, '~', F.MBLNR, 
                            '~', F.MJAHR, '~', F.ZEILE, '~', ISNULL(F.MATNR,''),
                            '~', F.WERKS, '~', ISNULL(F.CHARG,''),
                            '~', ISNULL(F.LGORT,''), '~', F.RECORD_TYPE,
                            '~', F.HEADER_COUNTER
                    )                                               AS W_INTEGRATION_ID
                    , 'N'                                           AS W_DELETE_FLG
                    , 1                                             AS W_DATASOURCE_NUM_ID
                    , DATEADD(HH, 7, GETDATE())                     AS W_INSERT_DT
                    , DATEADD(HH, 7, GETDATE())                     AS W_UPDATE_DT
                    , @p_batch_id                                   AS W_BATCH_ID
                    , 'N'                                           AS W_UPDATE_FLG
                FROM [FND].[W_SAP_MATDOC_F_TEMP] F
                    INNER JOIN D ON 1=1
                        AND F.MBLNR = D.MBLNR
                        AND F.ZEILE = D.ZEILE
                        AND F.MATNR = D.MATNR
                    LEFT JOIN R ON 1=1
                        AND R.MBLNR = F.SMBLN
                        AND R.ZEILE = F.SMBLP
                        AND F.BWART = '102'
                    LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL_X ON 1=1
                        AND PL_X.STO_LOC = CASE WHEN F.LGORT IS NULL THEN '' ELSE F.LGORT END
                        AND F.WERKS = PL_X.PLANT
                    LEFT JOIN [dbo].[W_PRODUCT_D] P ON 1=1
                        AND P.PRODUCT_CODE = REPLACE(LTRIM(REPLACE(F.MATNR, '0', ' ')), ' ', '0')
                        AND P.W_DATASOURCE_NUM_ID = 1
                -- WHERE F.BUDAT <= @Lastdayofmonth
        ;


        -- 3. Update main table using W_INTEGRATION_ID
        PRINT '3. Update main table using W_INTEGRATION_ID';

        -- 3.1. Mark existing records by flag 'Y'
        PRINT '3.1. Mark existing records by flag ''Y''';

        UPDATE #W_SAP_SPP_TRANS_F_tmp
		    SET W_UPDATE_FLG = 'Y'
		FROM #W_SAP_SPP_TRANS_F_tmp tg
            INNER JOIN [dbo].[W_SAP_SPP_TRANS_F] sc ON 1=1
                AND sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID
        ;

        -- 3.2. Start updating
        PRINT '3.2. Start updating';

		UPDATE [dbo].[W_SAP_SPP_TRANS_F] SET 
              DATE_WID = src.DATE_WID
            , PLANT_WID = src.PLANT_WID
            , MATERIAL_WID = src.MATERIAL_WID

			, POSTING_DATE = src.POSTING_DATE
            , PLANT_CODE = src.PLANT_CODE
            , STORAGE_LOCATION = src.STORAGE_LOCATION
            , VALUATION_TYPE = src.VALUATION_TYPE
            , PRICE_CONTROL = src.PRICE_CONTROL
            , MATERIAL_NUMBER = src.MATERIAL_NUMBER
            , QUANTITY = src.QUANTITY
            , MATERIAL_DOCUMENT = src.MATERIAL_DOCUMENT
            , MATERIAL_LINE = src.MATERIAL_LINE
            , MOVEMENT_TYPE = src.MOVEMENT_TYPE
            , ORGINAL_DOCUMENT = src.ORGINAL_DOCUMENT
            , ORIGINAL_LINE_ITEM = src.ORIGINAL_LINE_ITEM
            , ORG_POSTING_DATE = src.ORG_POSTING_DATE
            , PURCHASE_DOCUMENT = src.PURCHASE_DOCUMENT
            , PURCHASE_LINE_ITEM = src.PURCHASE_LINE_ITEM
            , OB_QUANTITY = src.OB_QUANTITY
            , OB_VALUE = src.OB_VALUE
            , STOCK_VALUE = src.STOCK_VALUE
            , DEBIT_IND = src.DEBIT_IND
            , LOCAL_AMT = src.LOCAL_AMT
            , DELIVERY_COST = src.DELIVERY_COST
            , REFERENCE_DOCUMENT = src.REFERENCE_DOCUMENT
            , MOVEMENT_IND = src.MOVEMENT_IND
            , RECEIPT_IND = src.RECEIPT_IND
            , CONSUMPTION_POSTING = src.CONSUMPTION_POSTING
            , SPECIAL_IND = src.SPECIAL_IND
            , VENDOR_CODE = src.VENDOR_CODE
            , WORK_ORDER = src.WORK_ORDER
            , COST_CENTER = src.COST_CENTER

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = DATEADD(HH, 7, getdate())
        FROM [dbo].[W_SAP_SPP_TRANS_F] tgt
            INNER JOIN #W_SAP_SPP_TRANS_F_tmp src ON 1=1
                AND src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID
        ;


	    -- 4. Insert non-existed records to main table from temp table
        PRINT '4. Insert non-existed records to main table from temp table';

        INSERT INTO [dbo].[W_SAP_SPP_TRANS_F] (
              DATE_WID
            , PLANT_WID
            , MATERIAL_WID

            , POSTING_DATE
            , PLANT_CODE
            , STORAGE_LOCATION
            , VALUATION_TYPE
            , PRICE_CONTROL
            , MATERIAL_NUMBER
            , QUANTITY
            , MATERIAL_DOCUMENT
            , MATERIAL_LINE
            , MOVEMENT_TYPE
            , ORGINAL_DOCUMENT
            , ORIGINAL_LINE_ITEM
            , ORG_POSTING_DATE
            , PURCHASE_DOCUMENT
            , PURCHASE_LINE_ITEM
            , OB_QUANTITY
            , OB_VALUE
            , STOCK_VALUE
            , DEBIT_IND
            , LOCAL_AMT
            , DELIVERY_COST
            , REFERENCE_DOCUMENT
            , MOVEMENT_IND
            , RECEIPT_IND
            , CONSUMPTION_POSTING
            , SPECIAL_IND
            , VENDOR_CODE
            , WORK_ORDER
            , COST_CENTER

            , W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
        ) SELECT
              DATE_WID
            , PLANT_WID
            , MATERIAL_WID

            , POSTING_DATE
            , PLANT_CODE
            , STORAGE_LOCATION
            , VALUATION_TYPE
            , PRICE_CONTROL
            , MATERIAL_NUMBER
            , QUANTITY
            , MATERIAL_DOCUMENT
            , MATERIAL_LINE
            , MOVEMENT_TYPE
            , ORGINAL_DOCUMENT
            , ORIGINAL_LINE_ITEM
            , ORG_POSTING_DATE
            , PURCHASE_DOCUMENT
            , PURCHASE_LINE_ITEM
            , OB_QUANTITY
            , OB_VALUE
            , STOCK_VALUE
            , DEBIT_IND
            , LOCAL_AMT
            , DELIVERY_COST
            , REFERENCE_DOCUMENT
            , MOVEMENT_IND
            , RECEIPT_IND
            , CONSUMPTION_POSTING
            , SPECIAL_IND
            , VENDOR_CODE
            , WORK_ORDER
            , COST_CENTER

            , W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
        FROM #W_SAP_SPP_TRANS_F_tmp
        WHERE W_UPDATE_FLG = 'N'
        ;


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
            FROM W_SAP_SPP_TRANS_F
        ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

		SET @src_rownum = ( SELECT COUNT(1) FROM #W_SAP_SPP_TRANS_F_tmp );
		SET @tgt_rownum = ( 
            SELECT 
                COUNT(DISTINCT W_INTEGRATION_ID)
            FROM W_SAP_SPP_TRANS_F
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
