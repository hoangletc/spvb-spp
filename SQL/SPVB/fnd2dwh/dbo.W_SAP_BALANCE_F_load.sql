SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (OBJECT_ID('[dbo].[SAP_proc_load_w_spp_balance_f]') is not null)
BEGIN
    DROP PROCEDURE [dbo].[SAP_proc_load_w_spp_balance_f]
END;
GO

CREATE PROC [dbo].[SAP_proc_load_w_spp_balance_f]
    @p_batch_id [bigint],
	@month      SMALLINT,
	@year       INT
AS 
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_SAP_SPP_BALANCE_F',
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

        IF OBJECT_ID(N'tempdb..#W_SAP_SPP_BALANCE_F_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_SAP_SPP_BALANCE_F_tmp'
            DROP Table #W_SAP_SPP_BALANCE_F_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

	
			
		PRINT CONCAT_WS('-', CONVERT(varchar, @year), CONVERT(varchar, @month), '01');
				
		DECLARE @cur_date 	DATE  		= GETDATE();
		DECLARE @eom        DATE        = FORMAT(
			EOMONTH(CONCAT_WS('-', CONVERT(varchar, @year), CONVERT(varchar, @month), '01')),
			'yyyyMMdd'
		);
		DECLARE @fom        DATE        = CONVERT(
			DATE,
			CONCAT_WS('-', CONVERT(varchar, @year), CONVERT(varchar, @month + 1), '01')
		);


		PRINT '--> 0. Remove tmp tables'

		IF OBJECT_ID(N'tempdb..#TMP_TRANS_AGING') IS NOT NULL 
		BEGIN
			PRINT N'DELETE temporary table #TMP_TRANS_AGING'
			DROP Table #TMP_TRANS_AGING
		END;

		IF OBJECT_ID(N'tempdb..#TMP_PIVOT') IS NOT NULL 
		BEGIN
			PRINT N'DELETE temporary table #TMP_PIVOT'
			DROP Table #TMP_PIVOT
		END;

		IF OBJECT_ID(N'tempdb..#TMP_1_ISS') IS NOT NULL 
		BEGIN
			PRINT N'DELETE temporary table #TMP_1_ISS'
			DROP Table #TMP_1_ISS
		END;

		IF OBJECT_ID(N'tempdb..#TMP_1_REC') IS NOT NULL 
		BEGIN
			PRINT N'DELETE temporary table #TMP_1_REC'
			DROP Table #TMP_1_REC
		END;

		IF OBJECT_ID(N'tempdb..#TMP_ACCUM_CUR_MONTH') IS NOT NULL 
		BEGIN
			PRINT N'DELETE temporary table #TMP_ACCUM_CUR_MONTH'
			DROP Table #TMP_ACCUM_CUR_MONTH
		END;

		IF OBJECT_ID(N'tempdb..#TMP_1_AGING') IS NOT NULL 
		BEGIN
			PRINT N'DELETE temporary table #TMP_1_AGING'
			DROP Table #TMP_1_AGING
		END;


		IF OBJECT_ID(N'tempdb..#TMP_REMAINING') IS NOT NULL 
		BEGIN
			PRINT N'DELETE temporary table #TMP_REMAINING'
			DROP Table #TMP_REMAINING
		END;

		IF OBJECT_ID(N'tempdb..#W_SAP_SPP_BALANCE_F_tmp') IS NOT NULL 
		BEGIN
			PRINT N'DELETE temporary table #W_SAP_SPP_BALANCE_F_tmp'
			DROP Table #W_SAP_SPP_BALANCE_F_tmp
		END;


		PRINT '--> 1. Select all receipt transactions grouped by plant, mat_num up to current month';

		SELECT 
			PLANT_CODE 
			, MATERIAL_NUMBER                                               AS MAT_NUM 
			, SUM(QUANTITY)                                                 AS ISS_QTY
			, SUM(LOCAL_AMOUNT)                                             AS ISS_AMOUNT
		INTO #TMP_1_ISS
		FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
		WHERE 1=1
			AND CONVERT(VARCHAR, DATE_WID) < @fom
			-- AND MOVEMENT_TYPE IN ('201', '202', '551', '311', '122')
			AND QUANTITY < 0
		GROUP BY PLANT_CODE, MATERIAL_NUMBER;


		SELECT 
			PLANT_CODE
			, MATERIAL_NUMBER                                               AS MAT_NUM
			, CONVERT(DATE, CONVERT(VARCHAR, DATE_WID))                     AS GR_DATE 
			, SUM(QUANTITY)                                                 AS REC_QTY
			, SUM(LOCAL_AMOUNT)                                             AS REC_AMNT
		INTO #TMP_1_REC
		FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
		WHERE 1=1
			AND CONVERT(DATE, CONVERT(VARCHAR, DATE_WID)) < @fom
			-- AND (
			--     MOVEMENT_TYPE NOT IN ('201', '202', '551', '311', '122')
			--     OR MOVEMENT_TYPE IS NULL
			-- )
			AND QUANTITY >=0

			-- AND MATERIAL_NUMBER = @mat_num
			-- AND PLANT_CODE = @plant_code
		GROUP BY DATE_WID, PLANT_CODE, MATERIAL_NUMBER;


		SELECT 
			R.*
			, I.ISS_QTY
			, I.ISS_AMOUNT
			, SUM(R.REC_QTY) OVER (
				PARTITION BY R.PLANT_CODE, R.MAT_NUM 
				ORDER BY GR_DATE
			)                                                               AS ACCUM_QTY_REC_ONLY
			, SUM(R.REC_AMNT) OVER (
				PARTITION BY R.PLANT_CODE, R.MAT_NUM 
				ORDER BY GR_DATE
			)                                                               AS ACCUM_AMNT_REC_ONLY

			, CASE WHEN SUM(R.REC_QTY) OVER 
				(PARTITION BY R.PLANT_CODE, R.MAT_NUM 
				ORDER BY GR_DATE) + I.ISS_QTY <= 0 THEN 0 
				ELSE 1 
			END                                                             AS FLG_LEFTOVER_QTY
			, CASE WHEN SUM(R.REC_AMNT) OVER 
				(PARTITION BY R.PLANT_CODE, R.MAT_NUM 
				ORDER BY GR_DATE) + I.ISS_AMOUNT <= 0 THEN 0 
				ELSE 1 
			END                                                             AS FLG_LEFTOVER_AMNT
		INTO #TMP_ACCUM_CUR_MONTH
		FROM #TMP_1_REC R
			LEFT JOIN #TMP_1_ISS I ON 1=1
				AND R.PLANT_CODE = I.PLANT_CODE 
				AND R.MAT_NUM = I.MAT_NUM;



		WITH TMP_BALANCE AS (
		SELECT
			PLANT_CODE
			, MAT_NUM
			, GR_DATE
			, CASE WHEN FLG_LEFTOVER_QTY = 0 THEN NULL 
				WHEN GR_DATE = MIN(GR_DATE) OVER (
					PARTITION BY PLANT_CODE, MAT_NUM, FLG_LEFTOVER_QTY
				) THEN ISNULL(ISS_QTY, 0) + ACCUM_QTY_REC_ONLY
				ELSE REC_QTY
			END                                                             AS REMAINING_QTY
			, CASE WHEN GR_DATE = MAX(GR_DATE) OVER (
					PARTITION BY PLANT_CODE, MAT_NUM
				) THEN ISNULL(ISS_AMOUNT, 0) + ACCUM_AMNT_REC_ONLY
				ELSE 0
			END                                                             AS REMAINING_AMNT
			FROM #TMP_ACCUM_CUR_MONTH
		)
			SELECT
				PLANT_CODE
				, MAT_NUM
				, GR_DATE
				, REMAINING_AMNT
				, REMAINING_QTY
				, CASE WHEN REMAINING_QTY > 0 
					THEN DATEDIFF(M, GR_DATE, CONVERT(DATE, @fom)) 
					ELSE NULL 
				END                                                           AS AGING_MONTH
			INTO #TMP_1_AGING
			FROM TMP_BALANCE;   


		SELECT
			PLANT_CODE
			, MAT_NUM

			, SUM(REMAINING_AMNT)   AS RMN_AMNT
			, SUM(REMAINING_QTY)    AS RMN_QTY
		INTO #TMP_REMAINING
		FROM #TMP_1_AGING
		GROUP BY PLANT_CODE, MAT_NUM;

		SELECT
			CASE WHEN ACC.AGING_MONTH < 4 THEN '< 4 MONTHS'
				WHEN ACC.AGING_MONTH BETWEEN 4 AND 12 THEN '4 - 12 MONTHS'
				WHEN ACC.AGING_MONTH BETWEEN 12 AND 24 THEN '1 - 2 YEARS'
				WHEN ACC.AGING_MONTH BETWEEN 24 AND 36 THEN '2 - 3 YEARS'
				WHEN ACC.AGING_MONTH BETWEEN 36 AND 48 THEN '3 - 4 YEARS'
				WHEN ACC.AGING_MONTH BETWEEN 48 AND 60 THEN '4 - 5 YEARS'
				ELSE '> 5 YEARS'
			END                                                             AS AGING_GROUP
			, ACC.PLANT_CODE
			, ACC.MAT_NUM
			, ACC.GR_DATE
			, ACC.REMAINING_QTY
			, ACC.REMAINING_AMNT
			, TRANS.PURCHASING_GROUP
			, TRANS.STORAGE_LOCATION
			, TRANS.COMPANY_CODE
			, TRANS.VALUATION_AREA
			, TRANS.VALUATION_CLASS
			, TRANS.MATERIAL_TYPE
			, TRANS.MATERIAL_GROUP
			, TRANS.BASE_UNIT_OF_MEASURE
			, TRANS.CURRENCY
			, TRANS.PRICE_CONTROL
		INTO #TMP_TRANS_AGING
		FROM #TMP_1_AGING ACC
			LEFT JOIN [dbo].[W_SAP_SPP_TRANSACTION_F] TRANS ON 1=1
				AND TRANS.DATE_WID = CONVERT(INT, FORMAT(ACC.GR_DATE, 'yyyyMMdd'))
				AND TRANS.PLANT_CODE = ACC.PLANT_CODE
				AND TRANS.MATERIAL_NUMBER = ACC.MAT_NUM
				AND (
					TRANS.MOVEMENT_TYPE NOT IN ('201', '202', '901', '902') 
					OR TRANS.MOVEMENT_TYPE IS NULL
				);


		PRINT '--> 2. Pivot';

		SELECT
			TMP.MAT_NUM
			, TMP.PLANT_CODE
			, [< 4 MONTHS]
			, [4 - 12 MONTHS]
			, [1 - 2 YEARS]
			, [2 - 3 YEARS]
			, [3 - 4 YEARS]
			, [4 - 5 YEARS]
		INTO #TMP_PIVOT
		FROM (
			SELECT MAT_NUM, PLANT_CODE, AGING_GROUP
			FROM #TMP_TRANS_AGING
		) AS SB
		PIVOT (
			COUNT(AGING_GROUP)
			FOR AGING_GROUP IN (
				[< 4 MONTHS]
				, [4 - 12 MONTHS]
				, [1 - 2 YEARS]
				, [2 - 3 YEARS]
				, [3 - 4 YEARS]
				, [4 - 5 YEARS]
			)
		) AS TMP;



		PRINT '--> 3. Create balance_month table';

		WITH TMP_B AS(
			SELECT
				PLANT_CODE
				, MAT_NUM
				, [< 4 MONTHS]
				, [4 - 12 MONTHS]
				, [1 - 2 YEARS]
				, [2 - 3 YEARS]
				, [3 - 4 YEARS]
				, [4 - 5 YEARS]
				, [4 - 12 MONTHS] + [1 - 2 YEARS]
				+ [2 - 3 YEARS] + [3 - 4 YEARS] + [4 - 5 YEARS]     AS TOTAL_LEFTOVER
				, [3 - 4 YEARS] + [4 - 5 YEARS]                     AS SLOW_MOVING
			FROM #TMP_PIVOT
		),
		TMP_AG AS (
			SELECT *
			, ROW_NUMBER() OVER (PARTITION BY T.PLANT_CODE, T.MAT_NUM ORDER BY GR_DATE DESC) AS RN
			FROM #TMP_TRANS_AGING T
		)
			SELECT
				CONVERT(VARCHAR, FORMAT(@eom, 'yyyyMMdd'))          AS DATE_WID
				, PL.PLANT_WID                                      AS PLANT_WID
				, @eom                                              AS [PERIOD]
				, TMP_B.PLANT_CODE
				, PL.PLANT_NAME_2									AS PLANT
				, TMP_B.MAT_NUM										AS MATERIAL_NUMBER
				, R.RMN_QTY                                         AS QUANTITY
				, R.RMN_AMNT                                        AS AMOUNT
				, NULL												AS [TYPE]
				, [< 4 MONTHS]
				, [4 - 12 MONTHS]
				, [1 - 2 YEARS]
				, [2 - 3 YEARS]
				, [3 - 4 YEARS]
				, [4 - 5 YEARS]
				, TMP_AG.PURCHASING_GROUP
				, TMP_AG.STORAGE_LOCATION
				, TMP_AG.COMPANY_CODE
				, TMP_AG.VALUATION_AREA
				, TMP_AG.VALUATION_CLASS
				, TMP_AG.MATERIAL_TYPE
				, TMP_AG.MATERIAL_GROUP
				, TMP_AG.BASE_UNIT_OF_MEASURE
				, TMP_AG.CURRENCY
				, TMP_AG.PRICE_CONTROL
				, MM.MAX                                            AS MAX
				, MM.MIN                                            AS MIN
				, CASE WHEN TOTAL_LEFTOVER > MM.MAX
					THEN TOTAL_LEFTOVER - MM.MAX
					ELSE 0
				END                                                 AS OVER_MAX
				, CASE WHEN TOTAL_LEFTOVER < MM.MIN
					THEN MM.MIN - TOTAL_LEFTOVER
					ELSE 0
				END                                                 AS UNDER_MIN
				, SLOW_MOVING
				, CONCAT_WS(
					'~' 
					, CONVERT(VARCHAR, FORMAT(@eom, 'yyyyMMdd'))
					, TMP_B.MAT_NUM
					, TMP_B.PLANT_CODE
					, TMP_AG.STORAGE_LOCATION
				)                                                   AS W_INTEGRATION_ID
				, 'N'                                               AS W_DELETE_FLG
				, 1                                                 AS W_DATASOURCE_NUM_ID
				, GETDATE()                                         AS W_INSERT_DT
				, GETDATE()                                         AS W_UPDATE_DT
				, NULL                                              AS W_BATCH_ID
				, 'N'                                               AS W_UPDATE_FLG
				INTO #W_SAP_SPP_BALANCE_F_tmp
				FROM TMP_B
					LEFT JOIN TMP_AG ON 1=1
						AND TMP_AG.PLANT_CODE = TMP_B.PLANT_CODE
						AND TMP_AG.MAT_NUM = TMP_B.MAT_NUM     
						AND TMP_AG.RN = 1           
					LEFT JOIN #TMP_REMAINING R ON 1=1
						AND TMP_B.PLANT_CODE = R.PLANT_CODE
						AND TMP_B.MAT_NUM = R.MAT_NUM
					LEFT JOIN [FND].[W_CMMS_MINMAX_D] MM ON 1=1
						AND MM.ITEM_NUM = TMP_B.MAT_NUM
						AND MM.PLANT = TMP_B.PLANT_CODE
					LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL ON 1=1
						AND PL.PLANT = TMP_B.PLANT_CODE
						AND PL.STO_LOC = TMP_AG.STORAGE_LOCATION;

------------------------------------------------------------

        -- 3. Update main table using W_INTEGRATION_ID
        PRINT '3. Update main table using W_INTEGRATION_ID'

        -- 3.1. Mark existing records by flag 'Y'
        PRINT '3.1. Mark existing records by flag ''Y'''

        UPDATE #W_SAP_SPP_BALANCE_F_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_SAP_SPP_BALANCE_F_tmp tg
        INNER JOIN [dbo].[W_SAP_SPP_BALANCE_F] sc 
        ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

        -- 3.2. Start updating
        PRINT '3.2. Start updating'

		UPDATE  [dbo].[W_SAP_SPP_BALANCE_F]
		SET 
			PLANT_WID = src.PLANT_WID
			, DATE_WID = src.DATE_WID

			, [PERIOD] = src.PERIOD
			, QUANTITY = src.QUANTITY
			, AMOUNT = src.AMOUNT
			, PLANT = src.PLANT
			, MATERIAL_NUMBER = src.MATERIAL_NUMBER
			, [TYPE] = src.TYPE
			, [< 4 MONTHS] = src.[< 4 MONTHS]
			, [4 - 12 MONTHS] = src.[4 - 12 MONTHS]
			, [1 - 2 YEARS] = src.[1 - 2 YEARS]
			, [2 - 3 YEARS] = src.[2 - 3 YEARS]
			, [3 - 4 YEARS] = src.[3 - 4 YEARS]
			, [4 - 5 YEARS] = src.[4 - 5 YEARS]
			, PLANT_CODE = src.PLANT_CODE
			, STORAGE_LOCATION = src.STORAGE_LOCATION
			, COMPANY_CODE = src.COMPANY_CODE
			, VALUATION_AREA = src.VALUATION_AREA
			, VALUATION_CLASS = src.VALUATION_CLASS
			, MATERIAL_TYPE = src.MATERIAL_TYPE
			, MATERIAL_GROUP = src.MATERIAL_GROUP
			, PURCHASING_GROUP = src.PURCHASING_GROUP
			, BASE_UNIT_OF_MEASURE = src.BASE_UNIT_OF_MEASURE
			, CURRENCY = src.CURRENCY
			, PRICE_CONTROL = src.PRICE_CONTROL
			, MAX = src.MAX
			, MIN = src.MIN
			, OVER_MAX = src.OVER_MAX
			, UNDER_MIN = src.UNDER_MIN
			, SLOW_MOVING = src.SLOW_MOVING

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = @cur_date
        FROM [dbo].[W_SAP_SPP_BALANCE_F] tgt
        INNER JOIN #W_SAP_SPP_BALANCE_F_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


	    -- 4. Insert non-existed records to main table from temp table
        PRINT '4. Insert non-existed records to main table from temp table'

        INSERT INTO [dbo].[W_SAP_SPP_BALANCE_F](
            PLANT_WID
			, DATE_WID

			, [PERIOD]
			, QUANTITY
			, AMOUNT
			, PLANT
			, MATERIAL_NUMBER
			, [TYPE]
			, [< 4 MONTHS]
			, [4 - 12 MONTHS]
			, [1 - 2 YEARS]
			, [2 - 3 YEARS]
			, [3 - 4 YEARS]
			, [4 - 5 YEARS]
			, PLANT_CODE
			, STORAGE_LOCATION
			, COMPANY_CODE
			, VALUATION_AREA
			, VALUATION_CLASS
			, MATERIAL_TYPE
			, MATERIAL_GROUP
			, PURCHASING_GROUP
			, BASE_UNIT_OF_MEASURE
			, CURRENCY
			, PRICE_CONTROL
			, MAX
			, MIN
			, OVER_MAX
			, UNDER_MIN
			, SLOW_MOVING

            , W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
        )
        SELECT
			PLANT_WID
			, DATE_WID

			, [PERIOD]
			, QUANTITY
			, AMOUNT
			, PLANT
			, MATERIAL_NUMBER
			, [TYPE]
			, [< 4 MONTHS]
			, [4 - 12 MONTHS]
			, [1 - 2 YEARS]
			, [2 - 3 YEARS]
			, [3 - 4 YEARS]
			, [4 - 5 YEARS]
			, PLANT_CODE
			, STORAGE_LOCATION
			, COMPANY_CODE
			, VALUATION_AREA
			, VALUATION_CLASS
			, MATERIAL_TYPE
			, MATERIAL_GROUP
			, PURCHASING_GROUP
			, BASE_UNIT_OF_MEASURE
			, CURRENCY
			, PRICE_CONTROL
			, MAX
			, MIN
			, OVER_MAX
			, UNDER_MIN
			, SLOW_MOVING

            , W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
        FROM #W_SAP_SPP_BALANCE_F_tmp
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
            FROM W_SAP_SPP_BALANCE_F
        ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

		SET @src_rownum = ( SELECT COUNT(1) FROM #W_SAP_SPP_BALANCE_F_tmp );
		SET @tgt_rownum = ( 
            SELECT 
                COUNT(DISTINCT W_INTEGRATION_ID)
            FROM W_SAP_SPP_BALANCE_F
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