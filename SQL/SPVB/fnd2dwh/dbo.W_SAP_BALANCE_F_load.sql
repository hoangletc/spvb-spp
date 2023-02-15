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
	@dateFrom   DATE,
	@dateTo		DATE
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
			@p_job_status varchar(100) = 'SUCCESS',
			@today DATE = GETDATE(),
			@dateLastMonth DATE;

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

	-- Modify @dateFrom, @dateTo
	IF MONTH(@dateFrom) <> MONTH(@today)
    SET @dateFrom = DATEADD(d, 1, EOMONTH(DATEADD(m, -1, @dateFrom)));
	IF (@dateTo >= @today)
		SET @dateTo = DATEADD(D, -1, @today);
	IF MONTH(@dateTo) <> MONTH(@today)
		SET @dateTo = EOMONTH(@dateTo);

	BEGIN TRY
		WHILE (@dateFrom <= @dateTo)
		BEGIN
			SET @dateLastMonth = CASE WHEN EOMONTH(@dateFrom) > @dateTo 
				THEN @dateTo ELSE EOMONTH(@dateFrom) END;


			-- 1. Check existence and remove of temp table
			PRINT '1. Check existence and remove of temp table'

			IF OBJECT_ID(N'tempdb..#W_SAP_SPP_BALANCE_F_tmp') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #W_SAP_SPP_BALANCE_F_tmp'
				DROP Table #W_SAP_SPP_BALANCE_F_tmp
			END;


			-- 2. Select everything into temp table
			PRINT '2. Select everything into temp table'
	
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

			IF OBJECT_ID(N'tempdb..#TMP_BALANCE_FINAL') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #TMP_BALANCE_FINAL'
				DROP Table #TMP_BALANCE_FINAL
			END;


			IF OBJECT_ID(N'tempdb..#TMP_MVM_TYPE') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #TMP_MVM_TYPE'
				DROP Table #TMP_MVM_TYPE
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
				, STORAGE_LOCATION                                              AS STO_LOC
				, SUM(QUANTITY)                                                 AS ISS_QTY
				, SUM(LOCAL_AMOUNT)                                             AS ISS_AMOUNT
			INTO #TMP_1_ISS
			FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
			WHERE 1=1
				-- AND MOVEMENT_TYPE IN ('201', '202', '551', '311', '122')
				AND QUANTITY < 0

				-- AND MATERIAL_NUMBER = @mat_num
				-- AND PLANT_CODE = @plant_code
				-- AND STORAGE_LOCATION = @sloc
				AND CONVERT(VARCHAR, DATE_WID) <= @dateLastMonth
			GROUP BY PLANT_CODE, MATERIAL_NUMBER, STORAGE_LOCATION;


			SELECT 
				PLANT_CODE
				, MATERIAL_NUMBER                                               AS MAT_NUM
				, STORAGE_LOCATION                                              AS STO_LOC
				, CONVERT(DATE, CONVERT(VARCHAR, DATE_WID))                     AS GR_DATE 
				, SUM(QUANTITY)                                                 AS REC_QTY
				, SUM(LOCAL_AMOUNT)                                             AS REC_AMNT
			INTO #TMP_1_REC
			FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
			WHERE 1=1
				-- AND (
				--     MOVEMENT_TYPE NOT IN ('201', '202', '551', '311', '122')
				--     OR MOVEMENT_TYPE IS NULL
				-- )
				AND QUANTITY >=0

				-- AND MATERIAL_NUMBER = @mat_num
				-- AND PLANT_CODE = @plant_code
				-- AND STORAGE_LOCATION = @sloc
				AND CONVERT(DATE, CONVERT(VARCHAR, DATE_WID)) <= @dateLastMonth
			GROUP BY DATE_WID, PLANT_CODE, MATERIAL_NUMBER, STORAGE_LOCATION;

			SELECT DISTINCT F.MOVEMENT_TYPE
				INTO #TMP_MVM_TYPE
				FROM [dbo].[W_SAP_SPP_TRANSACTION_F] F, #TMP_1_REC R
				WHERE 1=1
					AND F.PLANT_CODE = R.PLANT_CODE
					AND F.MATERIAL_NUMBER = R.MAT_NUM
					AND F.STORAGE_LOCATION = R.STO_LOC
			;


			WITH TMP_ACCUM AS (
				SELECT
					R.PLANT_CODE
					, R.MAT_NUM
					, R.STO_LOC
					, R.GR_DATE
					, SUM(R.REC_QTY) OVER (
						PARTITION BY R.PLANT_CODE, R.MAT_NUM, R.STO_LOC
					)     AS TOT_QTY 
					, SUM(R.REC_AMNT) OVER (
						PARTITION BY R.PLANT_CODE, R.MAT_NUM, R.STO_LOC
					)    AS TOT_AMNT
					, SUM(R.REC_QTY) OVER (
						PARTITION BY R.PLANT_CODE, R.MAT_NUM, R.STO_LOC
						ORDER BY GR_DATE
					)     AS ACCUM_QTY_REC_ONLY
					, SUM(R.REC_AMNT) OVER (
						PARTITION BY R.PLANT_CODE, R.MAT_NUM, R.STO_LOC
						ORDER BY GR_DATE
					)     AS ACCUM_AMNT_REC_ONLY
				FROM #TMP_1_REC R    
			)
				SELECT 
					R.*
					, I.ISS_QTY
					, I.ISS_AMOUNT
					, A.ACCUM_QTY_REC_ONLY
					, A.ACCUM_AMNT_REC_ONLY
					, ISNULL(ISS_QTY, 0) + A.TOT_QTY                                AS RMN_QTY
					, ISNULL(ISS_AMOUNT, 0) + A.TOT_AMNT                            AS RMN_AMNT

					, CASE WHEN ACCUM_QTY_REC_ONLY + I.ISS_QTY <= 0 THEN 0 ELSE 1 
					END                                                             AS FLG_LEFTOVER_QTY
					, CASE WHEN ACCUM_AMNT_REC_ONLY + I.ISS_AMOUNT <= 0 THEN 0 ELSE 1 
					END                                                             AS FLG_LEFTOVER_AMNT
				INTO #TMP_ACCUM_CUR_MONTH
				FROM #TMP_1_REC R
					LEFT JOIN #TMP_1_ISS I ON 1=1
						AND R.PLANT_CODE = I.PLANT_CODE 
						AND R.MAT_NUM = I.MAT_NUM
						AND R.STO_LOC = I.STO_LOC
					LEFT JOIN TMP_ACCUM A ON 1=1
						AND A.[PLANT_CODE] = R.PLANT_CODE
						AND A.MAT_NUM = R.MAT_NUM
						AND A.STO_LOC = R.STO_LOC
						AND A.GR_DATE = R.GR_DATE
			;



			WITH TMP_BALANCE AS (
				SELECT
					PLANT_CODE
					, MAT_NUM
					, STO_LOC
					, GR_DATE
					, CASE WHEN FLG_LEFTOVER_QTY = 0 THEN 
							( CASE WHEN RMN_QTY < 0 AND GR_DATE = MAX(GR_DATE) OVER (
									PARTITION BY PLANT_CODE, MAT_NUM, STO_LOC
								) THEN ACCUM_QTY_REC_ONLY + ISNULL(ISS_QTY, 0)
								ELSE NULL END
							) 
						WHEN GR_DATE = MIN(GR_DATE) OVER (
							PARTITION BY PLANT_CODE, MAT_NUM, STO_LOC, FLG_LEFTOVER_QTY
						) THEN ACCUM_QTY_REC_ONLY + ISNULL(ISS_QTY, 0)
						ELSE REC_QTY
					END                                                             AS REMAINING_QTY
					, CASE WHEN GR_DATE = MAX(GR_DATE) OVER (
							PARTITION BY PLANT_CODE, MAT_NUM, STO_LOC
						) THEN ACCUM_AMNT_REC_ONLY + ISNULL(ISS_AMOUNT, 0)
						ELSE 0
					END                                                             AS REMAINING_AMNT
					FROM #TMP_ACCUM_CUR_MONTH
				)
					SELECT
						PLANT_CODE
						, MAT_NUM
						, STO_LOC
						, GR_DATE
						, REMAINING_AMNT
						, REMAINING_QTY
						, CASE WHEN REMAINING_QTY > 0 
							THEN DATEDIFF(M, GR_DATE, CONVERT(DATE, @dateLastMonth)) 
							ELSE NULL 
						END                                                           AS AGING_MONTH
					INTO #TMP_1_AGING
					FROM TMP_BALANCE
				;   

				INSERT INTO #TMP_1_AGING
				SELECT
					T.PLANT_CODE
					, T.MATERIAL_NUMBER                                                 AS MAT_NUM
					, T.STORAGE_LOCATION                                                AS STO_LOC
					, CONVERT(DATE, CONVERT(VARCHAR, DATE_WID))                         AS GR_DATE
					, LOCAL_AMOUNT                                                      AS REMAINING_AMNT
					, QUANTITY                                                          AS REMAINING_QTY
					, -1                                                                AS AGING_MONTH
				FROM [dbo].[W_SAP_SPP_TRANSACTION_F] T
				WHERE 1=1
					and CONVERT(VARCHAR, DATE_WID) <= @dateLastMonth
					AND NOT EXISTS (
						SELECT MAT_NUM, PLANT_CODE, STO_LOC FROM #TMP_1_AGING A
						WHERE 1=1
							AND T.MATERIAL_NUMBER = A.MAT_NUM
							AND T.PLANT_CODE = A.PLANT_CODE
							AND T.STORAGE_LOCATION = A.STO_LOC
					)
			;  


			SELECT
				PLANT_CODE
				, MAT_NUM
				, STO_LOC

				, SUM(REMAINING_AMNT)   AS RMN_AMNT
				, SUM(REMAINING_QTY)    AS RMN_QTY
			INTO #TMP_REMAINING
			FROM #TMP_1_AGING
			GROUP BY PLANT_CODE, MAT_NUM, STO_LOC;

			SELECT
				CASE WHEN ACC.AGING_MONTH BETWEEN 0 AND 4 THEN '< 4 MONTHS'
					WHEN ACC.AGING_MONTH BETWEEN 4 AND 12 THEN '4 - 12 MONTHS'
					WHEN ACC.AGING_MONTH BETWEEN 12 AND 24 THEN '1 - 2 YEARS'
					WHEN ACC.AGING_MONTH BETWEEN 24 AND 36 THEN '2 - 3 YEARS'
					WHEN ACC.AGING_MONTH BETWEEN 36 AND 48 THEN '3 - 4 YEARS'
					WHEN ACC.AGING_MONTH BETWEEN 48 AND 60 THEN '4 - 5 YEARS'
					WHEN ACC.AGING_MONTH > 60 THEN '> 5 YEARS'
					ELSE 'AGE_NULL'
				END                                                             AS AGING_GROUP
				, ACC.PLANT_CODE
				, ACC.MAT_NUM
				, ACC.GR_DATE
				, ACC.STO_LOC
				, ACC.REMAINING_QTY
				, ACC.REMAINING_AMNT
			INTO #TMP_TRANS_AGING
			FROM #TMP_1_AGING ACC
			;


			PRINT '--> 2. Pivot';

			SELECT
				TMP.MAT_NUM
				, TMP.PLANT_CODE
				, TMP.STO_LOC
				, [< 4 MONTHS]
				, [4 - 12 MONTHS]
				, [1 - 2 YEARS]
				, [2 - 3 YEARS]
				, [3 - 4 YEARS]
				, [4 - 5 YEARS]
				, [> 5 YEARS]
				, [AGE_NULL]
				, CONCAT_WS('-', TMP.MAT_NUM, TMP.PLANT_CODE, TMP.STO_LOC) AS [KEY]
			INTO #TMP_PIVOT
			FROM (
				SELECT MAT_NUM, PLANT_CODE, AGING_GROUP, STO_LOC
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
					, [> 5 YEARS]
					, [AGE_NULL]
				)
			) AS TMP;



			PRINT '--> 3. Create balance_month table';

			WITH TMP_B AS(
				SELECT
					PLANT_CODE
					, MAT_NUM
					, STO_LOC
					, [< 4 MONTHS]
					, [4 - 12 MONTHS]
					, [1 - 2 YEARS]
					, [2 - 3 YEARS]
					, [3 - 4 YEARS]
					, [4 - 5 YEARS]
					, [> 5 YEARS]
					, [AGE_NULL]
					, [4 - 12 MONTHS] + [1 - 2 YEARS]
					+ [2 - 3 YEARS] + [3 - 4 YEARS] + [4 - 5 YEARS]     AS TOTAL_LEFTOVER
					, [3 - 4 YEARS] + [4 - 5 YEARS]                     AS SLOW_MOVING
					, [KEY]
				FROM #TMP_PIVOT
			),
			TMP_R AS (
				SELECT
					SUM(REMAINING_AMNT) AS TOT_AMNT
					, SUM(REMAINING_QTY) AS TOT_QTY
					, CONCAT_WS('-', MAT_NUM, PLANT_CODE, STO_LOC) AS [KEY]
				FROM #TMP_TRANS_AGING
				GROUP BY MAT_NUM, PLANT_CODE, STO_LOC
			)
				SELECT
					CONVERT(VARCHAR, FORMAT(@dateLastMonth, 'yyyyMMdd'))          AS DATE_WID
					, PL.PLANT_WID                                      AS PLANT_WID
					, IT.ITEM_WID                                       AS MATERIAL_WID

					, @dateLastMonth                                              AS [PERIOD]
					, TMP_B.PLANT_CODE
					, PL.PLANT_NAME_2									AS PLANT
					, TMP_B.MAT_NUM											AS MATERIAL_NUMBER
					, TMP_R.TOT_QTY                                         AS QUANTITY
					, TMP_R.TOT_AMNT                                        AS AMOUNT
					, NULL													AS [TYPE]
					, [< 4 MONTHS]
					, [4 - 12 MONTHS]
					, [1 - 2 YEARS]
					, [2 - 3 YEARS]
					, [3 - 4 YEARS]
					, [4 - 5 YEARS]
					, [> 5 YEARS]
					, [AGE_NULL]
					, TMP_B.STO_LOC											AS STORAGE_LOCATION
					, T.COMPANY_CODE
					, T.VALUATION_AREA
					, T.VALUATION_CLASS
					, T.MATERIAL_TYPE
					, T.MATERIAL_GROUP
					, T.BASE_UNIT_OF_MEASURE
					, T.CURRENCY
					, T.PRICE_CONTROL
					, T.PURCHASING_GROUP
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
						, CONVERT(VARCHAR, FORMAT(@dateLastMonth, 'yyyyMMdd'))
						, TMP_B.MAT_NUM
						, TMP_B.PLANT_CODE
						, T.STORAGE_LOCATION
					)                                                   AS W_INTEGRATION_ID
					, 'N'                                               AS W_DELETE_FLG
					, 1                                                 AS W_DATASOURCE_NUM_ID
					, GETDATE()                                         AS W_INSERT_DT
					, GETDATE()                                         AS W_UPDATE_DT
					, @p_batch_id                                       AS W_BATCH_ID
					, 'N'                                               AS W_UPDATE_FLG
					INTO #W_SAP_SPP_BALANCE_F_tmp
					FROM TMP_B
						LEFT JOIN [dbo].[W_CMMS_ITEM_D] IT ON 1=1
							AND TMP_B.MAT_NUM = IT.ITEM_NUM
					
						LEFT JOIN TMP_R ON 1=1
							AND TMP_R.[KEY] = TMP_B.[KEY]
						LEFT JOIN [FND].[W_CMMS_MINMAX_D] MM ON 1=1
							AND MM.ITEM_NUM = TMP_B.MAT_NUM
							AND MM.PLANT = TMP_B.PLANT_CODE
						LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL ON 1=1
							AND PL.PLANT = TMP_B.PLANT_CODE
							AND PL.STO_LOC = TMP_B.STO_LOC
						OUTER APPLY ( 
							SELECT top 1
								PLANT_CODE, MATERIAL_NUMBER, STORAGE_LOCATION, QUANTITY, LOCAL_AMOUNT,
								PURCHASING_GROUP, COMPANY_CODE, VALUATION_AREA, VALUATION_CLASS, MATERIAL_TYPE,
								MATERIAL_GROUP, BASE_UNIT_OF_MEASURE, CURRENCY, PRICE_CONTROL
							FROM [dbo].[W_SAP_SPP_TRANSACTION_F] TRANS
							WHERE 1=1
									AND CONVERT(DATE, CONVERT(VARCHAR, TRANS.DATE_WID)) <= @dateLastMonth
									AND TRANS.PLANT_CODE = TMP_B.PLANT_CODE
									AND TRANS.MATERIAL_NUMBER = TMP_B.MAT_NUM
									AND TRANS.STORAGE_LOCATION = TMP_B.STO_LOC
									AND TRANS.MOVEMENT_TYPE IN (SELECT MOVEMENT_TYPE FROM #TMP_MVM_TYPE)
									-- AND (
									--     TRANS.MOVEMENT_TYPE NOT IN ('201', '202', '901', '902') 
									--     OR TRANS.MOVEMENT_TYPE IS NULL
									-- )
							ORDER BY DATE_WID DESC
						) T
			;


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
				, MATERIAL_WID = src.MATERIAL_WID

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
				, [> 5 YEARS] = src.[> 5 YEARS]
				, [AGE_NULL] = src.[AGE_NULL]
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
				, W_UPDATE_DT = @today
			FROM [dbo].[W_SAP_SPP_BALANCE_F] tgt
			INNER JOIN #W_SAP_SPP_BALANCE_F_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


			-- 4. Insert non-existed records to main table from temp table
			PRINT '4. Insert non-existed records to main table from temp table'

			INSERT INTO [dbo].[W_SAP_SPP_BALANCE_F](
				PLANT_WID
				, DATE_WID
				, MATERIAL_WID

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
				, [> 5 YEARS]
				, [AGE_NULL]
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
				, MATERIAL_WID

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
				, [> 5 YEARS]
				, [AGE_NULL]
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

			
			SET @dateFrom = DATEADD(M, 1, @dateFrom);
		END;

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
	END CATCH;



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