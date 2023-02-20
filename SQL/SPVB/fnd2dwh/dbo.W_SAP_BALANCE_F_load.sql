SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[SAP_proc_load_w_spp_balance_f] @p_batch_id [bigint],@From [DATE],@To [DATE] AS 
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
			@Lastdayofmonth DATE;

    set @v_job_id= (select top 1 JOB_ID from [dbo].[SAP_ETL_JOB] where 1=1 and ACTIVE_FLG='Y' and TGT_TABLE = @tgt_TableName)
    set @v_jobinstance_id = convert(bigint, convert(varchar, @v_batch_id) + convert(varchar, @v_job_id))
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
	IF @From is null 
		begin 
			set @From = DATEADD(d, 1, EOMONTH(DATEADD(m, -1, dateadd(hh, 7, getdate()))))
		end
	else 
		begin
			set @From = DATEADD(d, 1, EOMONTH(DATEADD(m, -1,@From)))
		end ;
	IF  @To is null
		begin 
			set @To = EOMONTH(dateadd(hh, 7, getdate()))
		end
	else 
		begin 
			set @To = EOMONTH(@To)
		end;

	--IF MONTH(@dateFrom) <> MONTH(@today)
 --   SET @dateFrom = DATEADD(d, 1, EOMONTH(DATEADD(m, -1, @dateFrom)));
	--IF (@dateTo >= @today)
	--	SET @dateTo = DATEADD(D, -1, @today);
	--IF MONTH(@dateTo) <> MONTH(@today)
	--	SET @dateTo = EOMONTH(@dateTo);

	BEGIN TRY
		WHILE (@From <= @To)
		BEGIN
			SET @Lastdayofmonth = EOMONTH(@From);

			--DECLARE @Lastdayofmonth DATE
			--SET @Lastdayofmonth = '20220531'
			--PRINT @Lastdayofmonth
			delete [dbo].[W_SAP_SPP_BALANCE_F] where date_wid = convert(bigint, convert(varchar, @Lastdayofmonth,112))
		;

			-- 1. Check existence and remove of temp table
			PRINT '1. Check existence and remove of temp table'

			IF OBJECT_ID(N'tempdb..#W_SAP_SPP_BALANCE_F_tmp') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #W_SAP_SPP_BALANCE_F_tmp'
				DROP Table #W_SAP_SPP_BALANCE_F_tmp
			END;


			PRINT '--> 1. Select all receipt transactions grouped by plant, mat_num up to current month';
----------------------------------------------------------------------------------------------------			
			IF OBJECT_ID(N'tempdb..#trans') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #trans'
				DROP Table #trans
			END;

			WITH MDOC AS (
				SELECT  MBLNR, ZEILE, MATNR from [FND].[W_SAP_MATDOC_F_TEMP]
				WHERE BWART IN ('101', '102')
				AND MATNR LIKE '00000000006%'
				AND BUDAT <= @Lastdayofmonth
				GROUP BY MBLNR, ZEILE, MATNR
			),
			BDOC AS (
				SELECT MATERIAL_DOCUMENT, MATERIAL_LINE, MATERIAL_NUMBER
				FROM [dbo].[W_SAP_SPP_TRANSACTION_F]
				WHERE DATE_WID <= convert(bigint, convert(varchar,@Lastdayofmonth,112)) --- @dateLastMonth
				AND ACCOUNT_NUMBER = '0000120050'
				GROUP BY MATERIAL_DOCUMENT, MATERIAL_LINE, MATERIAL_NUMBER
			), 
			d as (			
				SELECT  D.MBLNR, D.ZEILE, D.MATNR
				FROM MDOC D 
				LEFT JOIN BDOC F ON D.MBLNR = F.MATERIAL_DOCUMENT AND CONCAT('00', D.ZEILE) = F.MATERIAL_LINE AND F.MATERIAL_NUMBER = RIGHT(D.MATNR,8)
				WHERE F.MATERIAL_LINE IS NULL
			), R AS 
			( 
				SELECT MBLNR, ZEILE, MATNR, BUDAT--, COUNT(1) 
				FROM [FND].[W_SAP_MATDOC_F_TEMP] 
				where CANCELLED = 'X' 
				AND MATNR LIKE '00000000006%'
				AND BUDAT <= @Lastdayofmonth
				GROUP BY MBLNR, ZEILE, MATNR, BUDAT			
			)		
			SELECT 
				  F.BUDAT AS POSTING_DATE
				, F.WERKS AS PLANT_CODE
				, F.LGORT AS STORAGE_LOCATION
				, F.BWTAR AS VALUATION_TYPE
				, F.VPRSV AS PRICE_CONTROL
				, REPLACE(LTRIM(REPLACE(F.MATNR, '0', ' ')), ' ', '0') AS MATERIAL_NUMBER
				, F.STOCK_QTY AS QUANTITY 
				, F.MBLNR AS MATERIAL_DOCUMENT
				, CONCAT('00', F.ZEILE) AS MATERIAL_LINE
				, F.bwart as MOVEMENT_TYPE
				, CASE WHEN F.SMBLN = '' THEN NULL ELSE F.SMBLN END AS [Orginal_document]
				, CASE WHEN F.SMBLP = '0000' THEN NULL ELSE F.SMBLP END AS ORIGINAL_LINE_ITEM
				, R.BUDAT AS ORG_POSTING_DATE
				, F.EBELN AS PURCHASE_DOCUMENT
				, F.EBELP AS PURCHASE_LINE_ITEM
				, F.LBKUM AS OB_QUANTITY
				, CASE WHEN F.WAERS IN ('VND', 'JPY') THEN F.SALK3*100 ELSE F.SALK3 END AS OB_VALUE
				, CASE WHEN F.WAERS IN ('VND', 'JPY') THEN F.DMBTR_STOCK*100 ELSE F.DMBTR_STOCK END AS STOCK_VALUE
				, F.SHKZG AS DEBIT_IND
				, 0 as LOCAL_AMT
				, CASE WHEN F.WAERS IN ('VND', 'JPY') THEN F.BNBTR*100 ELSE F.DMBTR_STOCK END AS DELIVERY_COST
			into #trans
			FROM [FND].[W_SAP_MATDOC_F_TEMP] F
			LEFT JOIN R ON R.MBLNR = F.SMBLN AND R.ZEILE = F.SMBLP AND F.BWART = '312'
			WHERE 
			F.BWART IN ('311', '312') 
			AND F.MATNR LIKE '00000000006%'
			--AND ACCOUNT_NUMBER = '0000120050'
			AND F.BUDAT <= @Lastdayofmonth	 --@dateLastMonth	
			UNION ALL
			SELECT
			   CONVERT(DATE, CONVERT(VARCHAR, F.DATE_WID)) AS POSTING_DATE
			 , F.PLANT_CODE
			 , F.STORAGE_LOCATION
			 , F.VALUATION_TYPE
			 , F.PRICE_CONTROL
			 , F.MATERIAL_NUMBER 
			 , F.QUANTITY  
			 , F.MATERIAL_DOCUMENT
			,  F.MATERIAL_LINE
			,  F.MOVEMENT_TYPE
			, F.ORIGINAL_DOCUMENT	
			, F.ORIGINAL_LINE_ITEM
			, R.BUDAT
			, PURCHASE_DOCUMENT
			, PURCHASE_LINE_ITEM
			, OPENING_VOLUMN
			, OPENING_VALUE
			, STOCK_VALUE
			, F.DEBIT_INDICATOR 
			, F.LOCAL_AMOUNT AS LOCAL_AMT
			, DELIVERY_COST
			FROM [dbo].[W_SAP_SPP_TRANSACTION_F] F
			left join R ON R.MBLNR = F.ORIGINAL_DOCUMENT AND R.ZEILE = F.ORIGINAL_LINE_ITEM
			WHERE 1=1
				AND F.QUANTITY <> 0 
			AND ACCOUNT_NUMBER = '0000120050'
			AND F.date_wid <= convert(bigint, convert(varchar, @Lastdayofmonth,112)) 
			AND F.MOVEMENT_TYPE NOT IN ('311', '312')
			union all
			SELECT 
				  F.BUDAT AS POSTING_DATE
				, F.WERKS AS PLANT_CODE
				, F.LGORT AS STORAGE_LOCATION
				, F.BWTAR AS VALUATION_TYPE
				, F.VPRSV AS PRICE_CONTROL
				, REPLACE(LTRIM(REPLACE(F.MATNR, '0', ' ')), ' ', '0') AS MATERIAL_NUMBER
				, F.STOCK_QTY AS QUANTITY 
				, F.MBLNR AS MATERIAL_DOCUMENT
				, CONCAT('00', F.ZEILE) AS MATERIAL_LINE
				, F.bwart as MOVEMENT_TYPE
				, CASE WHEN F.SMBLN = '' THEN NULL ELSE F.SMBLN END AS [Orginal_document]
				, CASE WHEN F.SMBLP = '0000' THEN NULL ELSE F.SMBLP END AS ORIGINAL_LINE_ITEM
				, R.BUDAT AS ORG_POSTING_DATE
				, F.EBELN AS PURCHASE_DOCUMENT
				, F.EBELP AS PURCHASE_LINE_ITEM
				, F.LBKUM AS OB_QUANTITY
				, CASE WHEN F.WAERS IN ('VND', 'JPY') THEN F.SALK3*100 ELSE F.SALK3 END AS OB_VALUE
				, CASE WHEN F.WAERS IN ('VND', 'JPY') THEN (F.DMBTR_STOCK+F.BNBTR)*100 ELSE F.DMBTR_STOCK END AS STOCK_VALUE
				, F.SHKZG AS DEBIT_IND
				, 0 AS LOCAL_AMOUNT
				, CASE WHEN F.WAERS IN ('VND', 'JPY') THEN F.BNBTR*100 ELSE F.DMBTR_STOCK END AS DELIVERY_COST
			FROM [FND].[W_SAP_MATDOC_F_TEMP] f
			inner join D ON F.MBLNR = D.MBLNR AND F.ZEILE = D.ZEILE AND F.MATNR = D.MATNR
			LEFT JOIN R ON R.MBLNR = F.SMBLN AND R.ZEILE = F.SMBLP AND F.BWART = '102'
			WHERE F.BUDAT <= @Lastdayofmonth 

--Select plant_code, storage_location,material_number
--, SUM(CASE WHEN MOVEMENT_TYPE IN ('101', '102') THEN CASE WHEN STOCK_VALUE = 0 THEN OB_VALUE/OB_QUANTITY*QUANTITY ELSE STOCK_VALUE + DELIVERY_COST END ELSE 0 END) AS TOTAL_RECEIPT
--, NULL AS TOTAL_ISSUE
--,  SUM(CASE WHEN MOVEMENT_TYPE IN ('101', '102') THEN (CASE WHEN STOCK_VALUE = 0 THEN OB_VALUE/OB_QUANTITY*QUANTITY ELSE STOCK_VALUE END) ELSE 0 END) AS PURCHASE_VALUE
--,  SUM(CASE WHEN MOVEMENT_TYPE IN ('201', '202') THEN (CASE WHEN STOCK_VALUE = 0 THEN OB_VALUE/OB_QUANTITY*QUANTITY ELSE STOCK_VALUE END) ELSE 0 END) AS INTERNAL_ISSUE_VALUE
--,  SUM(CASE WHEN MOVEMENT_TYPE IN ('311', '312', '641', '642') AND DEBIT_IND = 'S' THEN (CASE WHEN STOCK_VALUE = 0 THEN OB_VALUE/OB_QUANTITY*QUANTITY ELSE STOCK_VALUE END) ELSE 0 END) AS OTHER_VALUE
--,  SUM(CASE WHEN MOVEMENT_TYPE IN ('311', '312') AND DEBIT_IND = 'H' THEN (CASE WHEN STOCK_VALUE = 0 THEN OB_VALUE/OB_QUANTITY*QUANTITY ELSE STOCK_VALUE END) ELSE 0 END) AS OTHER_ISSUE_VALUE
--,  SUM(CASE WHEN MOVEMENT_TYPE IN ( '641', '642') AND DEBIT_IND = 'H' THEN (CASE WHEN STOCK_VALUE = 0 THEN OB_VALUE/OB_QUANTITY*QUANTITY ELSE STOCK_VALUE END) ELSE 0 END) AS TRANSFER_ISSUE_VALUE
--,  SUM(CASE WHEN MOVEMENT_TYPE IN ('901', '901') THEN (CASE WHEN STOCK_VALUE = 0 THEN OB_VALUE/OB_QUANTITY*QUANTITY ELSE STOCK_VALUE END) ELSE 0 END) AS MANUFACTURER_VALUE
--from #trans
--where material_number = '62184079' and plant_code = '1050'-- and storage_location IN ( 'OV01', 'SP01')
--AND POSTING_DATE >= '20230101'
--GROUP BY plant_code, storage_location,material_number
--ORDER BY 2


---------------------------------------------------------------------------------------------			
			IF OBJECT_ID(N'tempdb..#Pre_Balance') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #Pre_Balance'
				DROP Table #Pre_Balance
			END;

			SELECT PLANT_CODE, STORAGE_LOCATION, VALUATION_TYPE, PRICE_CONTROL, MATERIAL_NUMBER, SUM(QUANTITY) AS QUANTITY
			into #Pre_Balance
			FROM #Trans 
			GROUP BY PLANT_CODE, STORAGE_LOCATION
			, VALUATION_TYPE
			, PRICE_CONTROL
			, MATERIAL_NUMBER
------------------------------------------------------------------		
			IF OBJECT_ID(N'tempdb..#Total_Value') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #Total_Value'
				DROP Table #Total_Value
			END;
			
			SELECT PLANT_CODE, MATERIAL_NUMBER, SUM(LOCAL_AMOUNT) AS SPP_VALUE
			into #Total_Value
			FROM [dbo].[W_SAP_SPP_TRANSACTION_F] F
			WHERE PRICE_CONTROL = 'V' AND ACCOUNT_NUMBER = '0000120050'
			AND DATE_WID <= convert(bigint, convert(varchar,@Lastdayofmonth,112))
			GROUP BY PLANT_CODE, MATERIAL_NUMBER
			HAVING SUM(LOCAL_AMOUNT) <> 0

---------------------------------------------------------------
			IF OBJECT_ID(N'tempdb..#Balance') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #Balance'
				DROP Table #Balance
			END;

		SELECT  
			convert(bigint, convert(varchar, @Lastdayofmonth,112)) AS DATE_WID --@dateLastMonth
			, ISNULL(P.PRODUCT_WID,0) AS MATERIAL_WID
			, ISNULL(PL.PLANT_WID, 0) AS PLANT_WID
			, B.PLANT_CODE
			, PRICE_CONTROL
			, B.VALUATION_TYPE
			, STORAGE_LOCATION
			, B.MATERIAL_NUMBER
			, B.QUANTITY
			, CASE 
				WHEN  B.STORAGE_LOCATION IN ('SP02', 'SP03') THEN 0
				WHEN SUM(CASE WHEN B.STORAGE_LOCATION IN ('SP02', 'SP03') THEN 0 ELSE B.QUANTITY END) OVER (PARTITION BY B.PLANT_CODE, B.MATERIAL_NUMBER) = 0 THEN 0
				ELSE
				TV.SPP_VALUE/SUM(CASE WHEN B.STORAGE_LOCATION IN ('SP02', 'SP03') THEN 0 ELSE B.QUANTITY END) OVER (PARTITION BY B.PLANT_CODE, B.MATERIAL_NUMBER)*B.QUANTITY 
				END 
				AS SPP_VALUE
		into #Balance
		FROM #Pre_Balance B
		LEFT JOIN [dbo].[W_PRODUCT_D] P ON P.PRODUCT_CODE = B.MATERIAL_NUMBER AND P.W_DATASOURCE_NUM_ID = 1
		LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL ON PL.PLANT = B.PLANT_CODE AND PL.STo_LOC = B.STORAGE_LOCATION--CASE WHEN B.STORAGE_LOCATION = 'OV01' AND QUANTITY < 0 THEN 'SP01' ELSE B.STORAGE_LOCATION END
		LEFT JOIN #Total_Value TV ON TV.PLANT_CODE = B.PLANT_CODE AND TV.MATERIAL_NUMBER = B.MATERIAL_NUMBER
		WHERE QUANTITY <> 0

---------------------------------------------------------------------------------------------------------------------------
--In
			IF OBJECT_ID(N'tempdb..#In') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #In'
				DROP Table #In
			END;

			SELECT CASE WHEN ORG_POSTING_DATE IS NOT NULL THEN CONVERT(DATE,ORG_POSTING_DATE) ELSE POSTING_DATE END AS POSTING_DATE,
			 PLANT_CODE
			, STORAGE_LOCATION
			, MATERIAL_NUMBER
			, SUM(CASE WHEN (QUANTITY > 0 AND ORG_POSTING_DATE IS NULL) OR (QUANTITY < 0 AND ORG_POSTING_DATE IS NOT NULL) THEN QUANTITY ELSE 0 END) AS [In]
			INTO #In
			FROM #Trans 
			WHERE STORAGE_LOCATION IN ('SP01', 'SP05') 
			GROUP BY CASE WHEN ORG_POSTING_DATE IS NOT NULL THEN CONVERT(DATE,ORG_POSTING_DATE) ELSE POSTING_DATE END,
			 PLANT_CODE
			, STORAGE_LOCATION
			, MATERIAL_NUMBER
			HAVING SUM(CASE WHEN (QUANTITY > 0 AND ORG_POSTING_DATE IS NULL) OR (QUANTITY < 0 AND ORG_POSTING_DATE IS NOT NULL) THEN QUANTITY ELSE 0 END) <> 0
----------------------------------------------------------------------------------------------------
-- out
			IF OBJECT_ID(N'tempdb..#Out') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #Out'
				DROP Table #Out
			END;

			SELECT
			 PLANT_CODE
			, STORAGE_LOCATION
			, MATERIAL_NUMBER
			, SUM(CASE WHEN (QUANTITY > 0 AND ORG_POSTING_DATE IS NULL) OR (QUANTITY < 0 AND ORG_POSTING_DATE IS NOT NULL) THEN 0  ELSE QUANTITY END) AS [Out]
			INTO #Out
			FROM #Trans 
			WHERE STORAGE_LOCATION IN ('SP01', 'SP05') 
			GROUP by 
			 PLANT_CODE
			, STORAGE_LOCATION
			, MATERIAL_NUMBER
			having SUM(CASE WHEN (QUANTITY > 0 AND ORG_POSTING_DATE IS NULL) OR (QUANTITY < 0 AND ORG_POSTING_DATE IS NOT NULL) THEN 0  ELSE QUANTITY END) <> 0 
;
--SELECT * FROM #Out WHERE MATERIAL_NUMBER = '62564099'
--drop table #Aging
----------------------------------------------------------------------------------------------------------------------
			IF OBJECT_ID(N'tempdb..#Aging') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #Aging'
				DROP Table #Aging
			END;

			WITH In_Out as 
			(
				select  I.* , O.[Out]
					, SUM(I.[In]) OVER (PARTITION BY I.PLANT_CODE, I.STORAGE_LOCATION, I.MATERIAL_NUMBER ORDER BY I.POSTING_DATE) AS Acc_In
					, CASE WHEN SUM(I.[In]) OVER (PARTITION BY I.PLANT_CODE, I.STORAGE_LOCATION, I.MATERIAL_NUMBER ORDER BY I.POSTING_DATE) < ABS(O.[Out]) THEN 0 ELSE 1 END FLG
				from #In I 
				LEFT JOIN #Out O on I.PLANT_CODE = O.PLANT_CODE AND I.STORAGE_LOCATION = O.STORAGE_LOCATION AND I.MATERIAL_NUMBER = O.MATERIAL_NUMBER
			)
			select  POSTING_DATE,PLANT_CODE,STORAGE_LOCATION,MATERIAL_NUMBER
					, CASE 
						WHEN (MIN(POSTING_DATE) OVER (PARTITION BY PLANT_CODE,STORAGE_LOCATION,MATERIAL_NUMBER, FLG)) = POSTING_DATE THEN Acc_In + isnull([Out],0)
						ELSE [In]
					  END REMAINING_QTY
					, CASE 
						WHEN DATEDIFF(DD, POSTING_DATE, @Lastdayofmonth)  <= 120 THEN '< 4 MONTHS'
						WHEN DATEDIFF(DD, POSTING_DATE, @Lastdayofmonth)  <= 365 THEN '4 - 12 MONTHS'
						WHEN DATEDIFF(dd, POSTING_DATE, @Lastdayofmonth)  <= 365*2 THEN '1 - 2 YEARS'
						WHEN DATEDIFF(dd, POSTING_DATE, @Lastdayofmonth)  <= 365*3 THEN '2 - 3 YEARS'
						WHEN DATEDIFF(dd, POSTING_DATE, @Lastdayofmonth)  <= 365*4 THEN '3 - 4 YEARS'
						WHEN DATEDIFF(dd, POSTING_DATE, @Lastdayofmonth)  <= 365*5 THEN '4 - 5 YEARS'
						ELSE '> 5 YEARS'
					  END [AGING_GROUP] 
			into #Aging
			from In_Out
			WHERE FLG = 1 
----------------------------------------------------------------------------------------------
	--drop table #Aging
			IF OBJECT_ID(N'tempdb..#Aging_Pvt') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #Aging_Pvt'
				DROP Table #Aging_Pvt
			END;

			select PLANT_CODE,STORAGE_LOCATION,MATERIAL_NUMBER
				, SUM([< 4 MONTHS]) AS [< 4 MONTHS]
				, SUM([4 - 12 MONTHS]) [4 - 12 MONTHS]
				, SUM([1 - 2 YEARS]) [1 - 2 YEARS]
				, SUM([2 - 3 YEARS]) [2 - 3 YEARS]
				, SUM([3 - 4 YEARS]) [3 - 4 YEARS]
				, SUM([4 - 5 YEARS]) [4 - 5 YEARS]
				, SUM([> 5 YEARS]) [> 5 YEARS]
			INTO #Aging_Pvt
			from (SELECT * FROM #Aging WHERE ISNULL(REMAINING_QTY,0) <> 0) F
			PIVOT (
				SUM(REMAINING_QTY)
				FOR AGING_GROUP IN (
					[< 4 MONTHS]
					, [4 - 12 MONTHS]
					, [1 - 2 YEARS]
					, [2 - 3 YEARS]
					, [3 - 4 YEARS]
					, [4 - 5 YEARS]
					, [> 5 YEARS]
				)) P	
			GROUP BY PLANT_CODE,STORAGE_LOCATION,MATERIAL_NUMBER

			SELECT 
				  B.DATE_WID
				, B.MATERIAL_WID
				, B.PLANT_WID
				, B.PLANT_CODE
				, B.PRICE_CONTROL
				, B.VALUATION_TYPE
				, B.STORAGE_LOCATION
				, B.MATERIAL_NUMBER
				, B.QUANTITY
				, B.SPP_VALUE
				, AP.[< 4 MONTHS]
				, AP.[4 - 12 MONTHS]
				, AP.[1 - 2 YEARS]
				, AP.[2 - 3 YEARS]
				, AP.[3 - 4 YEARS]
				, AP.[4 - 5 YEARS]
				, AP.[> 5 YEARS]
				, AP.[3 - 4 YEARS] + AP.[4 - 5 YEARS] + AP.[> 5 YEARS] as SLOW_MOVING_QUANTITY
				, NULL AS SLOW_MOVING_FLG 
				, CASE WHEN B.QUANTITY = 0 THEN NULL ELSE B.SPP_VALUE/B.QUANTITY END AS UNIT_PRICE
				, CONCAT( FORMAT(@Lastdayofmonth, 'yyyyMMdd'), '~', B.PLANT_CODE, '~', B.STORAGE_LOCATION, '~', B.MATERIAL_NUMBER)  AS W_INTEGRATION_ID
				, 'N'   AS W_DELETE_FLG
				, 1     AS W_DATASOURCE_NUM_ID
				, DATEADD(HH, 7, GETDATE())  AS W_INSERT_DT
				, DATEADD(HH, 7, GETDATE())   AS W_UPDATE_DT
				, @p_batch_id                                       AS W_BATCH_ID
			INTO #W_SAP_SPP_BALANCE_F_tmp
			FROM #BALANCE B
			LEFT JOIN #Aging_Pvt AP ON AP.PLANT_CODE = B.PLANT_CODE AND AP.STORAGE_LOCATION = B.STORAGE_LOCATION AND AP.MATERIAL_NUMBER = B.MATERIAL_NUMBER
		

			-- 4. Insert non-existed records to main table from temp table
			PRINT '4. Insert non-existed records to main table from temp table'

			INSERT INTO [dbo].[W_SAP_SPP_BALANCE_F]
			(				
				  DATE_WID
				, MATERIAL_WID
				, PLANT_WID
				, QUANTITY
				, SPP_VALUE
				, PLANT_CODE
				, STORAGE_LOCATION
				, PRICE_CONTROL
				, VALUATION_TYPE
				, MATERIAL_NUMBER
				, [< 4 MONTHS]
				, [4 - 12 MONTHS]
				, [1 - 2 YEARS]
				, [2 - 3 YEARS]
				, [3 - 4 YEARS]
				, [4 - 5 YEARS]
				, [> 5 YEARS]
				, SLOW_MOVING_QUANTITY
				, SLOW_MOVING_FLG
				, UNIT_PRICE
				, W_DELETE_FLG
				, W_DATASOURCE_NUM_ID
				, W_INSERT_DT
				, W_UPDATE_DT
				, W_BATCH_ID
				, W_INTEGRATION_ID
			)
			SELECT
				  DATE_WID
				, MATERIAL_WID
				, PLANT_WID
				, QUANTITY
				, SPP_VALUE
				, PLANT_CODE
				, STORAGE_LOCATION
				, PRICE_CONTROL
				, VALUATION_TYPE
				, MATERIAL_NUMBER
				, [< 4 MONTHS]
				, [4 - 12 MONTHS]
				, [1 - 2 YEARS]
				, [2 - 3 YEARS]
				, [3 - 4 YEARS]
				, [4 - 5 YEARS]
				, [> 5 YEARS]
				, SLOW_MOVING_QUANTITY
				, SLOW_MOVING_FLG
				, UNIT_PRICE
				, W_DELETE_FLG
				, W_DATASOURCE_NUM_ID
				, W_INSERT_DT
				, W_UPDATE_DT
				, W_BATCH_ID
				, W_INTEGRATION_ID
			FROM #W_SAP_SPP_BALANCE_F_tmp

			SET @From = DATEADD(Mm, 1, @From);
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
