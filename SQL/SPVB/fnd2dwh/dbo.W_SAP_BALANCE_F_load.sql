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

			--DECLARE @Lastdayofmonth DATE
			--SET @Lastdayofmonth = '20220531';
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

			--DECLARE @Lastdayofmonth DATE
			--SET @Lastdayofmonth = '20220531';			
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

			--DECLARE @Lastdayofmonth DATE
			--SET @Lastdayofmonth = '20220531';
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

--SELECT SUM(SPP_VALUE) FROM #Balance
--In
			IF OBJECT_ID(N'tempdb..#In') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #In'
				DROP Table #In
			END;

			--DECLARE @Lastdayofmonth DATE
			--SET @Lastdayofmonth = '20220531';	
		WITH 
		SL AS (
				SELECT convert(varchar(100), [CMMS Storage location (TOSTORELOC/FROMSTORELOC)]) AS STORE_LOC
				, convert(varchar(100), [SAP Plant (WERKS)]) AS PLANT_CODE
				, convert(varchar(100), [SAP Storage location (LGORT)]) AS STORAGE_LOC
				FROM [STG].[W_EXCEL_SLOC_MAPPING_CMMS_VS_SAP_DS]
				WHERE [CMMS Storage location (TOSTORELOC/FROMSTORELOC)] IS NOT NULL
		),
		A AS (
			select ITEM_NUM, STORELOC AS STORE_LOC, CONVERT(DATE, actualdate) as ACTUAL_DATE, ISSUE_TYPE, CONVERT(DECIMAL(38,20), QUANTITY) AS QUANTITY, 'MATU' AS Type
			from FND.W_CMMS_MATU_F M
			where CONVERT(DATE, actualdate) <= '2020-03-29'
			and refwo ='' and issue_type = 'RETURN'
			union all
			select F.ITEMNUM AS ITEM_NUM, tostoreloc STORE_LOC, CONVERT(DATE, F.ACTUALDATE) ACTUAL_DATE, ISSUETYPE AS ISSUE_TYPE, CONVERT(DECIMAL(38,20), QUANTITY) AS QUANTITY, 'MATR' AS Type
			from FND.W_CMMS_MATR_F F
			where CONVERT(DATE, F.actualdate) <= '2020-03-29'
			AND (tostoreloc <> FromStoreloc or (tostoreloc = '' OR FromStoreloc = ''))
			and CONVERT(DECIMAL(38,20), quantity) <> 0.0
			union all
			select  ITEM_NUM, STORELOC AS STORE_LOC, CONVERT(DATE, TRANSDATE) ACTUAL_DATE, TRANSTYPE AS ISSUE_TYPE, CONVERT(DECIMAL(38,20), QUANTITY) AS QUANTITY, 'INVT' AS Type
			from FND.W_CMMS_INVT_F 
			WHERE CONVERT(DATE, TRANSDATE) <= '2020-03-29'
			and transtype not in  ('STDCSTADJ','AVGCSTADJ','PCOUNTADJ')
			AND CONVERT(DECIMAL(38,20), quantity) > 0.0
		)
		
		SELECT ACTUAL_DATE AS POSTING_DATE
			, PLANT_CODE
			, STORAGE_LOC
			, ITEM_NUM AS MATERIAL_NUMBER
			, CASE 
				WHEN ACTUAL_DATE >= DATEADD(MM,-4, @Lastdayofmonth) THEN '< 4 MONTHS'
				WHEN ACTUAL_DATE >= DATEADD(MM,-12, @Lastdayofmonth) THEN '4 - 12 MONTHS'
				WHEN ACTUAL_DATE >= DATEADD(MM,-24, @Lastdayofmonth) THEN '1 - 2 YEARS'
				WHEN ACTUAL_DATE >= DATEADD(MM,-36, @Lastdayofmonth) THEN '2 - 3 YEARS'
				WHEN ACTUAL_DATE >= DATEADD(MM,-48, @Lastdayofmonth) THEN '3 - 4 YEARS'
				WHEN ACTUAL_DATE >= DATEADD(MM,-60, @Lastdayofmonth) THEN '4 - 5 YEARS'
			  ELSE '> 5 YEARS'
				END AS [AGING_GROUP]
			, SUM(QUANTITY) AS [IN]
		INTO #In
		FROM A
		LEFT JOIN SL ON A.STORE_LOC = SL.STORE_LOC
		WHERE A.ISSUE_TYPE IN ('RECEIPT','VOIDRECEIPT','SHIPRECEIPT','RETURN','TRANSFER','SHIPTRANSFER','INSERTITEM','CURBALADJ','RECBALADJ')
		AND ISNULL(STORAGE_LOC,'') IN ('SP01', 'SP05')
		GROUP BY ACTUAL_DATE, PLANT_CODE, STORAGE_LOC, ITEM_NUM
		UNION ALL
		SELECT CASE WHEN ORG_POSTING_DATE IS NOT NULL THEN CONVERT(DATE,ORG_POSTING_DATE) ELSE POSTING_DATE END AS POSTING_DATE,
		  PLANT_CODE
		, STORAGE_LOCATION
		, MATERIAL_NUMBER
		, CASE 
			WHEN CASE WHEN ORG_POSTING_DATE IS NOT NULL THEN CONVERT(DATE,ORG_POSTING_DATE) ELSE POSTING_DATE END >= DATEADD(MM,-4, @Lastdayofmonth) THEN '< 4 MONTHS'
			WHEN CASE WHEN ORG_POSTING_DATE IS NOT NULL THEN CONVERT(DATE,ORG_POSTING_DATE) ELSE POSTING_DATE END >= DATEADD(MM,-12, @Lastdayofmonth) THEN '4 - 12 MONTHS'
			WHEN CASE WHEN ORG_POSTING_DATE IS NOT NULL THEN CONVERT(DATE,ORG_POSTING_DATE) ELSE POSTING_DATE END >= DATEADD(MM,-24, @Lastdayofmonth) THEN '1 - 2 YEARS'
			WHEN CASE WHEN ORG_POSTING_DATE IS NOT NULL THEN CONVERT(DATE,ORG_POSTING_DATE) ELSE POSTING_DATE END >= DATEADD(MM,-36, @Lastdayofmonth) THEN '2 - 3 YEARS'
			WHEN CASE WHEN ORG_POSTING_DATE IS NOT NULL THEN CONVERT(DATE,ORG_POSTING_DATE) ELSE POSTING_DATE END >= DATEADD(MM,-48, @Lastdayofmonth) THEN '3 - 4 YEARS'
			WHEN CASE WHEN ORG_POSTING_DATE IS NOT NULL THEN CONVERT(DATE,ORG_POSTING_DATE) ELSE POSTING_DATE END >= DATEADD(MM,-60, @Lastdayofmonth) THEN '4 - 5 YEARS'
			ELSE '> 5 YEARS'
		  END AS [AGING_GROUP] 
		, SUM(CASE WHEN (QUANTITY > 0 AND ORG_POSTING_DATE IS NULL) OR (QUANTITY < 0 AND ORG_POSTING_DATE IS NOT NULL) THEN QUANTITY ELSE 0 END) AS [In]
		FROM #Trans 
		WHERE STORAGE_LOCATION IN ('SP01', 'SP05') 
		AND MOVEMENT_TYPE <> '561'
		GROUP BY CASE WHEN ORG_POSTING_DATE IS NOT NULL THEN CONVERT(DATE,ORG_POSTING_DATE) ELSE POSTING_DATE END,
		 PLANT_CODE
		, STORAGE_LOCATION
		, MATERIAL_NUMBER
		HAVING SUM(CASE WHEN (QUANTITY > 0 AND ORG_POSTING_DATE IS NULL) OR (QUANTITY < 0 AND ORG_POSTING_DATE IS NOT NULL) THEN QUANTITY ELSE 0 END) <> 0



----------------------------------------------------------------------------------------------------
-- out
			IF OBJECT_ID(N'tempdb..#In_Agg_by_Aging') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #In_Agg_by_Aging'
				DROP Table #In_Agg_by_Aging
			END;

			select   PLANT_CODE,STORAGE_LOC,MATERIAL_NUMBER
					,SUM([< 4 MONTHS]) AS [< 4 MONTHS]
					,SUM([4 - 12 MONTHS]) AS [4 - 12 MONTHS]
					,SUM([1 - 2 YEARS]) AS [1 - 2 YEARS]
					,SUM([2 - 3 YEARS]) AS [2 - 3 YEARS]
					,SUM([3 - 4 YEARS]) AS [3 - 4 YEARS]
					,SUM([4 - 5 YEARS]) AS [4 - 5 YEARS]
					,SUM([> 5 YEARS]) AS [> 5 YEARS]
			into #In_Agg_by_Aging
			from #In
			PIVOT (	
				SUM([In])
				FOR AGING_GROUP IN 
				(
					  [< 4 MONTHS], [4 - 12 MONTHS], [1 - 2 YEARS], [2 - 3 YEARS], [3 - 4 YEARS], [4 - 5 YEARS], [> 5 YEARS])
				) pv
			GROUP BY PLANT_CODE,STORAGE_LOC,MATERIAL_NUMBER
----------------------------------------------------------------------------------------------------------------------
			IF OBJECT_ID(N'tempdb..#Last_Purchase') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #Last_Purchase'
				DROP Table #Last_Purchase
			END;

			WITH 
			SL AS (
					SELECT convert(varchar(100), [CMMS Storage location (TOSTORELOC/FROMSTORELOC)]) AS STORE_LOC
					, convert(varchar(100), [SAP Plant (WERKS)]) AS PLANT_CODE
					, convert(varchar(100), [SAP Storage location (LGORT)]) AS STORAGE_LOC
					FROM [STG].[W_EXCEL_SLOC_MAPPING_CMMS_VS_SAP_DS]
					WHERE [CMMS Storage location (TOSTORELOC/FROMSTORELOC)] IS NOT NULL
			)
			SELECT PLANT_CODE,STORAGE_LOCATION,  MATERIAL_NUMBER, MAX(POSTING_DATE) AS LAST_PURCHASE
			into #Last_Purchase
			FROM (
				SELECT PLANT_CODE,STORAGE_LOCATION,  MATERIAL_NUMBER, MAX(POSTING_DATE) AS POSTING_DATE 
				FROM #TRANS 
				WHERE PURCHASE_DOCUMENT <> '' GROUP BY PLANT_CODE, MATERIAL_NUMBER, STORAGE_LOCATION
				UNION ALL
				SELECT SL.PLANT_CODE, SL.STORAGE_LOC, F.ITEMNUM AS ITEM_NUM,MAX(CONVERT(DATE, F.ACTUALDATE)) AS POSTING_DATE 
				FROM FND.W_CMMS_MATR_F F 
				LEFT JOIN SL ON SL.STORE_LOC = F.TOSTORELOC
				WHERE CONVERT(DATE, F.actualdate) <= '2020-03-29' AND ISSUETYPE = 'RECEIPT' AND PONUM IS NOT NULL
				GROUP BY F.ITEMNUM, SL.STORAGE_LOC, SL.PLANT_CODE
			) A
			GROUP BY PLANT_CODE,STORAGE_LOCATION,  MATERIAL_NUMBER
----------------------------------------------------------------------------------------------------------------------
			IF OBJECT_ID(N'tempdb..#Last_Use') IS NOT NULL 
			BEGIN
				PRINT N'DELETE temporary table #Last_Use'
				DROP Table #Last_Use
			END;

			WITH 
			SL AS (
					SELECT convert(varchar(100), [CMMS Storage location (TOSTORELOC/FROMSTORELOC)]) AS STORE_LOC
					, convert(varchar(100), [SAP Plant (WERKS)]) AS PLANT_CODE
					, convert(varchar(100), [SAP Storage location (LGORT)]) AS STORAGE_LOC
					FROM [STG].[W_EXCEL_SLOC_MAPPING_CMMS_VS_SAP_DS]
					WHERE [CMMS Storage location (TOSTORELOC/FROMSTORELOC)] IS NOT NULL
			)
			SELECT PLANT_CODE,STORAGE_LOCATION, MATERIAL_NUMBER, MAX(POSTING_DATE ) AS LAST_USE
			into #Last_Use
			FROM (
				SELECT PLANT_CODE,STORAGE_LOCATION, MATERIAL_NUMBER, MAX(POSTING_DATE) AS POSTING_DATE 				
				FROM #TRANS 
				WHERE MOVEMENT_TYPE IN ('201', '202')
				GROUP BY PLANT_CODE, MATERIAL_NUMBER, STORAGE_LOCATION
				UNION ALL
				SELECT SL.PLANT_CODE, STORAGE_LOC, F.ITEM_NUM AS ITEM_NUM,MAX(CONVERT(DATE, F.ACTUALDATE)) AS POSTING_DATE 
				FROM FND.W_CMMS_MATU_F F 
				LEFT JOIN SL ON SL.STORE_LOC = F.STORELOC
				WHERE CONVERT(DATE, F.actualdate) <= '2020-03-29' AND ISSUE_TYPE = 'ISSUE' AND REFWO <> ''
				GROUP BY F.ITEM_NUM, STORAGE_LOC, SL.PLANT_CODE
			) A
			GROUP BY PLANT_CODE,STORAGE_LOCATION, MATERIAL_NUMBER

----------------------------------------------------------------------------------------------------------------------
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
				, CASE 
					WHEN B.STORAGE_LOCATION NOT IN ('SP01', 'SP05') THEN NULL 
					WHEN ISNULL(IA.[< 4 MONTHS],0) <= B.QUANTITY THEN IA.[< 4 MONTHS]
					ELSE B.QUANTITY
				 END [< 4 MONTHS]
				, CASE 
					WHEN B.STORAGE_LOCATION NOT IN ('SP01', 'SP05') THEN NULL 
					WHEN B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) < 0 THEN NULL
					WHEN ISNULL(IA.[4 - 12 MONTHS],0) <= B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) THEN IA.[4 - 12 MONTHS]
					ELSE B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0)
				  END [4 - 12 MONTHS]
				, CASE 
					WHEN B.STORAGE_LOCATION NOT IN ('SP01', 'SP05') THEN NULL
					WHEN B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) < 0 THEN NULL
					WHEN ISNULL(IA.[1 - 2 YEARS],0) <= B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0)  THEN IA.[1 - 2 YEARS]
					ELSE B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0)
				  END [1 - 2 YEARS]
				, CASE 
					WHEN B.STORAGE_LOCATION NOT IN ('SP01', 'SP05') THEN NULL
					WHEN B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) - ISNULL(IA.[1 - 2 YEARS],0) < 0 THEN NULL
					WHEN ISNULL(IA.[2 - 3 YEARS],0) <= B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) - ISNULL(IA.[1 - 2 YEARS],0) THEN IA.[2 - 3 YEARS]
					ELSE B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) - ISNULL(IA.[1 - 2 YEARS],0)
				  END [2 - 3 YEARS]
				, CASE 
					WHEN B.STORAGE_LOCATION NOT IN ('SP01', 'SP05') THEN NULL
					WHEN B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) - ISNULL(IA.[1 - 2 YEARS],0) - ISNULL(IA.[2 - 3 YEARS],0) < 0 THEN NULL
					WHEN ISNULL(IA.[3 - 4 YEARS],0) <= B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) - ISNULL(IA.[1 - 2 YEARS],0) - ISNULL(IA.[2 - 3 YEARS],0) THEN IA.[3 - 4 YEARS]
					ELSE B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) - ISNULL(IA.[1 - 2 YEARS],0) - ISNULL(IA.[2 - 3 YEARS],0)
				  END [3 - 4 YEARS]
				, CASE 
					WHEN B.STORAGE_LOCATION NOT IN ('SP01', 'SP05') THEN NULL
					WHEN B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) - ISNULL(IA.[1 - 2 YEARS],0) - ISNULL(IA.[2 - 3 YEARS],0) - ISNULL(IA.[3 - 4 YEARS],0) < 0 THEN NULL
					WHEN ISNULL(IA.[4 - 5 YEARS],0) <= B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) - ISNULL(IA.[1 - 2 YEARS],0) - ISNULL(IA.[2 - 3 YEARS],0) - ISNULL(IA.[3 - 4 YEARS],0) THEN IA.[4 - 5 YEARS]
					ELSE B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) - ISNULL(IA.[1 - 2 YEARS],0) - ISNULL(IA.[2 - 3 YEARS],0) - ISNULL(IA.[3 - 4 YEARS],0) 
				  END [4 - 5 YEARS]
				, CASE 
					WHEN B.STORAGE_LOCATION NOT IN ('SP01', 'SP05') THEN NULL
					WHEN B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) - ISNULL(IA.[1 - 2 YEARS],0) - ISNULL(IA.[2 - 3 YEARS],0) - ISNULL(IA.[3 - 4 YEARS],0) - ISNULL(IA.[4 - 5 YEARS],0) < 0 THEN NULL
					WHEN ISNULL(IA.[> 5 YEARS],0) <= B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) - ISNULL(IA.[1 - 2 YEARS],0) - ISNULL(IA.[2 - 3 YEARS],0) - ISNULL(IA.[3 - 4 YEARS],0) - ISNULL(IA.[4 - 5 YEARS],0) THEN IA.[> 5 YEARS]
					ELSE B.QUANTITY  - ISNULL(IA.[< 4 MONTHS],0) - ISNULL(IA.[4 - 12 MONTHS],0) - ISNULL(IA.[1 - 2 YEARS],0) - ISNULL(IA.[2 - 3 YEARS],0) - ISNULL(IA.[3 - 4 YEARS],0) - ISNULL(IA.[4 - 5 YEARS],0)
				  END [> 5 YEARS]
				, NULL AS SLOW_MOVING_QUANTITY
				, CASE 
					WHEN B.STORAGE_LOCATION NOT IN ('SP01', 'SP05') THEN NULL
					WHEN ISNULL(LAST_PURCHASE, '1900-01-01') >= DATEADD(YY,-3, @Lastdayofmonth) THEN 'Purchase in 3 years' 
					WHEN ISNULL(LAST_USE, '1900-01-01') >= DATEADD(YY,-3, @Lastdayofmonth) THEN 'No Mvt'
				  END AS SLOW_MOVING_FLG 
				, CASE WHEN B.STORAGE_LOCATION NOT IN ('SP01', 'SP05') THEN NULL ELSE LP.LAST_PURCHASE END LAST_PURCHASE
				, CASE WHEN B.STORAGE_LOCATION NOT IN ('SP01', 'SP05') THEN NULL ELSE LU.LAST_USE END LAST_USE
				, CASE WHEN B.QUANTITY = 0 THEN NULL ELSE B.SPP_VALUE/B.QUANTITY END AS UNIT_PRICE
				, CONCAT( FORMAT(@Lastdayofmonth, 'yyyyMMdd'), '~', B.PLANT_CODE, '~', B.STORAGE_LOCATION, '~', B.MATERIAL_NUMBER)  AS W_INTEGRATION_ID
				, 'N'   AS W_DELETE_FLG
				, 1     AS W_DATASOURCE_NUM_ID
				, DATEADD(HH, 7, GETDATE())  AS W_INSERT_DT
				, DATEADD(HH, 7, GETDATE())   AS W_UPDATE_DT
				, @p_batch_id                                       AS W_BATCH_ID
			INTO #W_SAP_SPP_BALANCE_F_tmp
			FROM #BALANCE B
			LEFT JOIN #In_Agg_by_Aging IA ON IA.PLANT_CODE = B.PLANT_CODE AND IA.STORAGE_LOC = B.STORAGE_LOCATION AND IA.MATERIAL_NUMBER = B.MATERIAL_NUMBER
			LEFT JOIN #Last_Purchase LP ON LP.PLANT_CODE = B.PLANT_CODE AND LP.STORAGE_LOCATION = B.STORAGE_LOCATION AND LP.MATERIAL_NUMBER = B.MATERIAL_NUMBER
			LEFT JOIN #Last_Use LU ON LU.PLANT_CODE = B.PLANT_CODE AND LU.STORAGE_LOCATION = B.STORAGE_LOCATION AND LU.MATERIAL_NUMBER = B.MATERIAL_NUMBER

--alter table [dbo].[W_SAP_SPP_BALANCE_F]
--add LAST_PURCHASE date, LAST_USE date

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
				, LAST_PURCHASE
				, LAST_USE
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
				, LAST_PURCHASE
				, LAST_USE
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
