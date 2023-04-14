SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [dbo].[CMMS_proc_load_w_transaction_f] @p_batch_id [bigint] AS 
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_CMMS_TRANSACTION_F',
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

    execute	[dbo].[SAP_proc_etl_util_start_job_instance]
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

        IF OBJECT_ID(N'tempdb..#tmp_trans') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #tmp_trans'
            DROP Table #tmp_trans
        END;
		IF OBJECT_ID(N'tempdb..#TMP_WO_X') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #TMP_WO_X'
            DROP Table #TMP_WO_X
        END;
		IF OBJECT_ID(N'tempdb..#TMP_WO_STA') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #TMP_WO_STA'
            DROP Table #TMP_WO_STA
        END;

		-- 2. Select everything into temp table
		PRINT '2.1. Create temp table';
		CREATE TABLE #tmp_trans (
			[LOC_WID] INT NULL,
			[ASSET_WID] INT NULL,
			[INVU_WID] INT NULL,
			[INVUL_WID] INT NULL,
			[PRODUCT_WID] INT NULL,
			[WO_WID] INT NULL,
			[DATE_WID] INT NULL,
			[PLANT_WID] INT NULL,

			[USAGE] [nvarchar](1000) NULL,
			[WAREHOUSE] [nvarchar](1000) NULL,
			[WAREHOUSE_NAME] [nvarchar](1000) NULL,
			[TRANSACTION_TYPE] [varchar](1000) NULL,
			[TRANSACTION_DATE] DATETIMEOFFSET NULL,
			[ACTUAL_DATE] DATETIMEOFFSET NULL,
			[ITEM_NO] [nvarchar](1000) NULL,
			[DESCRIPTION] [nvarchar](1000) NULL,
			[TRANSACTION_QUANT] [nvarchar](1000) NULL,
			[TRANSACTION_UOM] [nvarchar](1000) NULL,
			[BINNUM] [nvarchar](1000) NULL,
			[OVERHAUL] [nvarchar](1) NULL,
			[MUST_RETURN_ORIGINAL] [nvarchar](1) NULL,
			[MUST_RETURN_USER_INPUT] [nvarchar](1) NULL,
			[MUST_RETURN_REMARK] [nvarchar](1000) NULL,
			[PRICE] DECIMAL(38, 20) NULL,
			[AMOUNT] DECIMAL(38, 20) NULL,
			[MRNUM] [nvarchar](1000) NULL,
			[WORK_ORDER] [nvarchar](1000) NULL,
			[ASSET] [nvarchar](1000) NULL,
			[LINE] [nvarchar](1000) NULL,
			[WORK_TYPE] [nvarchar](1000) NULL,
			[WORKORDER_STATUS] [nvarchar](1000) NULL,
			[ACTUAL_FINISH] [nvarchar](1000) NULL,
			[WO_LAST_STATUSDATE] DATETIMEOFFSET NULL,
			[WO_DONE_BY] [nvarchar](1000) NULL,
			[USER_ID] [nvarchar](1000) NULL,
			[REASON_CODE] [nvarchar](1000) NULL,
			[JOURNAL_CMT_HEADER] [nvarchar](1000) NULL,
			[JOURNAL_CMT] [nvarchar](1000) NULL,
			[PONUM] [nvarchar](1000) NULL,
			[SAP_DND] [nvarchar](1000) NULL,
			[RET_WONUM] [nvarchar](1000) NULL,
			[SITE_ID] NVARCHAR(30) NULL,
			[FROM_TABLE] NVARCHAR(10) NULL,
			[FROM_STORE_LOC] NVARCHAR(50) NULL,
			[TO_STORE_LOC] NVARCHAR(50) NULL,

			[W_INTEGRATION_ID] [nvarchar](500) NULL,
			[W_DELETE_FLG] VARCHAR(1) NULL,
			[W_UPDATE_FLG] VARCHAR(1) NULL,
			[W_DATASOURCE_NUM_ID] INT NULL,
			[W_UPDATE_DT] DATETIME2 NULL,
			[W_INSERT_DT] DATETIME2 NULL,
			[W_BATCH_ID] [bigint] NULL
		);


        PRINT '2.2. Select everything into temp table';

		SELECT
			WORKORDER_ID
			, [STATUS]
			, MAX(WOSTATUS_ID) AS WOSTATUS_ID
		into #TMP_WO_STA
		FROM [FND].[W_CMMS_WO_STATUS_D] W
		GROUP BY WORKORDER_ID, [STATUS]
		;


		SELECT
			WO.*
			, DATE_FINISHED							AS ACTUAL_FINISH
			, CONVERT(datetimeoffset, 
				W.CHANGEDATE, 103)             		AS LAST_STATUS_DATE
			, AST.LINE_ASSET_NUM
		INTO #TMP_WO_X
		FROM [dbo].[W_CMMS_WO_F] AS WO
			LEFT JOIN [dbo].W_CMMS_ASSET_D AST ON 1=1
				AND WO.ASSET_NUM IS NOT NULL
				AND WO.ASSET_NUM <> ''
				AND WO.ASSET_NUM = AST.ASSET_NUM
				AND WO.SITE = AST.SITE_ID
			LEFT JOIN #TMP_WO_STA T ON 1=1
				AND T.WORKORDER_ID = WO.WORKORDER_ID
				AND T.STATUS = WO.[STATUS]
			LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] W ON 1=1
				AND W.WOSTATUS_ID = T.WORKORDER_ID
		;

		
		INSERT INTO #tmp_trans
		SELECT
			ISNULL(LOC.LOC_WID, 0)                  AS LOC_WID
			, ISNULL(WO.ASSET_WID, 0)             	AS ASSET_WID
			, ISNULL(INVU.INVU_WID, 0)              AS INVU_WID
			, ISNULL(INVUL.INVUL_WID, 0)            AS INVUL_WID
			, ISNULL(P.PRODUCT_WID, 0)              AS PRODUCT_WID
			, ISNULL(WO.WO_WID, 0)                	AS WO_WID
			, FORMAT(
				CONVERT(DATE, MATU.ACTUALDATE), 
				'yyyyMMdd'
			)                                       AS DATE_WID
			, ISNULL(PL.PLANT_WID, 0)				AS PLANT_WID

			, INVU.INVUSE_NUM                       AS USAGE
			, MATU.STORELOC                         AS WAREHOUSE
			, LOC.[DESCRIPTION]                     AS WAREHOUSE_NAME
			, CASE WHEN MATU.REFWO IS NULL
			AND MATU.ISSUE_TYPE = 'RETURN' 
				THEN 'RECEIPT MISC'
				ELSE MATU.ISSUE_TYPE 
			END                                     AS TRANSACTION_TYPE
			, MATU.TRANSDATE                        AS TRANSACTION_DATE
			, MATU.ACTUALDATE                       AS ACTUAL_DATE
			, MATU.ITEM_NUM                         AS ITEM_NO
			, P.PRODUCT_NAME                        AS [DESCRIPTION]
			, MATU.QUANTITY                         AS TRANSACTION_QUANT
			, MATU.ISSUE_UNIT                       AS TRANSACTION_UOM
			, MATU.BINNUM                           AS BINNUM
			, WO.OVERHAUL							AS OVERHAUL
			, CASE WHEN INVUL.SPVB_MUSTRETURN_ORG = 'True'
				THEN CONVERT(NVARCHAR, 'Y')
				ELSE CONVERT(NVARCHAR, 'N')
			END                                     AS MUST_RETURN_ORIGINAL
			, CASE WHEN INVUL.SPVB_MUSTRETURN = 'True'
				THEN CONVERT(NVARCHAR, 'Y')
				ELSE CONVERT(NVARCHAR, 'N')
			END                                     AS MUST_RETURN_USER_INPUT
			, INVUL.SPVB_REASON                     AS MUST_RETURN_REMARK
			, MATU.UNITCOST                         AS PRICE
			, MATU.LINECOST                         AS AMOUNT
			, MATU.MRNUM                            AS MRNUM
			, CASE WHEN WO.PARENT IS NULL 
				THEN WO.WORK_ORDERS
				ELSE NULL 
			END                                     AS WORK_ORDER
			, MATU.ASSET_NUM                        AS ASSET
			, WO.LINE_ASSET_NUM               		AS [LINE]
			, WO.[TYPE]                       		AS WORK_TYPE
			, WO.[STATUS]                     		AS WORKORDER_STATUS
			, WO.ACTUAL_FINISH                		AS ACTUAL_FINISH
			, WO.LAST_STATUS_DATE             		AS WO_LAST_STATUSDATE
			, WO.SUPERVISOR                   		AS WO_DONE_BY
			, INVUL.ENTER_BY                        AS [USER_ID]
			, INVUL.SPVB_EXTREASONCODE              AS REASON_CODE
			, INVU.DESCRIPTION                      AS JOURNAL_CMT_HEADER
			, INVUL.REMARK                          AS JOURNAL_CMT
			, CONVERT(NVARCHAR, MATU.PONUM)         AS PONUM
			, CONVERT(NVARCHAR, NULL)               AS SAP_DND
			, INVUL.SPVB_WONUMREF                   AS RET_WONUM
			, MATU.TO_SITEID						AS SITE_ID
			, 'MATU'								AS FROM_TABLE
			, CONVERT(VARCHAR, NULL)				AS FROM_STORE_LOC
			, CONVERT(VARCHAR, NULL)				AS TO_STORE_LOC

			, CONVERT(
				NVARCHAR(100), 
				CONCAT(MATU.MATU_ID, '~', 'NULL', '~',
						'NULL', '~', 'NULL')
			)										AS W_INTEGRATION_ID
			, 'N'                                   AS W_DELETE_FLG
			, 'N' 									AS W_UPDATE_FLG
			, 8                                     AS W_DATASOURCE_NUM_ID
			, DATEADD(HH, 7, GETDATE())             AS W_UPDATE_DT
			, DATEADD(HH, 7, GETDATE())             AS W_INSERT_DT
			, @p_batch_id                           AS W_BATCH_ID
		FROM FND.W_CMMS_MATU_F MATU
			LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
				AND LOC.[LOCATION] = MATU.STORELOC
			LEFT JOIN [dbo].[W_PRODUCT_D] P ON 1=1
				AND MATU.ITEM_NUM = P.PRODUCT_CODE
				AND P.W_DATASOURCE_NUM_ID = 1
			LEFT JOIN [dbo].[W_CMMS_ASSET_D] AST ON 1=1
				AND AST.ASSET_NUM = MATU.ASSET_NUM
				AND LEFT(AST.LOCATION, 3) = LEFT(MATU.LOCATION, 3)
			LEFT JOIN #TMP_WO_X [WO] ON 1=1
				AND WO.WORK_ORDERS = MATU.REFWO
				AND WO.SITE = MATU.TO_SITEID
			LEFT JOIN [dbo].[W_CMMS_INVUL_D] INVUL ON 1=1
				AND MATU.INVUSELINE_ID IS NOT NULL
				AND MATU.INVUSELINE_ID <> ''
				AND INVUL.INVUSELINE_ID = MATU.INVUSELINE_ID
				AND INVUL.[FROM] = 'MATU'
			LEFT JOIN [dbo].[W_CMMS_INVU_D] INVU ON 1=1
				AND INVU.INVU_ID = MATU.INVUSE_ID
				AND INVU.[FROM] = 'MATU'
			OUTER APPLY (
				SELECT TOP 1 
					M.SAP_PLANT
				FROM [FND].[W_EXCEL_SPP_MAPPING_SLOC_SAP_CMMS_D] M
				WHERE 1=1
					AND MATU.TO_SITEID = M.CMMS_PLANT
			) MAP
			LEFT JOIN dbo.W_SAP_PLANT_EXTENDED_D PL ON 1=1
				AND MAP.SAP_PLANT = PL.PLANT
				AND PL.STO_LOC = ''
		;

		
		INSERT INTO #tmp_trans
		SELECT
			ISNULL(LOC.LOC_WID, 0)                AS LOC_WID
			, ISNULL(WO.ASSET_WID, 0)               AS ASSET_WID
			, ISNULL(INVU_WID, 0)                   AS INVU_WID
			, ISNULL(INVUL_WID, 0)                  AS INVUL_WID
			, ISNULL(P.PRODUCT_WID, 0)              AS PRODUCT_WID
			, ISNULL(WO.WO_WID, 0)                  AS WO_WID
			, FORMAT(
				CONVERT(DATE, MATR.ACTUALDATE), 
				'yyyyMMdd'
			)                                       AS DATE_WID
			, ISNULL(PL.PLANT_WID, 0)				AS PLANT_WID

			, CASE WHEN MATR.ISSUETYPE = 'TRANSFER'
				THEN INVU.INVUSE_NUM       
				ELSE MATR.SPVB_SAPRECEIPT
			END                                     AS USAGE
			, CASE WHEN MATR.FROMSTORELOC = 'INSPECTION'
					THEN MATR.TOSTORELOC
				WHEN MATR.TOSTORELOC = 'INSPECTION'
					THEN MATR.FROMSTORELOC
				WHEN MATR.FROMSTORELOC = MATR.TOSTORELOC
					THEN MATR.FROMSTORELOC
				ELSE 'From ' + MATR.FROMSTORELOC + ' To ' + MATR.TOSTORELOC
			END                                     AS WAREHOUSE
			, LOC.[DESCRIPTION]                     AS WAREHOUSE_NAME
			, CASE WHEN MATR.ISSUETYPE = 'TRANSFER' 
					AND MATR.FROMSTORELOC = 'INSPECTION'
					AND MATR.SHIPMENTNUM IS NOT NULL
				THEN 'SHIPRECEIPT' 
				WHEN MATR.ISSUETYPE = 'TRANSFER'
					AND MATR.FROMSTORELOC = 'INSPECTION'
					AND MATR.SHIPMENTNUM IS NULL
				THEN 'RECEIPT'
				ELSE MATR.ISSUETYPE
			END                                     AS TRANSACTION_TYPE
			, MATR.TRANSDATE                        AS TRANSACTION_DATE
			, MATR.ACTUALDATE                       AS ACTUAL_DATE
			, MATR.ITEMNUM                          AS ITEM_NO
			, P.PRODUCT_NAME                        AS [DESCRIPTION]
			, MATR.QUANTITY                         AS TRANSACTION_QUANT
			, MATR.ISSUEUNIT                        AS TRANSACTION_UOM
			, MATR.BINNUM                           AS BINNUM
			, [WO].OVERHAUL                   		AS OVERHAUL
			, CASE WHEN INVUL.SPVB_MUSTRETURN_ORG = 'True'
				THEN 'Y'
				ELSE 'N'
			END                                     AS MUST_RETURN_ORIGINAL
			, NULL                                  AS MUST_RETURN_USER_INPUT
			, INVUL.SPVB_REASON                     AS MUST_RETURN_REMARK
			, MATR.UNITCOST                         AS PRICE
			, MATR.LINECOST                         AS AMOUNT
			, MATR.MRNUM                            AS MRNUM
			, CASE WHEN WO.PARENT IS NULL 
				THEN WO.WORK_ORDERS
				ELSE NULL 
			END                                     AS WORK_ORDER
			, MATR.ASSETNUM                         AS ASSET
			, WO.LINE_ASSET_NUM               		AS [LINE]
			, [WO].[TYPE]                     		AS WORK_TYPE
			, [WO].[STATUS]                   		AS WORKORDER_STATUS
			, [WO].ACTUAL_FINISH              		AS ACTUAL_FINISH
			, [WO].LAST_STATUS_DATE           		AS WO_LAST_STATUSDATE
			, [WO].SUPERVISOR                 		AS WO_DONE_BY
			, INVUL.ENTER_BY                        AS USER_ID
			, INVUL.SPVB_EXTREASONCODE              AS REASON_CODE
			, INVU.DESCRIPTION                      AS JOURNAL_CMT_HEADER
			, MATR.SPVB_SAPREMARK                   AS JOURNAL_CMT
			, CONVERT(VARCHAR, MATR.SPVB_SAPPO)     AS PONUM
			, MATR.SPVB_DND                         AS SAP_DND
			, INVUL.SPVB_WONUMREF                   AS RET_WONUM
			, SITEID								AS SITE_ID
			, 'MATR'								AS FROM_TABLE
			, MATR.FROMSTORELOC 					AS FROM_STORE_LOC
			, MATR.TOSTORELOC 						AS TO_STORE_LOC

			, CONVERT(
				NVARCHAR(100), 
				CONCAT('NULL', '~', MATR_ID, '~',
						'NULL', '~', 'NULL')
			)										AS W_INTEGRATION_ID
			, 'N'                                   AS W_DELETE_FLG
			, 'N' 									AS W_UPDATE_FLG
			, 8                                     AS W_DATASOURCE_NUM_ID
			, DATEADD(HH, 7, GETDATE())             AS W_UPDATE_DT
			, DATEADD(HH, 7, GETDATE())             AS W_INSERT_DT
			, @p_batch_id                           AS W_BATCH_ID

		FROM FND.W_CMMS_MATR_F MATR
			LEFT JOIN [dbo].[W_CMMS_INVU_D] INVU ON 1=1
				AND INVU.INVU_ID = MATR.INVUSE_ID
				AND INVU.[FROM] = 'MATR'
			LEFT JOIN [dbo].[W_CMMS_INVUL_D] INVUL ON 1=1
				AND MATR.INVUSELINE_ID IS NOT NULL
				AND MATR.INVUSELINE_ID <> ''
				AND INVUL.INVUSELINE_ID = MATR.INVUSELINE_ID
				AND INVUL.[FROM] = 'MATR'
			LEFT JOIN [dbo].[W_PRODUCT_D] P ON 1=1
				AND MATR.ITEMNUM = P.PRODUCT_CODE
				AND P.W_DATASOURCE_NUM_ID = 1
			LEFT JOIN #TMP_WO_X [WO] ON 1=1
				AND WO.WORK_ORDERS = MATR.REFWO
				AND WO.SITE = MATR.SITEID
			LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
				AND MATR.TOSTORELOC <> 'INSPECTION'
				AND LOC.[LOCATION] = MATR.TOSTORELOC
			OUTER APPLY (
				SELECT TOP 1 
					M.SAP_PLANT
				FROM [FND].[W_EXCEL_SPP_MAPPING_SLOC_SAP_CMMS_D] M
				WHERE 1=1
					AND MATR.SITEID = M.CMMS_PLANT
			) MAP
			LEFT JOIN dbo.W_SAP_PLANT_EXTENDED_D PL ON 1=1
				AND MAP.SAP_PLANT = PL.PLANT
				AND PL.STO_LOC = ''
		;


		
		INSERT INTO #tmp_trans
		SELECT
			ISNULL(LOC.LOC_WID, 0)                  AS LOC_WID
			, NULL                                  AS ASSET_WID
			, NULL                                  AS INVU_WID
			, NULL                                  AS INVUL_WID
			, ISNULL(P.PRODUCT_WID, 0)              AS PRODUCT_WID
			, NULL                                  AS WO_WID
			, FORMAT(
				CONVERT(DATE, INVT.TRANSDATE), 
				'yyyyMMdd'
			)                                       AS DATE_WID
			, ISNULL(PL.PLANT_WID, 0)				AS PLANT_WID

			, INVT.EXTERNAL_REFID                   AS USAGE
			, INVT.STORELOC                         AS WAREHOUSE
			, LOC.[DESCRIPTION]                     AS WAREHOUSE_NAME
			, 'Physical Count'                      AS TRANSACTION_TYPE
			, INVT.TRANSDATE                        AS TRANSACTION_DATE
			, NULL                       			AS ACTUAL_DATE
			, INVT.ITEM_NUM                         AS ITEM_NO
			, P.PRODUCT_NAME                        AS [DESCRIPTION]
			, INVT.QUANTITY                         AS TRANSACTION_QUANT
			, NULL                                  AS TRANSACTION_UOM
			, INVT.BIN_NUM                          AS BINNUM
			, NULL                                  AS OVERHAUL
			, NULL                                  AS MUST_RETURN_ORIGINAL
			, NULL                                  AS MUST_RETURN_USER_INPUT
			, NULL                                  AS MUST_RETURN_REMARK
			, 0                                     AS PRICE
			, 0                                     AS AMOUNT
			, NULL                                  AS MRNUM
			, NULL                                  AS WORK_ORDER
			, NULL                                  AS ASSET
			, NULL                                  AS [LINE]
			, NULL                                  AS WORK_TYPE
			, NULL                                  AS WORKORDER_STATUS
			, CONVERT(DATETIMEOFFSET, NULL)         AS ACTUAL_FINISH
			, CONVERT(DATETIMEOFFSET, NULL)         AS WO_LAST_STATUSDATE
			, NULL                                  AS WO_DONE_BY
			, NULL                                  AS USER_ID
			, NULL                                  AS REASON_CODE
			, NULL                                  AS JOURNAL_CMT_HEADER
			, NULL                                  AS JOURNAL_CMT
			, NULL                                  AS PONUM
			, NULL                                  AS SAP_DND
			, NULL                                  AS RET_WONUM
			, SITE_ID								AS SITE_ID
			, 'INVT'								AS FROM_TABLE
			, CONVERT(VARCHAR, NULL)				AS FROM_STORE_LOC
			, CONVERT(VARCHAR, NULL)				AS TO_STORE_LOC

			, CONVERT(
				NVARCHAR(100), 
				CONCAT('NULL', '~', 'NULL', '~',
						INVTRANS_ID, '~', 'NULL')
			)										AS W_INTEGRATION_ID
			, 'N'                                   AS W_DELETE_FLG
			, 'N' 									AS W_UPDATE_FLG
			, 8                                     AS W_DATASOURCE_NUM_ID
			, DATEADD(HH, 7, GETDATE())             AS W_UPDATE_DT
			, DATEADD(HH, 7, GETDATE())             AS W_INSERT_DT
			, @p_batch_id                           AS W_BATCH_ID
		FROM [FND].[W_CMMS_INVT_F] INVT
			LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
				AND LOC.[LOCATION] = INVT.STORELOC
			LEFT JOIN [dbo].[W_PRODUCT_D] P ON 1=1
				AND INVT.ITEM_NUM = P.PRODUCT_CODE
				AND P.W_DATASOURCE_NUM_ID = 1
			OUTER APPLY (
				SELECT TOP 1 
					M.SAP_PLANT
				FROM [FND].[W_EXCEL_SPP_MAPPING_SLOC_SAP_CMMS_D] M
				WHERE 1=1
					AND INVT.SITE_ID = M.CMMS_PLANT
			) MAP
			LEFT JOIN dbo.W_SAP_PLANT_EXTENDED_D PL ON 1=1
				AND MAP.SAP_PLANT = PL.PLANT
				AND PL.STO_LOC = ''
		;


		INSERT INTO #tmp_trans
		SELECT
			NULL                                    AS LOC_WID
			, NULL                                  AS ASSET_WID
			, NULL                                  AS INVU_WID
			, NULL                                  AS INVUL_WID
			, NULL                                  AS PRODUCT_WID
			, NULL                                  AS WO_WID
			, FORMAT(
				CONVERT(DATE, SERV.ACTUALDATE), 
				'yyyyMMdd'
			)                                       AS DATE_WID
			, ISNULL(PL.PLANT_WID, 0)				AS PLANT_WID

			, SERV.SPVB_SAPRECEIPT                  AS USAGE
			, NULL                                  AS WAREHOUSE
			, NULL                                  AS WAREHOUSE_NAME
			, CASE WHEN SERV.ISSUE_TYPE = 'RETURN'
				THEN 'SERVICE RETURN'
				ELSE 'SERVICE RECEIPT'
			END                                     AS TRANSACTION_TYPE
			, SERV.TRANSDATE                        AS TRANSACTION_DATE
			, SERV.ACTUALDATE                       AS ACTUAL_DATE
			, SERV.ITEM_NUM                         AS ITEM_NO
			, SERV.[DESCRIPTION]                    AS [DESCRIPTION]
			, SERV.QUANTITY                         AS TRANSACTION_QUANT
			, NULL                                  AS TRANSACTION_UOM
			, NULL                                  AS BINNUM
			, CASE WHEN WO.OVERHAUL = 'Y'
				THEN 'Y'
				ELSE 'N'
			END                                     AS OVERHAUL
			, NULL                                  AS MUST_RETURN_ORIGINAL
			, NULL                                  AS MUST_RETURN_USER_INPUT
			, NULL                                  AS MUST_RETURN_REMARK
			, SERV.UNITCOST                         AS PRICE
			, SERV.LINECOST                         AS AMOUNT
			, NULL                                  AS MRNUM
			, NULL                                  AS WORK_ORDER
			, SERV.ASSET_NUM                        AS ASSET
			, [WO].LINE_ASSET_NUM             		AS [LINE]
			, WO.[TYPE]                       		AS WORK_TYPE
			, WO.[STATUS]                     		AS WORKORDER_STATUS
			, WO.ACTUAL_FINISH                		AS ACTUAL_FINISH
			, WO.LAST_STATUS_DATE             		AS WO_LAST_STATUSDATE
			, WO.SUPERVISOR                   		AS WO_DONE_BY
			, NULL                                  AS USER_ID
			, NULL                                  AS REASON_CODE
			, NULL                                  AS JOURNAL_CMT_HEADER
			, NULL                                  AS JOURNAL_CMT
			, SERV.SPVB_SAPPO                       AS PONUM
			, NULL                                  AS SAP_DND
			, NULL                                  AS RET_WONUM
			, SITE_ID								AS SITE_ID
			, 'SERV'								AS FROM_TABLE
			, CONVERT(VARCHAR, NULL)				AS FROM_STORE_LOC
			, CONVERT(VARCHAR, NULL)				AS TO_STORE_LOC

			, CONVERT(
				NVARCHAR(100), 
				CONCAT('NULL', '~', 'NULL', '~',
						'NULL' , '~', SERV_ID)
			)										AS W_INTEGRATION_ID
			, 'N'                                   AS W_DELETE_FLG
			, 'N' 									AS W_UPDATE_FLG
			, 8                                     AS W_DATASOURCE_NUM_ID
			, DATEADD(HH, 7, GETDATE())             AS W_UPDATE_DT
			, DATEADD(HH, 7, GETDATE())             AS W_INSERT_DT
			, @p_batch_id                           AS W_BATCH_ID
		FROM [FND].[W_CMMS_SERV_F] SERV
			LEFT JOIN #TMP_WO_X [WO] ON 1=1
				AND WO.WORK_ORDERS = SERV.REFWO
				AND WO.SITE = SERV.SITE_ID
			OUTER APPLY (
				SELECT TOP 1 
					M.SAP_PLANT
				FROM [FND].[W_EXCEL_SPP_MAPPING_SLOC_SAP_CMMS_D] M
				WHERE 1=1
					AND SERV.SITE_ID = M.CMMS_PLANT
			) MAP
			LEFT JOIN dbo.W_SAP_PLANT_EXTENDED_D PL ON 1=1
				AND MAP.SAP_PLANT = PL.PLANT
				AND PL.STO_LOC = ''
		;



		-- 3. Update main table using W_INTEGRATION_ID
		PRINT '3. Update main table using W_INTEGRATION_ID'

		-- 3.1. Mark existing records by flag 'Y'
		PRINT '3.1. Mark existing records by flag ''Y'''

		UPDATE #tmp_trans
		SET W_UPDATE_FLG = 'Y'
		FROM #tmp_trans tg
		INNER JOIN [dbo].[W_CMMS_TRANSACTION_F] sc 
		ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

		-- 3.2. Start updating
		PRINT '3.2. Start updating'

		UPDATE  [dbo].[W_CMMS_TRANSACTION_F]
		SET 
			LOC_WID = src.LOC_WID
			, ASSET_WID = src.ASSET_WID
			, INVU_WID = src.INVU_WID
			, INVUL_WID = src.INVUL_WID
			, PRODUCT_WID = src.PRODUCT_WID
			, WO_WID = src.WO_WID
			, DATE_WID = src.DATE_WID
			, PLANT_WID = src.PLANT_WID

			, USAGE = src.USAGE
			, WAREHOUSE = src.WAREHOUSE
			, WAREHOUSE_NAME = src.WAREHOUSE_NAME
			, TRANSACTION_TYPE = src.TRANSACTION_TYPE
			, TRANSACTION_DATE = src.TRANSACTION_DATE
			, ACTUAL_DATE = src.ACTUAL_DATE
			, ITEM_NO = src.ITEM_NO
			, [DESCRIPTION] = src.DESCRIPTION
			, TRANSACTION_QUANT = src.TRANSACTION_QUANT
			, TRANSACTION_UOM = src.TRANSACTION_UOM
			, BINNUM = src.BINNUM
			, OVERHAUL = src.OVERHAUL
			, MUST_RETURN_ORIGINAL = src.MUST_RETURN_ORIGINAL
			, MUST_RETURN_USER_INPUT = src.MUST_RETURN_USER_INPUT
			, MUST_RETURN_REMARK = src.MUST_RETURN_REMARK
			, PRICE = src.PRICE
			, AMOUNT = src.AMOUNT
			, MRNUM = src.MRNUM
			, WORK_ORDER = src.WORK_ORDER
			, ASSET = src.ASSET
			, [LINE] = src.LINE
			, WORK_TYPE = src.WORK_TYPE
			, WORKORDER_STATUS = src.WORKORDER_STATUS
			, ACTUAL_FINISH = src.ACTUAL_FINISH
			, WO_LAST_STATUSDATE = src.WO_LAST_STATUSDATE
			, WO_DONE_BY = src.WO_DONE_BY
			, USER_ID = src.USER_ID
			, REASON_CODE = src.REASON_CODE
			, JOURNAL_CMT_HEADER = src.JOURNAL_CMT_HEADER
			, PONUM = src.PONUM
			, SAP_DND = src.SAP_DND
			, RET_WONUM = src.RET_WONUM
			, SITE_ID = src.SITE_ID
			, FROM_TABLE = src.FROM_TABLE
			, FROM_STORE_LOC = src.FROM_STORE_LOC
			, TO_STORE_LOC = src.TO_STORE_LOC
			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = DATEADD(HH, 7, GETDATE())
		FROM [dbo].[W_CMMS_TRANSACTION_F] tgt
		INNER JOIN #tmp_trans src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID

--alter table [dbo].[W_CMMS_TRANSACTION_F]
--add FROM_STORE_LOC varchar(20), TO_STORE_LOC varchar(20)
		-- 4. Insert non-existed records to main table from temp table
		PRINT '4. Insert non-existed records to main table from temp table'

		INSERT INTO [dbo].[W_CMMS_TRANSACTION_F](
			LOC_WID
			, ASSET_WID
			, INVU_WID
			, INVUL_WID
			, PRODUCT_WID
			, WO_WID
			, DATE_WID
			, PLANT_WID

			, USAGE
			, WAREHOUSE
			, WAREHOUSE_NAME
			, TRANSACTION_TYPE
			, TRANSACTION_DATE
			, ACTUAL_DATE
			, ITEM_NO
			, [DESCRIPTION]
			, TRANSACTION_QUANT
			, TRANSACTION_UOM
			, BINNUM
			, OVERHAUL
			, MUST_RETURN_ORIGINAL
			, MUST_RETURN_USER_INPUT
			, MUST_RETURN_REMARK
			, PRICE
			, AMOUNT
			, MRNUM
			, WORK_ORDER
			, ASSET
			, [LINE]
			, WORK_TYPE
			, WORKORDER_STATUS
			, ACTUAL_FINISH
			, WO_LAST_STATUSDATE
			, WO_DONE_BY
			, USER_ID
			, REASON_CODE
			, JOURNAL_CMT_HEADER
			, PONUM
			, SAP_DND
			, RET_WONUM
			, SITE_ID
			, FROM_TABLE
			, FROM_STORE_LOC
			, TO_STORE_LOC
			, W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
		)
		SELECT
			LOC_WID
			, ASSET_WID
			, INVU_WID
			, INVUL_WID
			, PRODUCT_WID
			, WO_WID
			, DATE_WID
			, PLANT_WID

			, USAGE
			, WAREHOUSE
			, WAREHOUSE_NAME
			, TRANSACTION_TYPE
			, TRANSACTION_DATE
			, ACTUAL_DATE
			, ITEM_NO
			, [DESCRIPTION]
			, TRANSACTION_QUANT
			, TRANSACTION_UOM
			, BINNUM
			, OVERHAUL
			, MUST_RETURN_ORIGINAL
			, MUST_RETURN_USER_INPUT
			, MUST_RETURN_REMARK
			, PRICE
			, AMOUNT
			, MRNUM
			, WORK_ORDER
			, ASSET
			, [LINE]
			, WORK_TYPE
			, WORKORDER_STATUS
			, ACTUAL_FINISH
			, WO_LAST_STATUSDATE
			, WO_DONE_BY
			, USER_ID
			, REASON_CODE
			, JOURNAL_CMT_HEADER
			, PONUM
			, SAP_DND
			, RET_WONUM
			, SITE_ID
			, FROM_TABLE
			, FROM_STORE_LOC
			, TO_STORE_LOC
			, W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
		FROM #tmp_trans
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
            FROM W_CMMS_TRANSACTION_F
        ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

		SET @src_rownum = ( SELECT COUNT(1) FROM #tmp_trans );
		SET @tgt_rownum = ( 
            SELECT 
                COUNT(DISTINCT W_INTEGRATION_ID)
            FROM W_CMMS_TRANSACTION_F
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
