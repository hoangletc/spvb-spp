SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (OBJECT_ID('[dbo].[proc_load_w_cmms_transaction_f]') is not null)
BEGIN
    DROP PROCEDURE [dbo].[proc_load_w_cmms_transaction_f]
END;
GO

CREATE PROC [dbo].[proc_load_w_cmms_transaction_f]
    @p_batch_id [bigint]
AS 
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

        IF OBJECT_ID(N'tempdb..#W_CMMS_TRANSACTION_F_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_CMMS_TRANSACTION_F_tmp'
            DROP Table #W_CMMS_TRANSACTION_F_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

		;WITH
		TMP_WO_X AS (
			SELECT
				WO.*
				, CASE WHEN WO.IS_SCHED = 'False'
					THEN WO.DATE_FINISHED_BFR
					ELSE WO.DATE_FINISHED_AFT
				END                                 	AS ACTUAL_FINISH
				, WO_STA.CHANGEDATE               		AS LAST_STATUS_DATE
			FROM [dbo].[W_CMMS_WO_F] AS WO
				LEFT JOIN [FND].[W_CMMS_WO_STATUS_D] WO_STA ON 1=1
					AND WO_STA.PARENT = WO.WORK_ORDERS
					AND WO_STA.STATUS = WO.[STATUS]
		)
			SELECT 
				TRANS.*

				, CONVERT(
					NVARCHAR(100), 
					CONCAT_WS('~', ASSET, ITEM_NO, PONUM, MRNUM, BINNUM)
				)                                           AS W_INTEGRATION_ID
				, 'N'                                       AS W_DELETE_FLG
				, 'N' 										AS W_UPDATE_FLG
				, 1                                         AS W_DATASOURCE_NUM_ID
				, GETDATE()                                 AS W_INSERT_DT
				, GETDATE()                                 AS W_UPDATE_DT
				, NULL                                      AS W_BATCH_ID
			INTO #W_CMMS_TRANSACTION_F_tmp
			FROM (
				SELECT
					LOC.LOC_WID                             AS LOC_WID
					, AST.ASSET_WID                         AS ASSET_WID
					, INVU.INVU_WID                         AS INVU_WID
					, INVUL.INVUL_WID                       AS INVUL_WID
					, IT.ITEM_WID                           AS ITEM_WID
					, TMP_WO_X.WO_WID                       AS WO_WID
					, FORMAT(
						CONVERT(DATE, MATU.ACTUALDATE), 
						'yyyymmdd'
					)                                       AS DATE_WID

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
					, IT.DESCRIPTION                        AS [DESCRIPTION]
					, MATU.QUANTITY                         AS TRANSACTION_QUANT
					, MATU.ISSUE_UNIT                       AS TRANSACTION_UOM
					, MATU.BINNUM                           AS BINNUM
					, TMP_WO_X.OVERHAUL                     AS OVERHAUL
					, CASE WHEN INVUL.SPVB_MUSTRETURN_ORG = 'True'
						THEN 'Y'
						ELSE 'N'
					END                                     AS MUST_RETURN_ORIGINAL
					, CASE WHEN INVUL.SPVB_MUSTRETURN = 'True'
						THEN 'Y'
						ELSE 'N'
					END                                     AS MUST_RETURN_USER_INPUT
					, INVUL.SPVB_REASON                     AS MUST_RETURN_REMARK
					, MATU.UNITCOST                         AS PRICE
					, MATU.LINECOST                         AS AMOUNT
					, MATU.MRNUM                            AS MRNUM
					, CASE WHEN TMP_WO_X.PARENT IS NULL 
						THEN TMP_WO_X.WORK_ORDERS
						ELSE NULL 
					END                                     AS WORK_ORDER
					, MATU.ASSET_NUM                        AS ASSET
					, AST.LINE_ASSET_NUM                    AS [LINE]
					, TMP_WO_X.[TYPE]                       AS WORK_TYPE
					, TMP_WO_X.[STATUS]                     AS WORKORDER_STATUS
					, TMP_WO_X.ACTUAL_FINISH                AS ACTUAL_FINISH
					, TMP_WO_X.LAST_STATUS_DATE             AS WO_LAST_STATUSDATE
					, TMP_WO_X.SUPERVISOR                   AS WO_DONE_BY
					, INVUL.ENTER_BY                        AS USER_ID
					, INVUL.SPVB_EXTREASONCODE              AS REASON_CODE
					, INVU.DESCRIPTION                      AS JOURNAL_CMT_HEADER
					, INVUL.REMARK                          AS JOURNAL_CMT
					, MATU.PONUM                            AS PONUM
					, NULL                                  AS SAP_DND
					, INVUL.SPVB_WONUMREF                   AS RET_WONUM

				FROM FND.W_CMMS_MATU_F MATU
					LEFT JOIN [dbo].[W_CMMS_INVU_D] INVU ON 1=1
						AND INVU.MATU_ID = MATU.MATU_ID
						AND INVU.ASSET_NUM = MATU.ASSET_NUM
						AND INVU.ITEM_NUM = MATU.ITEM_NUM
					LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
						AND LOC.[LOCATION] = MATU.STORELOC
					LEFT JOIN [dbo].[W_CMMS_ITEM_D] IT ON 1=1
						AND IT.ITEM_NUM = MATU.ITEM_NUM
					LEFT JOIN [dbo].[W_CMMS_INVUL_D] INVUL ON 1=1
						AND INVU.MATU_ID = MATU.MATU_ID
						AND INVU.ASSET_NUM = MATU.ASSET_NUM
						AND INVU.ITEM_NUM = MATU.ITEM_NUM
					LEFT JOIN [dbo].[W_CMMS_ASSET_D] AST ON 1=1
						AND AST.ASSET_NUM = MATU.ASSET_NUM
					LEFT JOIN [TMP_WO_X] ON 1=1
						AND TMP_WO_X.WORK_ORDERS = MATU.REFWO

				UNION ALL

				
				SELECT
					LOC.LOC_WID                             AS LOC_WID
					, AST.ASSET_WID                         AS ASSET_WID
					, INVU_WID                              AS INVU_WID
					, INVUL_WID                             AS INVUL_WID
					, ITEM_WID                              AS ITEM_WID
					, TMP_WO_X.WO_WID                       AS WO_WID
					, FORMAT(
						CONVERT(DATE, MATR.ACTUALDATE), 
						'yyyymmdd'
					)                                       AS DATE_WID

					, CASE WHEN MATR.ISSUE_TYPE = 'TRANSFER'
						THEN INVU.INVUSE_NUM       
						ELSE MATR.SPVB_SAPRECEIPT
					END                                     AS USAGE
					, CASE WHEN MATR.FROM_STORELOC = 'INSPECTION' THEN MATR.FROM_STORELOC
						WHEN MATR.TO_STORELOC = 'INSPECTION' THEN MATR.FROM_STORELOC
						WHEN MATR.FROM_STORELOC = MATR.TO_STORELOC THEN MATR.FROM_STORELOC
						ELSE 'From ' + MATR.FROM_STORELOC + ' To ' + MATR.TO_STORELOC
					END                                     AS WAREHOUSE
					, LOC.[DESCRIPTION]                     AS WAREHOUSE_NAME
					, CASE WHEN MATR.ISSUE_TYPE = 'TRANSFER' 
							AND MATR.FROM_STORELOC = 'INSPECTION'
							AND MATR.SHIPMENT_NUM IS NOT NULL
						THEN 'SHIPRECEIPT' 
						WHEN MATR.ISSUE_TYPE = 'TRANSFER'
							AND MATR.FROM_STORELOC = 'INSPECTION'
							AND MATR.SHIPMENT_NUM IS NULL
						THEN 'RECEIPT'
						ELSE MATR.ISSUE_TYPE
					END                                     AS TRANSACTION_TYPE
					, MATR.TRANSDATE                        AS TRANSACTION_DATE
					, MATR.ACTUALDATE                       AS ACTUAL_DATE
					, MATR.ITEM_NUM                         AS ITEM_NO
					, IT.DESCRIPTION                        AS [DESCRIPTION]
					, MATR.QUANTITY                         AS TRANSACTION_QUANT
					, MATR.ISSUE_UNIT                       AS TRANSACTION_UOM
					, MATR.BINNUM                           AS BINNUM
					, [TMP_WO_X].OVERHAUL                   AS OVERHAUL
					, CASE WHEN INVUL.SPVB_MUSTRETURN_ORG = 'True'
						THEN 'Y'
						ELSE 'N'
					END                                     AS MUST_RETURN_ORIGINAL
					, NULL                                  AS MUST_RETURN_USER_INPUT
					, INVUL.SPVB_REASON                     AS MUST_RETURN_REMARK
					, MATR.UNIT_COST                        AS PRICE
					, MATR.LINECOST                         AS AMOUNT
					, MATR.MRNUM                            AS MRNUM
					, CASE WHEN TMP_WO_X.PARENT IS NULL 
						THEN TMP_WO_X.WORK_ORDERS
						ELSE NULL 
					END                                     AS WORK_ORDER
					, MATR.ASSET_NUM                        AS ASSET
					, AST.LINE_ASSET_NUM                    AS [LINE]
					, [TMP_WO_X].TYPE                       AS WORK_TYPE
					, [TMP_WO_X].[STATUS]                   AS WORKORDER_STATUS
					, [TMP_WO_X].ACTUAL_FINISH              AS ACTUAL_FINISH
					, [TMP_WO_X].LAST_STATUS_DATE           AS WO_LAST_STATUSDATE
					, [TMP_WO_X].SUPERVISOR                 AS WO_DONE_BY
					, INVUL.ENTER_BY                        AS USER_ID
					, INVUL.SPVB_EXTREASONCODE              AS REASON_CODE
					, INVU.DESCRIPTION                      AS JOURNAL_CMT_HEADER
					, MATR.SPVB_SAPREMARK                   AS JOURNAL_CMT
					, MATR.SPVB_SAPPO                       AS PONUM
					, MATR.SPVB_DND                         AS SAO_DND
					, INVUL.SPVB_WONUMREF                   AS RET_WONUM

				FROM FND.W_CMMS_MATR_F MATR
					LEFT JOIN [dbo].[W_CMMS_INVU_D] INVU ON 1=1
						AND INVU.ASSET_NUM = MATR.ASSET_NUM
						AND INVU.ITEM_NUM = MATR.ITEM_NUM
					LEFT JOIN [dbo].[W_CMMS_ITEM_D] IT ON 1=1
						AND IT.ITEM_NUM = MATR.ITEM_NUM
					LEFT JOIN [dbo].[W_CMMS_INVUL_D] INVUL ON 1=1
						AND INVUL.ASSET_NUM = MATR.ASSET_NUM
						AND INVUL.ITEM_NUM = MATR.ITEM_NUM
					LEFT JOIN [dbo].[W_CMMS_ASSET_D] AST ON 1=1
						AND AST.ASSET_NUM = MATR.ASSET_NUM
					LEFT JOIN [TMP_WO_X] ON 1=1
						AND TMP_WO_X.WORK_ORDERS = MATR.REFWO
					LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
						AND LOC.[LOCATION] = MATR.TO_STORELOC

				UNION ALL

				SELECT
					LOC.LOC_WID                             AS LOC_WID
					, NULL                                  AS ASSET_WID
					, NULL                                  AS INVU_WID
					, INVUL.INVUL_WID                       AS INVUL_WID
					, IT.ITEM_WID                           AS ITEM_WID
					, NULL                                  AS WO_WID
					, FORMAT(
						CONVERT(DATE, INVT.ACTUALDATE), 
						'yyyymmdd'
					)                                       AS DATE_WID

					, INVT.EXTERNAL_REFID                   AS USAGE
					, INVT.STORELOC                         AS WAREHOUSE
					, LOC.[DESCRIPTION]                     AS WAREHOUSE_NAME
					, 'Physical Count'                      AS TRANSACTION_TYPE
					, INVT.TRANSDATE                        AS TRANSACTION_DATE
					, INVT.ACTUALDATE                       AS ACTUAL_DATE
					, INVT.ITEM_NUM                         AS ITEM_NO
					, IT.DESCRIPTION                        AS [DESCRIPTION]
					, INVT.QUANTITY                         AS TRANSACTION_QUANT
					, NULL                                  AS TRANSACTION_UOM
					, INVT.BIN_NUM                          AS BINNUM
					, NULL                                  AS OVERHAUL
					, CASE WHEN INVUL.SPVB_MUSTRETURN_ORG = 1
						THEN 'Y'
						ELSE 'N'
					END                                     AS MUST_RETURN_ORIGINAL
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
					, NULL                                  AS ACTUAL_FINISH
					, NULL                                  AS WO_LAST_STATUSDATE
					, NULL                                  AS WO_DONE_BY
					, NULL                                  AS USER_ID
					, NULL                                  AS REASON_CODE
					, NULL                                  AS JOURNAL_CMT_HEADER
					, NULL                                  AS JOURNAL_CMT
					, NULL                                  AS PONUM
					, NULL                                  AS SAO_DND
					, NULL                                  AS RET_WONUM
				FROM [FND].[W_CMMS_INVT_F] INVT
					LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
						AND LOC.[LOCATION] = INVT.STORELOC
					LEFT JOIN [dbo].[W_CMMS_ITEM_D] IT ON 1=1
						AND IT.ITEM_NUM = INVT.ITEM_NUM
					LEFT JOIN [dbo].[W_CMMS_INVUL_D] INVUL ON 1=1
						AND INVUL.ITEM_NUM = INVT.ITEM_NUM

				UNION ALL

				SELECT
					NULL                                    AS LOC_WID
					, NULL                                  AS ASSET_WID
					, NULL                                  AS INVU_WID
					, NULL                                  AS INVUL_WID
					, NULL                                  AS ITEM_WID
					, NULL                                  AS WO_WID
					, FORMAT(
						CONVERT(DATE, SERV.ACTUALDATE), 
						'yyyymmdd'
					)                                       AS DATE_WID

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
					, CASE WHEN TMP_WO_X.OVERHAUL = 1
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
					, AST.LINE_ASSET_NUM                    AS [LINE]
					, TMP_WO_X.[TYPE]                       AS WORK_TYPE
					, TMP_WO_X.[STATUS]                     AS WORKORDER_STATUS
					, TMP_WO_X.ACTUAL_FINISH                AS ACTUAL_FINISH
					, TMP_WO_X.LAST_STATUS_DATE             AS WO_LAST_STATUSDATE
					, TMP_WO_X.SUPERVISOR                   AS WO_DONE_BY
					, NULL                                  AS USER_ID
					, NULL                                  AS REASON_CODE
					, NULL                                  AS JOURNAL_CMT_HEADER
					, NULL                                  AS JOURNAL_CMT
					, SERV.SPVB_SAPPO                       AS PONUM
					, NULL                                  AS SAO_DND
					, NULL                                  AS RET_WONUM
				FROM [FND].[W_CMMS_SERV_F] SERV
					LEFT JOIN [TMP_WO_X] ON 1=1
						AND TMP_WO_X.WORK_ORDERS = SERV.REFWO
					LEFT JOIN [dbo].[W_CMMS_ASSET_D] AST ON 1=1
						AND AST.ASSET_NUM = SERV.ASSET_NUM
		) TRANS


		-- 3. Update main table using W_INTEGRATION_ID
		PRINT '3. Update main table using W_INTEGRATION_ID'

		-- 3.1. Mark existing records by flag 'Y'
		PRINT '3.1. Mark existing records by flag ''Y'''

		UPDATE #W_CMMS_TRANSACTION_F_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_CMMS_TRANSACTION_F_tmp tg
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
			, ITEM_WID = src.ITEM_WID
			, WO_WID = src.WO_WID
			, DATE_WID = src.DATE_WID

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

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = getdate()
		FROM [dbo].[W_CMMS_TRANSACTION_F] tgt
		INNER JOIN #W_CMMS_TRANSACTION_F_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


		-- 4. Insert non-existed records to main table from temp table
		PRINT '4. Insert non-existed records to main table from temp table'

		INSERT INTO [dbo].[W_CMMS_TRANSACTION_F](
			LOC_WID
			, ASSET_WID
			, INVU_WID
			, INVUL_WID
			, ITEM_WID
			, WO_WID
			, DATE_WID

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
			, ITEM_WID
			, WO_WID
			, DATE_WID

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

			, W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
		FROM #W_CMMS_TRANSACTION_F_tmp
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

		SET @src_rownum = ( SELECT COUNT(1) FROM #W_CMMS_TRANSACTION_F_tmp );
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