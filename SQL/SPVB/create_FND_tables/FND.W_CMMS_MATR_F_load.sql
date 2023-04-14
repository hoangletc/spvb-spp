SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[SAP_proc_load_w_cmms_spp_fnd_matr_d]
    @p_batch_id [bigint]
AS
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'FND.W_CMMS_MATR_F',
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
		-- 1. Check existence of (and remove) temp table
        PRINT '1. Check existence and remove of temp table'

        IF OBJECT_ID(N'tempdb..#W_CMMS_MATR_F_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_CMMS_MATR_F_tmp'
            DROP Table #W_CMMS_MATR_F_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

        SELECT
              CONVERT(BIGINT, MATR_ID)                          AS MATR_ID
            , CONVERT(BIGINT, INVUSE_ID)                        AS INVUSE_ID
            , CONVERT(BIGINT, INVUSELINE_ID)                    AS INVUSELINE_ID

            , CASE WHEN ISNUMERIC([ACTUALCOST]) = 0 THEN 0.
                ELSE CONVERT(REAL, [ACTUALCOST]) 
            END                                                 AS [ACTUALCOST] 
            , CONVERT(DATETIME2, [ACTUALDATE], 103)             AS [ACTUALDATE]
            , CONVERT(NVARCHAR(100), [ASSETNUM])                AS [ASSETNUM]
            , CONVERT(NVARCHAR(100), [BINNUM])                  AS [BINNUM]
            , CONVERT(REAL, [CURBAL])                AS [CURBAL] 
            , CONVERT(NVARCHAR(100), [CURRENCYCODE])            AS [CURRENCYCODE]
            , CASE WHEN ISNUMERIC(CURRENCYLINECOST) = 0 THEN 0.
                ELSE CONVERT(REAL, CURRENCYLINECOST) 
            END                                                 AS CURRENCYLINECOST 
            , CONVERT(NVARCHAR(1000), [DESCRIPTION])            AS [DESCRIPTION]
            , CASE WHEN ISNUMERIC(EXCHANGERATE) = 0 THEN 0.
                ELSE CONVERT(REAL, EXCHANGERATE) 
            END                                                 AS EXCHANGERATE 
            , CONVERT(NVARCHAR(100), [FINANCIALPERIOD])         AS [FINANCIALPERIOD]
            , CONVERT(NVARCHAR(100), [FROMSITEID])              AS [FROMSITEID]
            , CONVERT(NVARCHAR(100), [FROMSTORELOC])            AS [FROMSTORELOC]
            , CONVERT(NVARCHAR(100), [ISSUE])                   AS [ISSUE]
            , CONVERT(NVARCHAR(100), [ISSUETYPE])               AS [ISSUETYPE]
            , CONVERT(NVARCHAR(100), [ISSUEUNIT])               AS [ISSUEUNIT]
            , CONVERT(NVARCHAR(100), [ITEMNUM])                 AS [ITEMNUM]
            , CONVERT(NVARCHAR(100), [LINECOST])                AS [LINECOST]
            , CONVERT(NVARCHAR(100), [LINETYPE])                AS [LINETYPE]
            , CONVERT(NVARCHAR(1000), [LINETYPE_DESCRIPTION])   AS [LINETYPE_DESCRIPTION]
            , CONVERT(NVARCHAR(100), [MRNUM])                   AS [MRNUM]
            , CONVERT(NVARCHAR(100), [POLINENUM])               AS [POLINENUM]
            , CONVERT(NVARCHAR(100), [PONUM])                   AS [PONUM]
            , CONVERT(NVARCHAR(100), [POSITEID])                AS [POSITEID]
            , CONVERT(NVARCHAR(100), [QTYOVERRECEIVED])         AS [QTYOVERRECEIVED]
            , CASE WHEN ISNUMERIC([QUANTITY]) = 0 THEN 0.
                ELSE CONVERT(REAL, [QUANTITY]) 
            END                                                 AS [QUANTITY]
            , CONVERT(NVARCHAR(100), [RECEIVEDUNIT])            AS [RECEIVEDUNIT]
            , CONVERT(NVARCHAR(100), [REFWO])                   AS [REFWO]
            , CONVERT(NVARCHAR(100), [SHIPMENTNUM])             AS [SHIPMENTNUM]
            , CONVERT(NVARCHAR(100), [SITEID])                  AS [SITEID]
            , CONVERT(NVARCHAR(100), [SPVB_DND])                AS [SPVB_DND]
            , CONVERT(NVARCHAR(100), [SPVB_SAPPO])              AS [SPVB_SAPPO]
            , CONVERT(NVARCHAR(100), [SPVB_SAPRECEIPT])         AS [SPVB_SAPRECEIPT]
            , CONVERT(NVARCHAR(100), [SPVB_SAPREMARK])          AS [SPVB_SAPREMARK]
            , CONVERT(NVARCHAR(100), [TOBIN])                   AS [TOBIN]
            , CONVERT(NVARCHAR(100), [TOSTORELOC])              AS [TOSTORELOC]
            , CASE WHEN ISNUMERIC([TOTALCURBAL]) = 0 THEN 0.
                ELSE CONVERT(REAL, [TOTALCURBAL]) 
            END                                                 AS [TOTALCURBAL]
            , CONVERT(DATETIME2, [TRANSDATE], 103)              AS [TRANSDATE]
            , CASE WHEN ISNUMERIC([UNITCOST]) = 0 THEN 0.
                ELSE CONVERT(REAL, [UNITCOST]) 
            END                                                 AS [UNITCOST]
            , CONVERT(NVARCHAR(100), [FILE_NAME])               AS [FILE_NAME]

            , CONVERT(NVARCHAR, MATR_ID)                        AS W_INTEGRATION_ID
            , 'N'                                               AS W_DELETE_FLG
            , 'N' 											    AS W_UPDATE_FLG
            , 8                                                 AS W_DATASOURCE_NUM_ID
            , DATEADD(HH, 7, GETDATE())                         AS W_INSERT_DT
            , DATEADD(HH, 7, GETDATE())                         AS W_UPDATE_DT
            , @p_batch_id                                       AS W_BATCH_ID
        INTO #W_CMMS_MATR_F_tmp
        FROM [STG].[W_CMMS_MATR_FS]


        -- 3. Update main table using W_INTEGRATION_ID
        PRINT '3. Update main table using W_INTEGRATION_ID'

        -- 3.1. Mark existing records by flag 'Y'
        PRINT '3.1. Mark existing records by flag ''Y'''

        UPDATE #W_CMMS_MATR_F_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_CMMS_MATR_F_tmp tg
        INNER JOIN [FND].[W_CMMS_MATR_F] sc
        ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

        -- 3.2. Start updating
        PRINT '3.2. Start updating'

		UPDATE [FND].[W_CMMS_MATR_F]
		SET
            MATR_ID = src.MATR_ID
            , INVUSE_ID = src.INVUSE_ID
            , INVUSELINE_ID = src.INVUSELINE_ID

            , [ACTUALCOST] = src.[ACTUALCOST]
            , [ACTUALDATE] = src.[ACTUALDATE]
            , [ASSETNUM] = src.[ASSETNUM]
            , [BINNUM] = src.[BINNUM]
            , [CURBAL]  = src.[CURBAL] 
            , [CURRENCYCODE] = src.[CURRENCYCODE]
            , [CURRENCYLINECOST]  = src.[CURRENCYLINECOST] 
            , [DESCRIPTION] = src.[DESCRIPTION]
            , [EXCHANGERATE] = src.[EXCHANGERATE]
            , [FINANCIALPERIOD] = src.[FINANCIALPERIOD]
            , [FROMSITEID] = src.[FROMSITEID]
            , [FROMSTORELOC] = src.[FROMSTORELOC]
            , [ISSUE] = src.[ISSUE]
            , [ISSUETYPE] = src.[ISSUETYPE]
            , [ISSUEUNIT] = src.[ISSUEUNIT]
            , [ITEMNUM] = src.[ITEMNUM]
            , [LINECOST] = src.[LINECOST]
            , [LINETYPE] = src.[LINETYPE]
            , [LINETYPE_DESCRIPTION] = src.[LINETYPE_DESCRIPTION]
            , [MRNUM] = src.[MRNUM]
            , [POLINENUM] = src.[POLINENUM]
            , [PONUM] = src.[PONUM]
            , [POSITEID] = src.[POSITEID]
            , [QTYOVERRECEIVED] = src.[QTYOVERRECEIVED]
            , [QUANTITY] = src.[QUANTITY]
            , [RECEIVEDUNIT] = src.[RECEIVEDUNIT]
            , [REFWO] = src.[REFWO]
            , [SHIPMENTNUM] = src.[SHIPMENTNUM]
            , [SITEID] = src.[SITEID]
            , [SPVB_DND] = src.[SPVB_DND]
            , [SPVB_SAPPO] = src.[SPVB_SAPPO]
            , [SPVB_SAPRECEIPT] = src.[SPVB_SAPRECEIPT]
            , [SPVB_SAPREMARK] = src.[SPVB_SAPREMARK]
            , [TOBIN] = src.[TOBIN]
            , [TOSTORELOC] = src.[TOSTORELOC]
            , [TOTALCURBAL]  = src.[TOTALCURBAL] 
            , [TRANSDATE] = src.[TRANSDATE]
            , [UNITCOST]  = src.[UNITCOST] 
            , [FILE_NAME] = src.[FILE_NAME]

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = DATEADD(HH, 7, GETDATE())
        FROM [FND].[W_CMMS_MATR_F] tgt
        INNER JOIN #W_CMMS_MATR_F_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


	    -- 4. Insert non-existed records to main table from temp table
        PRINT '4. Insert non-existed records to main table from temp table'

        INSERT INTO [FND].[W_CMMS_MATR_F](
            MATR_ID
            , INVUSE_ID
            , INVUSELINE_ID
            
            , [ACTUALCOST]
            , [ACTUALDATE]
            , [ASSETNUM]
            , [BINNUM]
            , [CURBAL] 
            , [CURRENCYCODE]
            , [CURRENCYLINECOST] 
            , [DESCRIPTION]
            , [EXCHANGERATE]
            , [FINANCIALPERIOD]
            , [FROMSITEID]
            , [FROMSTORELOC]
            , [ISSUE]
            , [ISSUETYPE]
            , [ISSUEUNIT]
            , [ITEMNUM]
            , [LINECOST]
            , [LINETYPE]
            , [LINETYPE_DESCRIPTION]
            , [MRNUM]
            , [POLINENUM]
            , [PONUM]
            , [POSITEID]
            , [QTYOVERRECEIVED]
            , [QUANTITY]
            , [RECEIVEDUNIT]
            , [REFWO]
            , [SHIPMENTNUM]
            , [SITEID]
            , [SPVB_DND]
            , [SPVB_SAPPO]
            , [SPVB_SAPRECEIPT]
            , [SPVB_SAPREMARK]
            , [TOBIN]
            , [TOSTORELOC]
            , [TOTALCURBAL] 
            , [TRANSDATE]
            , [UNITCOST] 
            , [FILE_NAME]

            , W_DELETE_FLG
            , W_DATASOURCE_NUM_ID
            , W_INSERT_DT
            , W_UPDATE_DT
            , W_BATCH_ID
            , W_INTEGRATION_ID
       )
        SELECT
            MATR_ID
            , INVUSE_ID
            , INVUSE_ID
            
            , [ACTUALCOST]
            , [ACTUALDATE]
            , [ASSETNUM]
            , [BINNUM]
            , [CURBAL] 
            , [CURRENCYCODE]
            , [CURRENCYLINECOST] 
            , [DESCRIPTION]
            , [EXCHANGERATE]
            , [FINANCIALPERIOD]
            , [FROMSITEID]
            , [FROMSTORELOC]
            , [ISSUE]
            , [ISSUETYPE]
            , [ISSUEUNIT]
            , [ITEMNUM]
            , [LINECOST]
            , [LINETYPE]
            , [LINETYPE_DESCRIPTION]
            , [MRNUM]
            , [POLINENUM]
            , [PONUM]
            , [POSITEID]
            , [QTYOVERRECEIVED]
            , [QUANTITY]
            , [RECEIVEDUNIT]
            , [REFWO]
            , [SHIPMENTNUM]
            , [SITEID]
            , [SPVB_DND]
            , [SPVB_SAPPO]
            , [SPVB_SAPRECEIPT]
            , [SPVB_SAPREMARK]
            , [TOBIN]
            , [TOSTORELOC]
            , [TOTALCURBAL] 
            , [TRANSDATE]
            , [UNITCOST] 
            , [FILE_NAME]

            , W_DELETE_FLG
            , W_DATASOURCE_NUM_ID
            , W_INSERT_DT
            , W_UPDATE_DT
            , W_BATCH_ID
            , W_INTEGRATION_ID
        FROM #W_CMMS_MATR_F_tmp
        where W_UPDATE_FLG = 'N'		

		/*delete & re-insert data refresh*/
		DELETE FROM [dbo].[SAP_ETL_DATAFRESH_CONF] WHERE UPPER(TABLE_NAME) = UPPER(@tgt_TableName)

		INSERT INTO [dbo].[SAP_ETL_DATAFRESH_CONF](
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
            FROM FND.W_CMMS_MATR_F
           ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

            SET @src_rownum = ( SELECT COUNT(1)
        FROM #W_CMMS_MATR_F_tmp);
            SET @tgt_rownum = ( 
                SELECT
            COUNT(DISTINCT W_INTEGRATION_ID)
        FROM W_CMMS_MATR_F
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