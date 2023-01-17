SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (OBJECT_ID('[dbo].[proc_load_w_cmms_asset_d]') is not null)
BEGIN
    DROP PROCEDURE [dbo].[proc_load_w_cmms_asset_d]
END;
GO

CREATE PROC [dbo].[proc_load_w_cmms_asset_d]
    @p_batch_id [bigint]
AS
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_CMMS_ASSET_D',
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

        IF OBJECT_ID(N'tempdb..#W_CMMS_ASSET_D_tmp') IS NOT NULL 
        BEGIN
        PRINT N'DELETE temporary table #W_CMMS_ASSET_D_tmp'
        DROP Table #W_CMMS_ASSET_D_tmp
    END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

		SELECT
        CONVERT(nvarchar(20), ASSET_UID) AS ASSET_UID
            , CONVERT(nvarchar(50), SPVB_COSTCENTER) AS SPVB_COSTCENTER
            , CONVERT(nvarchar(50), CHANGE_DATE) AS CHANGE_DATE
            , CONVERT(nvarchar(50), SPVB_FIXEDASSETNUM) AS SPVB_FIXEDASSETNUM
            , CONVERT(nvarchar(50), TOTAL_COST) AS TOTAL_COST
            , CONVERT(nvarchar(50), STATUS) AS STATUS
            , CONVERT(nvarchar(50), STATUS_DESCRIPTION) AS STATUS_DESCRIPTION
            , CONVERT(nvarchar(50), TOTAL_DOWNTIME) AS TOTAL_DOWNTIME
            , CONVERT(nvarchar(50), ASSET_NUM) AS ASSET_NUM
            , CONVERT(nvarchar(50), ASSET_TYPE) AS ASSET_TYPE
            , CONVERT(nvarchar(50), SPVB_COSTCENTER_DESCRIPTION) AS SPVB_COSTCENTER_DESCRIPTION
            , INV_COST AS INV_COST
            , CONVERT(nvarchar(50), ISRUNNING) AS ISRUNNING
            , CONVERT(nvarchar(50), LOCATION) AS LOCATION
            , CONVERT(nvarchar(50), SITE_ID) AS SITE_ID
            , CONVERT(nvarchar(50), ASSET_HIERACHICAL_TYPE) AS ASSET_HIERACHICAL_TYPE
            , CONVERT(nvarchar(50), PARENT) AS PARENT
            , CONVERT(nvarchar(50), GRANDPARENT) AS GRANDPARENT
            , CONVERT(nvarchar(100), DESCRIPTION) AS DESCRIPTION


			, CONVERT(nvarchar(100), CONCAT_WS('~', ASSET_UID, CHANGE_DATE, SPVB_COSTCENTER)) AS W_INTEGRATION_ID
			, 'N' AS W_DELETE_FLG
			, 1 AS W_DATASOURCE_NUM_ID
			, GETDATE() AS W_INSERT_DT
			, GETDATE() AS W_UPDATE_DT
			, NULL W_BATCH_ID
            , 'N' AS W_UPDATE_FLG
    INTO #W_CMMS_ASSET_D_tmp
    FROM [FND].[W_CMMS_ASSET_D] F


        -- 3. Update main table using W_INTEGRATION_ID
        PRINT '3. Update main table using W_INTEGRATION_ID'

        -- 3.1. Mark existing records by flag 'Y'
        PRINT '3.1. Mark existing records by flag ''Y'''

        UPDATE #W_CMMS_ASSET_D_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_CMMS_ASSET_D_tmp tg
        INNER JOIN [dbo].[W_CMMS_ASSET_D] sc
        ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

        -- 3.2. Start updating
        PRINT '3.2. Start updating'

		UPDATE [dbo].[W_CMMS_ASSET_D]
		SET 
            ASSET_UID = src.ASSET_UID
			, SPVB_COSTCENTER = src.SPVB_COSTCENTER
            , CHANGE_DATE = src.CHANGE_DATE
            , SPVB_FIXEDASSETNUM = src.SPVB_FIXEDASSETNUM
            , TOTAL_COST = src.TOTAL_COST
            , STATUS = src.STATUS
            , STATUS_DESCRIPTION = src.STATUS_DESCRIPTION
            , TOTAL_DOWNTIME = src.TOTAL_DOWNTIME
            , ASSET_NUM = src.ASSET_NUM
            , ASSET_TYPE = src.ASSET_TYPE
            , SPVB_COSTCENTER_DESCRIPTION = src.SPVB_COSTCENTER_DESCRIPTION
            , INV_COST = src.INV_COST
            , ISRUNNING = src.ISRUNNING
            , LOCATION = src.LOCATION
            , SITE_ID = src.SITE_ID
            , ASSET_HIERACHICAL_TYPE = src.ASSET_HIERACHICAL_TYPE
            , PARENT = src.PARENT
            , GRANDPARENT = src.GRANDPARENT
            , DESCRIPTION = src.DESCRIPTION

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = getdate()
        FROM [dbo].[W_CMMS_ASSET_D] tgt
        INNER JOIN #W_CMMS_ASSET_D_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


	    -- 4. Insert non-existed records to main table from temp table
        PRINT '4. Insert non-existed records to main table from temp table'

        INSERT INTO [dbo].[W_CMMS_ASSET_D]
        (
        ASSET_UID
        , SPVB_COSTCENTER
        , CHANGE_DATE
        , SPVB_FIXEDASSETNUM
        , TOTAL_COST
        , STATUS
        , STATUS_DESCRIPTION
        , TOTAL_DOWNTIME
        , ASSET_NUM
        , ASSET_TYPE
        , SPVB_COSTCENTER_DESCRIPTION
        , INV_COST
        , ISRUNNING
        , LOCATION
        , SITE_ID
        , ASSET_HIERACHICAL_TYPE
        , PARENT
        , GRANDPARENT
        , DESCRIPTION

        , W_DELETE_FLG
        , W_DATASOURCE_NUM_ID
        , W_INSERT_DT
        , W_UPDATE_DT
        , W_BATCH_ID
        , W_INTEGRATION_ID
        )
    SELECT
        ASSET_UID
            , SPVB_COSTCENTER
            , CHANGE_DATE
            , SPVB_FIXEDASSETNUM
            , TOTAL_COST
            , STATUS
            , STATUS_DESCRIPTION
            , TOTAL_DOWNTIME
            , ASSET_NUM
            , ASSET_TYPE
            , SPVB_COSTCENTER_DESCRIPTION
            , INV_COST
            , ISRUNNING
            , LOCATION
            , SITE_ID
            , ASSET_HIERACHICAL_TYPE
            , PARENT
            , GRANDPARENT
            , DESCRIPTION

            , W_DELETE_FLG
            , W_DATASOURCE_NUM_ID
            , W_INSERT_DT
            , W_UPDATE_DT
            , W_BATCH_ID
            , W_INTEGRATION_ID
    FROM #W_CMMS_ASSET_D_tmp
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
        FROM W_CMMS_ASSET_D
        ) M
    WHERE 1=1
        AND W_BATCH_ID = @p_batch_id
        AND W_DELETE_FLG = 'N'

		SET @src_rownum = ( SELECT COUNT(1)
    FROM #W_CMMS_ASSET_D_tmp );
		SET @tgt_rownum = ( 
            SELECT
        COUNT(DISTINCT W_INTEGRATION_ID)
    FROM W_CMMS_ASSET_D
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