SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (OBJECT_ID('[dbo].[proc_load_w_sap_csks_d]') is not null)
BEGIN
    DROP PROCEDURE [dbo].[proc_load_w_sap_csks_d]
END;
GO

CREATE PROC [dbo].[proc_load_w_sap_csks_d]
    @p_batch_id [bigint]
AS 
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_SAP_CSKS_D',
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

        IF OBJECT_ID(N'tempdb..#W_SAP_CSKS_D_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_SAP_CSKS_D_tmp'
            DROP Table #W_SAP_CSKS_D_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

		SELECT
            CONVERT(nvarchar(100), MANDT) AS MANDT
            , CONVERT(nvarchar(100), KOKRS) AS KOKRS
            , CONVERT(nvarchar(100), KOSTL) AS KOSTL
            , CONVERT(nvarchar(100), DATBI) AS DATBI
            , CONVERT(nvarchar(100), DATAB) AS DATAB
            , CONVERT(nvarchar(100), BKZKP) AS BKZKP
            , CONVERT(nvarchar(100), PKZKP) AS PKZKP
            , CONVERT(nvarchar(100), BUKRS) AS BUKRS
            , CONVERT(nvarchar(100), GSBER) AS GSBER
            , CONVERT(nvarchar(100), KOSAR) AS KOSAR
            , CONVERT(nvarchar(100), VERAK) AS VERAK
            , CONVERT(nvarchar(100), VERAK_USER) AS VERAK_USER
            , CONVERT(nvarchar(100), WAERS) AS WAERS
            , CONVERT(nvarchar(100), KALSM) AS KALSM
            , CONVERT(nvarchar(100), TXJCD) AS TXJCD
            , CONVERT(nvarchar(100), PRCTR) AS PRCTR
            , CONVERT(nvarchar(100), WERKS) AS WERKS
            , CONVERT(nvarchar(100), LOGSYSTEM) AS LOGSYSTEM
            , CONVERT(nvarchar(100), ERSDA) AS ERSDA
            , CONVERT(nvarchar(100), USNAM) AS USNAM
            , CONVERT(nvarchar(100), BKZKS) AS BKZKS
            , CONVERT(nvarchar(100), BKZER) AS BKZER
            , CONVERT(nvarchar(100), BKZOB) AS BKZOB
            , CONVERT(nvarchar(100), PKZKS) AS PKZKS
            , CONVERT(nvarchar(100), PKZER) AS PKZER
            , CONVERT(nvarchar(100), VMETH) AS VMETH
            , CONVERT(nvarchar(100), MGEFL) AS MGEFL
            , CONVERT(nvarchar(100), ABTEI) AS ABTEI
            , CONVERT(nvarchar(100), NKOST) AS NKOST
            , CONVERT(nvarchar(100), KVEWE) AS KVEWE
            , CONVERT(nvarchar(100), KAPPL) AS KAPPL
            , CONVERT(nvarchar(100), KOSZSCHL) AS KOSZSCHL
            , CONVERT(nvarchar(100), LAND1) AS LAND1
            , CONVERT(nvarchar(100), ANRED) AS ANRED
            , CONVERT(nvarchar(100), NAME1) AS NAME1
            , CONVERT(nvarchar(100), NAME2) AS NAME2
            , CONVERT(nvarchar(100), NAME3) AS NAME3
            , CONVERT(nvarchar(100), NAME4) AS NAME4
            , CONVERT(nvarchar(100), ORT01) AS ORT01
            , CONVERT(nvarchar(100), ORT02) AS ORT02
            , CONVERT(nvarchar(100), STRAS) AS STRAS
            , CONVERT(nvarchar(100), PFACH) AS PFACH
            , CONVERT(nvarchar(100), PSTLZ) AS PSTLZ
            , CONVERT(nvarchar(100), PSTL2) AS PSTL2
            , CONVERT(nvarchar(100), REGIO) AS REGIO
            , CONVERT(nvarchar(100), SPRAS) AS SPRAS
            , CONVERT(nvarchar(100), TELBX) AS TELBX
            , CONVERT(nvarchar(100), TELF1) AS TELF1
            , CONVERT(nvarchar(100), TELF2) AS TELF2
            , CONVERT(nvarchar(100), TELFX) AS TELFX
            , CONVERT(nvarchar(100), TELTX) AS TELTX
            , CONVERT(nvarchar(100), TELX1) AS TELX1
            , CONVERT(nvarchar(100), DATLT) AS DATLT
            , CONVERT(nvarchar(100), DRNAM) AS DRNAM
            , CONVERT(nvarchar(100), KHINR) AS KHINR
            , CONVERT(nvarchar(100), CCKEY) AS CCKEY
            , CONVERT(nvarchar(100), KOMPL) AS KOMPL
            , CONVERT(nvarchar(100), STAKZ) AS STAKZ
            , CONVERT(nvarchar(100), OBJNR) AS OBJNR
            , CONVERT(nvarchar(100), FUNKT) AS FUNKT
            , CONVERT(nvarchar(100), AFUNK) AS AFUNK
            , CONVERT(nvarchar(100), CPI_TEMPL) AS CPI_TEMPL
            , CONVERT(nvarchar(100), CPD_TEMPL) AS CPD_TEMPL
            , CONVERT(nvarchar(100), FUNC_AREA) AS FUNC_AREA
            , CONVERT(nvarchar(100), SCI_TEMPL) AS SCI_TEMPL
            , CONVERT(nvarchar(100), SCD_TEMPL) AS SCD_TEMPL
            , CONVERT(nvarchar(100), SKI_TEMPL) AS SKI_TEMPL
            , CONVERT(nvarchar(100), SKD_TEMPL) AS SKD_TEMPL
            , CONVERT(nvarchar(100), EEW_CSKS_PS_DUMMY) AS EEW_CSKS_PS_DUMMY
            , CONVERT(nvarchar(100), VNAME) AS VNAME
            , CONVERT(nvarchar(100), RECID) AS RECID
            , CONVERT(nvarchar(100), ETYPE) AS ETYPE
            , CONVERT(nvarchar(100), JV_OTYPE) AS JV_OTYPE
            , CONVERT(nvarchar(100), JV_JIBCL) AS JV_JIBCL
            , CONVERT(nvarchar(100), JV_JIBSA) AS JV_JIBSA
            , CONVERT(nvarchar(100), FERC_IND) AS FERC_IND
            , CONVERT(nvarchar(100), BUDGET_CARRYING_COST_CTR) AS BUDGET_CARRYING_COST_CTR
            , CONVERT(nvarchar(100), AVC_PROFILE) AS AVC_PROFILE
            , CONVERT(nvarchar(100), AVC_ACTIVE) AS AVC_ACTIVE
            , CONVERT(nvarchar(100), FUND) AS FUND
            , CONVERT(nvarchar(100), GRANT_ID) AS GRANT_ID
            , CONVERT(nvarchar(100), FUND_FIX_ASSIGNED) AS FUND_FIX_ASSIGNED
            , CONVERT(nvarchar(100), GRANT_FIX_ASSIGNED) AS GRANT_FIX_ASSIGNED
            , CONVERT(nvarchar(100), FUNC_AREA_FIX_ASSIGNED) AS FUNC_AREA_FIX_ASSIGNED

			, CONVERT([varbinary](100), CONCAT_WS('~', MANDT, KOKRS, KOSTL, DATBI)) AS W_INTEGRATION_ID
			, 'N' AS W_DELETE_FLG
			, 1 AS W_DATASOURCE_NUM_ID
			, GETDATE() AS W_INSERT_DT
			, GETDATE() AS W_UPDATE_DT
			, NULL W_BATCH_ID
            , 'N' AS W_UPDATE_FLG

        INTO #W_SAP_CSKS_D_tmp
        FROM [FND].[W_SAP_CSKS_D]

        -- 3. Update main table using W_INTEGRATION_ID
        PRINT '3. Update main table using W_INTEGRATION_ID'

        -- 3.1. Mark existing records by flag 'Y'
        PRINT '3.1. Mark existing records by flag ''Y'''

        UPDATE #W_SAP_CSKS_D_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_SAP_CSKS_D_tmp tg
        INNER JOIN [dbo].[W_SAP_CSKS_D] sc 
        ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

        -- 3.2. Start updating
        PRINT '3.2. Start updating'

		UPDATE  [dbo].[W_SAP_CSKS_D]
		SET 
           MANDT = src.MANDT
            , KOKRS = src.KOKRS
            , KOSTL = src.KOSTL
            , DATBI = src.DATBI
            , DATAB = src.DATAB
            , BKZKP = src.BKZKP
            , PKZKP = src.PKZKP
            , BUKRS = src.BUKRS
            , GSBER = src.GSBER
            , KOSAR = src.KOSAR
            , VERAK = src.VERAK
            , VERAK_USER = src.VERAK_USER
            , WAERS = src.WAERS
            , KALSM = src.KALSM
            , TXJCD = src.TXJCD
            , PRCTR = src.PRCTR
            , WERKS = src.WERKS
            , LOGSYSTEM = src.LOGSYSTEM
            , ERSDA = src.ERSDA
            , USNAM = src.USNAM
            , BKZKS = src.BKZKS
            , BKZER = src.BKZER
            , BKZOB = src.BKZOB
            , PKZKS = src.PKZKS
            , PKZER = src.PKZER
            , VMETH = src.VMETH
            , MGEFL = src.MGEFL
            , ABTEI = src.ABTEI
            , NKOST = src.NKOST
            , KVEWE = src.KVEWE
            , KAPPL = src.KAPPL
            , KOSZSCHL = src.KOSZSCHL
            , LAND1 = src.LAND1
            , ANRED = src.ANRED
            , NAME1 = src.NAME1
            , NAME2 = src.NAME2
            , NAME3 = src.NAME3
            , NAME4 = src.NAME4
            , ORT01 = src.ORT01
            , ORT02 = src.ORT02
            , STRAS = src.STRAS
            , PFACH = src.PFACH
            , PSTLZ = src.PSTLZ
            , PSTL2 = src.PSTL2
            , REGIO = src.REGIO
            , SPRAS = src.SPRAS
            , TELBX = src.TELBX
            , TELF1 = src.TELF1
            , TELF2 = src.TELF2
            , TELFX = src.TELFX
            , TELTX = src.TELTX
            , TELX1 = src.TELX1
            , DATLT = src.DATLT
            , DRNAM = src.DRNAM
            , KHINR = src.KHINR
            , CCKEY = src.CCKEY
            , KOMPL = src.KOMPL
            , STAKZ = src.STAKZ
            , OBJNR = src.OBJNR
            , FUNKT = src.FUNKT
            , AFUNK = src.AFUNK
            , CPI_TEMPL = src.CPI_TEMPL
            , CPD_TEMPL = src.CPD_TEMPL
            , FUNC_AREA = src.FUNC_AREA
            , SCI_TEMPL = src.SCI_TEMPL
            , SCD_TEMPL = src.SCD_TEMPL
            , SKI_TEMPL = src.SKI_TEMPL
            , SKD_TEMPL = src.SKD_TEMPL
            , EEW_CSKS_PS_DUMMY = src.EEW_CSKS_PS_DUMMY
            , VNAME = src.VNAME
            , RECID = src.RECID
            , ETYPE = src.ETYPE
            , JV_OTYPE = src.JV_OTYPE
            , JV_JIBCL = src.JV_JIBCL
            , JV_JIBSA = src.JV_JIBSA
            , FERC_IND = src.FERC_IND
            , BUDGET_CARRYING_COST_CTR = src.BUDGET_CARRYING_COST_CTR
            , AVC_PROFILE = src.AVC_PROFILE
            , AVC_ACTIVE = src.AVC_ACTIVE
            , FUND = src.FUND
            , GRANT_ID = src.GRANT_ID
            , FUND_FIX_ASSIGNED = src.FUND_FIX_ASSIGNED
            , GRANT_FIX_ASSIGNED = src.GRANT_FIX_ASSIGNED
            , FUNC_AREA_FIX_ASSIGNED = src.FUNC_AREA_FIX_ASSIGNED

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = DATEADD(HH, 7, GETDATE())

        FROM [dbo].[W_SAP_CSKS_D] tgt
        INNER JOIN #W_SAP_CSKS_D_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


	    -- 4. Insert non-existed records to main table from temp table
        PRINT '4. Insert non-existed records to main table from temp table'

        INSERT INTO [dbo].[W_SAP_CSKS_D](
             MANDT
            , KOKRS
            , KOSTL
            , DATBI
            , DATAB
            , BKZKP
            , PKZKP
            , BUKRS
            , GSBER
            , KOSAR
            , VERAK
            , VERAK_USER
            , WAERS
            , KALSM
            , TXJCD
            , PRCTR
            , WERKS
            , LOGSYSTEM
            , ERSDA
            , USNAM
            , BKZKS
            , BKZER
            , BKZOB
            , PKZKS
            , PKZER
            , VMETH
            , MGEFL
            , ABTEI
            , NKOST
            , KVEWE
            , KAPPL
            , KOSZSCHL
            , LAND1
            , ANRED
            , NAME1
            , NAME2
            , NAME3
            , NAME4
            , ORT01
            , ORT02
            , STRAS
            , PFACH
            , PSTLZ
            , PSTL2
            , REGIO
            , SPRAS
            , TELBX
            , TELF1
            , TELF2
            , TELFX
            , TELTX
            , TELX1
            , DATLT
            , DRNAM
            , KHINR
            , CCKEY
            , KOMPL
            , STAKZ
            , OBJNR
            , FUNKT
            , AFUNK
            , CPI_TEMPL
            , CPD_TEMPL
            , FUNC_AREA
            , SCI_TEMPL
            , SCD_TEMPL
            , SKI_TEMPL
            , SKD_TEMPL
            , EEW_CSKS_PS_DUMMY
            , VNAME
            , RECID
            , ETYPE
            , JV_OTYPE
            , JV_JIBCL
            , JV_JIBSA
            , FERC_IND
            , BUDGET_CARRYING_COST_CTR
            , AVC_PROFILE
            , AVC_ACTIVE
            , FUND
            , GRANT_ID
            , FUND_FIX_ASSIGNED
            , GRANT_FIX_ASSIGNED
            , FUNC_AREA_FIX_ASSIGNED

            , W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
        )
        SELECT
            MANDT
            , KOKRS
            , KOSTL
            , DATBI
            , DATAB
            , BKZKP
            , PKZKP
            , BUKRS
            , GSBER
            , KOSAR
            , VERAK
            , VERAK_USER
            , WAERS
            , KALSM
            , TXJCD
            , PRCTR
            , WERKS
            , LOGSYSTEM
            , ERSDA
            , USNAM
            , BKZKS
            , BKZER
            , BKZOB
            , PKZKS
            , PKZER
            , VMETH
            , MGEFL
            , ABTEI
            , NKOST
            , KVEWE
            , KAPPL
            , KOSZSCHL
            , LAND1
            , ANRED
            , NAME1
            , NAME2
            , NAME3
            , NAME4
            , ORT01
            , ORT02
            , STRAS
            , PFACH
            , PSTLZ
            , PSTL2
            , REGIO
            , SPRAS
            , TELBX
            , TELF1
            , TELF2
            , TELFX
            , TELTX
            , TELX1
            , DATLT
            , DRNAM
            , KHINR
            , CCKEY
            , KOMPL
            , STAKZ
            , OBJNR
            , FUNKT
            , AFUNK
            , CPI_TEMPL
            , CPD_TEMPL
            , FUNC_AREA
            , SCI_TEMPL
            , SCD_TEMPL
            , SKI_TEMPL
            , SKD_TEMPL
            , EEW_CSKS_PS_DUMMY
            , VNAME
            , RECID
            , ETYPE
            , JV_OTYPE
            , JV_JIBCL
            , JV_JIBSA
            , FERC_IND
            , BUDGET_CARRYING_COST_CTR
            , AVC_PROFILE
            , AVC_ACTIVE
            , FUND
            , GRANT_ID
            , FUND_FIX_ASSIGNED
            , GRANT_FIX_ASSIGNED
            , FUNC_AREA_FIX_ASSIGNED

            , W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
        FROM #W_SAP_CSKS_D_tmp
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
            FROM W_SAP_CSKS_D
        ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

		SET @src_rownum = ( SELECT COUNT(1) FROM #W_SAP_CSKS_D_tmp );
		SET @tgt_rownum = ( 
            SELECT 
                COUNT(DISTINCT W_INTEGRATION_ID)
            FROM W_SAP_CSKS_D
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
