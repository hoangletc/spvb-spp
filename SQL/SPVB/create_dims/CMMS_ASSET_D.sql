DECLARE @p_batch_id NVARCHAR(8) = '20230220';

WITH TMP_ASSET_LINE_INFO AS (
    -- NOTE: Nên để phần tìm Line info của mỗi asset vào Python script thay vì phải chạy lại mỗi lần query như vầy
    select
        T1.ASSET_NUM        AS ASSET_NUM
        , T2.ASSET_NUM      AS LINE_ASSET_NUM
        , T2.[DESCRIPTION]  AS LINE_ASSET_DESC
    from [FND].[W_CMMS_ASSET_D] T1, [FND].[W_CMMS_ASSET_D] T2
    where 1=1
        AND T2.ASSET_NUM = CASE 
            WHEN T1.ASSET_HIERACHICAL_TYPE = 'machine' THEN T1.[PARENT]
            WHEN T1.ASSET_HIERACHICAL_TYPE = 'component' THEN T1.[GRANDPARENT]
            ELSE T1.[ASSET_NUM]
        END
)
    SELECT
        ISNULL(LOC_X.LOC_WID, 0)                            AS LOCATION_WID

        , CASE 
            WHEN AST.ASSET_HIERACHICAL_TYPE = 'machine' THEN AST.[PARENT]
            WHEN AST.ASSET_HIERACHICAL_TYPE = 'component' THEN AST.[GRANDPARENT]
            ELSE AST.[ASSET_NUM] 
        END                                                 AS LINE_ASSET_NUM
        , TMP_ASST_L_INF.LINE_ASSET_DESC                    AS LINE_ASSET_DES
        , CONVERT(nvarchar(50), SPVB_COSTCENTER)            AS SPVB_COSTCENTER
        , CONVERT(nvarchar(50), CHANGE_DATE)                AS CHANGE_DATE
        , CONVERT(nvarchar(50), SPVB_FIXEDASSETNUM)         AS SPVB_FIXEDASSETNUM
        , CONVERT(nvarchar(50), TOTAL_COST)                 AS TOTAL_COST
        , CONVERT(nvarchar(50), AST.[STATUS])               AS [STATUS]
        , CONVERT(nvarchar(50), AST.[STATUS_DESCRIPTION])   AS STATUS_DESCRIPTION
        , CONVERT(nvarchar(50), TOTAL_DOWNTIME)             AS TOTAL_DOWNTIME
        , CONVERT(nvarchar(50), AST.ASSET_NUM)              AS ASSET_NUM
        , CONVERT(nvarchar(50), ASSET_TYPE)                 AS ASSET_TYPE
        , CONVERT(nvarchar(50), SPVB_COSTCENTER_DESCRIPTION) AS SPVB_COSTCENTER_DESCRIPTION
        , INV_COST                                          AS INV_COST
        , CONVERT(nvarchar(50), ISRUNNING)                  AS ISRUNNING
        , CONVERT(nvarchar(50), AST.[LOCATION])             AS [LOCATION]
        , CONVERT(nvarchar(50), SITE_ID)                    AS SITE_ID
        , CONVERT(nvarchar(50), ASSET_HIERACHICAL_TYPE)     AS ASSET_HIERACHICAL_TYPE
        , CONVERT(nvarchar(50), PARENT)                     AS PARENT
        , CONVERT(nvarchar(50), GRANDPARENT)                AS GRANDPARENT
        , CONVERT(nvarchar(100), AST.[DESCRIPTION])         AS [DESCRIPTION]

        , CONVERT(
            nvarchar(200), 
            CONCAT_WS(ASSET_UID, '~', SPVB_COSTCENTER, '~',
                    SPVB_FIXEDASSETNUM, '~', AST.[LOCATION])
        )                                                   AS W_INTEGRATION_ID
        , 'N'                                               AS W_DELETE_FLG
        , 'N' 											    AS W_UPDATE_FLG
        , 8                                                 AS W_DATASOURCE_NUM_ID
        , DATEADD(HH, 7, GETDATE())                         AS W_INSERT_DT
        , DATEADD(HH, 7, GETDATE())                         AS W_UPDATE_DT
        , @p_batch_id                                       AS W_BATCH_ID
    -- INTO #W_CMMS_ASSET_D_tmp
    FROM [FND].[W_CMMS_ASSET_D] AST
        LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC_X ON 1=1
            AND LOC_X.[LOCATION] = LEFT(AST.[LOCATION], 7)
        LEFT JOIN TMP_ASSET_LINE_INFO TMP_ASST_L_INF ON 1=1
            AND AST.ASSET_NUM = TMP_ASST_L_INF.ASSET_NUM