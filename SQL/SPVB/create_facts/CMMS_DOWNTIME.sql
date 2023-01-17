WITH TMP_ASSET_LINE_INFO AS (
    # NOTE: Nên để phần tìm Line info của mỗi asset vào Python script thay vì phải chạy lại mỗi lần query như vầy
    select
        T1.ASSET_WID,
        T1.ASSET_NUM AS ASSET_NUM,
        T2.ASSET_NUM AS LINE_ASSET_NUM,
        T2.[DESCRIPTION] AS LINE_DESCRIPTION
    from [dbo].[W_CMMS_ASSET_D] T1, [dbo].[W_CMMS_ASSET_D] T2
    where 1=1
        AND T2.ASSET_NUM = CASE 
            WHEN T1.ASSET_HIERACHICAL_TYPE = 'machine' THEN T1.[PARENT]
            WHEN T1.ASSET_HIERACHICAL_TYPE = 'component' THEN T1.[GRANDPARENT]
            ELSE T1.[ASSET_NUM]
        END
)
    SELECT TOP 100
        TMP_ASST_L_INF.LINE_ASSET_NUM AS LINE_ASSET_NUM
        , TMP_ASST_L_INF.LINE_DESCRIPTION AS LINE_DESCRIPTION
        , F.ASSET_NUM AS ASSET_NUM
        , CONVERT(DECIMAL(38,20), DOWNTIME) * 60 AS DOWNTIME
        , CONVERT(nvarchar(100), ASST.DESCRIPTION) AS NAME
        , NULL AS ANALYSIS_1 -- NOTE: Không biết là cột nào
        , NULL AS ANALYSIS_2 -- NOTE: Không biết là cột nào
        , CONVERT(nvarchar(5), CODE) AS ANALYSIS_3
        , CONVERT(nvarchar(50), CODE_DESCRIPTION) AS DOWNTIME_CODE
        , NULL AS ISSUE -- NOTE: Không biết là cột nào
        
        , CONCAT_WS('~', ASSET_STATUS_ID, F.LOCATION, CHANGEDATE, CODE) AS W_INTEGRATION_ID
        , 'N' AS W_DELETE_FLG
        , 1 AS W_DATASOURCE_NUM_ID
        , GETDATE() AS W_INSERT_DT
        , GETDATE() AS W_UPDATE_DT
        , NULL W_BATCH_ID
        , 'N' AS W_UPDATE_FLG
    FROM [FND].[W_CMMS_ASSET_STATUS_F] F
        LEFT JOIN [dbo].[W_PLANT_SAP_D] PLANT ON 1=1
            AND PLANT.PLANT_NAME_2 = LEFT(F.LOCATION, 3)
        LEFT JOIN [dbo].[W_CMMS_LOC_D] LOC ON 1=1
            AND LOC.LOCATION = F.LOCATION
        LEFT JOIN [dbo].[W_CMMS_ASSET_D] ASST ON 1=1
            AND ASST.ASSET_NUM = F.ASSET_NUM
        LEFT JOIN TMP_ASSET_LINE_INFO AS TMP_ASST_L_INF ON 1=1
            AND F.ASSET_NUM = TMP_ASST_L_INF.ASSET_NUM
