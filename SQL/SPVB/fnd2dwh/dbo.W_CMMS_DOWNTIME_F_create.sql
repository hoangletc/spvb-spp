SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_CMMS_DOWNTIME_F]
(
    [DATE_WID] VARCHAR(8) NULL,
    [PLANT_WID] INT NULL,
    [LOCATION_WID] INT NULL,
    [ASSET_WID] INT NULL,
    [LINE_CAT_WID] INT NULL,

    [LINE_ASSET_NUM] [nvarchar](30) NULL,
    [LINE_ASSET_DESCRIPTION] [nvarchar](1000) NULL,
    [LINE_CATEGORY] [nvarchar](100) NULL,
    [LINE_CAT_FROM_DATE] datetime2 NULL,
    [LINE_CAT_TO_DATE] datetime2 NULL,

    [MACHINE_ASSET_NUM] [nvarchar](30) NULL,
    [MACHINE_SHORTNAME] [nvarchar](100) NULL,

    [ASSET_STATUS_UID] [nvarchar](30) NULL,
    [ASSET_UID] [nvarchar](30) NULL,
    [ASSET_NUM] [nvarchar](30) NULL,
    [SITE_ID] [nvarchar](10) NULL,
    [DOWNTIME] FLOAT NULL,
    [FROMDATE] DATETIME2 NULL,
    [TODATE] DATETIME2 NULL,
    [NAME] [nvarchar](100) NULL,
    [IS_SPLIT] INT NULL,
    [N_SPLIT] INT NULL,
    [ANALYSIS_1] [nvarchar](100) NULL,
    [ANALYSIS_2] [nvarchar](100) NULL,
    [ANALYSIS_3] [nvarchar](10) NULL,
    [DOWNTIME_CODE] [nvarchar](100) NULL,
    [ISSUE] [nvarchar](100) NULL,
    [CORRECTIVE_ACTION] [nvarchar](50) NULL,
    [PREVENTIVE_ACTION] [nvarchar](50) NULL,
    [REMARKS] [nvarchar](50) NULL,
    [FILLER] [nvarchar](500) NULL,
	[FILLER_DOWNTIME] FLOAT NULL,

    [W_DELETE_FLG] VARCHAR(1) NULL,
    [W_DATASOURCE_NUM_ID] INT NULL,
    [W_INTEGRATION_ID] [nvarchar](300) NULL,
    [W_INSERT_DT] [datetime] NULL,
    [W_UPDATE_DT] [datetime] NULL,
    [W_BATCH_ID] [bigint] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO