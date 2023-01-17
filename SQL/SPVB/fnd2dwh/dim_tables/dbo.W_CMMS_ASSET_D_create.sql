SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_CMMS_ASSET_D]
(
    [ASSET_WID] INT NOT NULL IDENTITY(1, 1),

    [ASSET_UID] [nvarchar](20) NULL,
    [SPVB_COSTCENTER] [nvarchar](100) NULL,
    [CHANGE_DATE] [nvarchar](50) NULL,
    [SPVB_FIXEDASSETNUM] [nvarchar](100) NULL,
    [TOTAL_COST] [real] NULL,
    [STATUS] [nvarchar](100) NULL,
    [STATUS_DESCRIPTION] [nvarchar](100) NULL,
    [TOTAL_DOWNTIME] [real] NULL,
    [ASSET_NUM] [nvarchar](100) NULL,
    [ASSET_TYPE] [nvarchar](100) NULL,
    [SPVB_COSTCENTER_DESCRIPTION] [nvarchar](100) NULL,
    [INV_COST] [real] NULL,
    [ISRUNNING] [nvarchar](5) NULL,
    [LOCATION] [nvarchar](100) NULL,
    [SITE_ID] [nvarchar](100) NULL,
    [ASSET_HIERACHICAL_TYPE] [nvarchar](100) NULL,
    [PARENT] [nvarchar](100) NULL,
    [GRANDPARENT] [nvarchar](100) NULL,
    [DESCRIPTION] [nvarchar](200) NULL,

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
