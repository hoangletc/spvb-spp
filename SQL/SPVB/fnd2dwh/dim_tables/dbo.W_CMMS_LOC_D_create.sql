SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_CMMS_LOC_D]
(
    [LOC_WID] INT IDENTITY(1, 1),

    [SPVB_WHC_GROUP] [nvarchar](50) NULL,
    [DESCRIPTION] [nvarchar](50) NULL,
    [TYPE] [nvarchar](50) NULL,
    [LOCATIONS_ID] [bigint] NULL,
    [SITE] [nvarchar](5) NULL,
    [TYPE_DESCRIPTION] [nvarchar](50) NULL,
    [STATUS_DESCRIPTION] [nvarchar](50) NULL,
    [LOCATION] [nvarchar](50) NULL,
    [STATUS] [nvarchar](50) NULL,

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