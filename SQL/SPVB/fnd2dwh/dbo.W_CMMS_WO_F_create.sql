SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_CMMS_WO_F]
(
    [WO_WID] INT NOT NULL IDENTITY(1, 1),
    [PLANT_WID] INT NULL,
    [DATE_WID] INT NULL,
    [LOC_WID] INT NULL,
    [ASSET_WID] INT NULL, 

    [WORK_ORDERS] [nvarchar](50) NULL,
    [DESCRIPTION] [nvarchar](100) NULL,
    [TYPE] [nvarchar](30) NULL,
    [OVERHAUL] [varchar](1) NULL,
    [PM] [nvarchar](50) NULL,
    [JOB_PLAN] [nvarchar](100) NULL,
    [SITE] [nvarchar](100) NULL,
    [LOCATION] [nvarchar](50) NULL,
    [ASSET_NUM] [nvarchar](30) NULL,
    [STATUS] [nvarchar](10) NULL,
    [SUPERVISOR] [nvarchar](100) NULL,

    [DATE_CREATION] [DATETIMEOFFSET] NULL,
    [DATE_TARGET_START] [DATETIMEOFFSET] NULL,
    [DATE_TARGET_FINISH] [DATETIMEOFFSET] NULL,
    [DATE_SCHEDULE_START] [DATETIMEOFFSET] NULL,
    [DATE_SCHEDULE_FINISH] [DATETIMEOFFSET] NULL,
    [DATE_PLANNING] [DATETIMEOFFSET] NULL,
    [DATE_APPROVED] [DATETIMEOFFSET] NULL,
    [DATE_FINISHED] [DATETIMEOFFSET] NULL,
    [DATE_ACCEPTED] [DATETIMEOFFSET] NULL,
    [DATE_COMPLETED] [DATETIMEOFFSET] NULL,

    [W_DELETE_FLG] VARCHAR(1) NULL,
    [W_DATASOURCE_NUM_ID] INT NULL,
    [W_INTEGRATION_ID] [nvarchar](500) NULL,
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