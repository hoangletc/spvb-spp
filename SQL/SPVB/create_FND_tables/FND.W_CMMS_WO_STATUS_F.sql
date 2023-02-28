SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_WO_STATUS_D]
(
	[PARENT] [nvarchar](100) NULL,
	[WONUM] [nvarchar](100) NULL,
	[GLACCOUNT] [nvarchar](100) NULL,
	[CHANGEDATE] [nvarchar](100) NULL,
	[WOSTATUSID] [nvarchar](100) NULL,
	[STATUS] [nvarchar](100) NULL,
	[FILE_NAME] [nvarchar](100) NULL,

	[W_BATCH_ID] [bigint] NULL,
	[W_INSERT_DT] [datetime2](7) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO

