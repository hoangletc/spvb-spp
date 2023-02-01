SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_WO_F]
(
	[TARGCOMPDATE] [nvarchar](60) NULL,
	[PMNUM] [nvarchar](60) NULL,
	[ASSETNUM] [nvarchar](60) NULL,
	[WONUM] [nvarchar](60) NULL,
	[SCHEDFINISH] [nvarchar](60) NULL,
	[SUPERVISOR] [nvarchar](60) NULL,
	[TARGSTARTDATE] [nvarchar](60) NULL,
	[REPORTDATE] [nvarchar](60) NULL,
	[SPVB_OVERHAUL] [nvarchar](60) NULL,
	[WORKTYPE] [nvarchar](60) NULL,
	[SCHEDSTART] [nvarchar](60) NULL,
	[LOCATION] [nvarchar](60) NULL,
	[JPNUM] [nvarchar](60) NULL,
	[STATUS] [nvarchar](60) NULL,
	[DESCRIPTION] [nvarchar](200) NULL,
	[SITEID] [nvarchar](60) NULL,
	[ISTASK] [nvarchar](60) NULL,
	[PARENT] [nvarchar](60) NULL,
	[W_BATCH_ID] BIGINT NULL,
	[W_INSERT_DT] [nvarchar](100) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
