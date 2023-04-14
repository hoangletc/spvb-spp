SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_WO_F]
(
	[ACTFINISH] [nvarchar](100) NULL,
	[ACTSTART] [nvarchar](100) NULL,
	[WORKORDER_ID] [nvarchar](20) NULL,
	[SITE_ID] [nvarchar](20) NULL,
	[ASSETNUM] [nvarchar](100) NULL,
	[DESCRIPTION] [nvarchar](1000) NULL,
	[GLACCOUNT] [nvarchar](100) NULL,
	[HASCHILDREN] [nvarchar](100) NULL,
	[ISTASK] [nvarchar](100) NULL,
	[JPNUM] [nvarchar](100) NULL,
	[LOCATION] [nvarchar](100) NULL,
	[PMDUEDATE] [nvarchar](100) NULL,
	[PMNUM] [nvarchar](100) NULL,
	[REPORTDATE] [nvarchar](100) NULL,
	[SCHEDFINISH] [nvarchar](100) NULL,
	[SPVB_OVERHAUL] [nvarchar](100) NULL,
	[SCHEDSTART] [nvarchar](100) NULL,
	[SPVB_TASK_STATUS] [nvarchar](100) NULL,
	[STATUS] [nvarchar](100) NULL,
	[SUPERVISOR] [nvarchar](100) NULL,
	[TARGCOMPDATE] [nvarchar](100) NULL,
	[TARGSTARTDATE] [nvarchar](100) NULL,
	[WONUM] [nvarchar](100) NULL,
	[WOPRIORITY] [nvarchar](100) NULL,
	[WORKTYPE] [nvarchar](100) NULL,
	[PARENT] [nvarchar](100) NULL,
	[SUPPERVISORNAME] [nvarchar](100) NULL,
	[FILE_NAME] [nvarchar](100) NULL,

	[SCHEDSTART_ORG] [nvarchar](100) NULL,
	[SCHEDFINISH_ORG] [nvarchar](100) NULL,

	[W_BATCH_ID] BIGINT NULL,
	[W_INSERT_DT] [nvarchar](100) NULL

)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO