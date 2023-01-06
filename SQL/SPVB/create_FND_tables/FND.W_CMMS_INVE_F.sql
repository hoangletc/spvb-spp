SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_INVE_F]
(
	[ITEM_NUMBER] [nvarchar](max) NULL,
	[MAX_LEVEL] [bigint] NULL,
	[LAST_ISSUE_DATE] [nvarchar](max) NULL,
	[SITE_ID] [bigint] NULL,
	[LOCATION] [nvarchar](max) NULL,
	[AVG_COST] [bigint] NULL,
	[W_BATCH_ID] [nvarchar](max) NULL,
	[W_INSERT_DT] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
