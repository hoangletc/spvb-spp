SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_MATR_D]
(
	[ISSUE_TYPE] [nvarchar](max) NULL,
	[LINE_COST] [real] NULL,
	[QUANTITY] [real] NULL,
	[ISSUE_TYPE_DESCRIPTION] [nvarchar](max) NULL,
	[UNIT_COST] [nvarchar](max) NULL,
	[TRANS_DATE] [nvarchar](max) NULL,
	[ACTUAL_DATE] [nvarchar](max) NULL,
	[W_BATCH_ID] [nvarchar](max) NULL,
	[W_INSERT_DT] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
