SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_SAP_MBEWH_D]
(
	[MANDT] [nvarchar](500) NULL,
	[MATNR] [nvarchar](1000) NULL,
	[BWKEY] [nvarchar](1000) NULL,
	[BWTAR] [nvarchar](1000) NULL,
	[LFGJA] [nvarchar](1000) NULL,
	[LFMON] [nvarchar](1000) NULL,
	[LBKUM] [decimal](38, 18) NULL,
	[SALK3] [decimal](38, 18) NULL,
	[VPRSV] [nvarchar](max) NULL,
	[VERPR] [decimal](38, 18) NULL,
	[STPRS] [decimal](38, 18) NULL,
	[PEINH] [decimal](38, 18) NULL,
	[BKLAS] [nvarchar](1000) NULL,
	[SALKV] [decimal](38, 18) NULL,
	[VKSAL] [decimal](38, 18) NULL,
	[W_BATCH_ID] int NULL,
	[W_INSERT_DT] datetime2 NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO