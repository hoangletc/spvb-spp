SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_EXCEL_SPP_SPENDING_AOP_F]
(
	[AP_ACCOUNT] [nvarchar](100) NULL,
	[SAP_ACCOUNT_NAME] [nvarchar](100) NULL,
	[DESCRIPTION] [nvarchar](1000) NULL,
	[AOP_AMOUNT] [decimal](38, 20) NULL,
	[NORM_ONE_OFF] [nvarchar](100) NULL,
	[JULF_22] [decimal](38, 20) NULL,

	[INC_DEC_PERCENT] [decimal](38, 20) NULL,
	[NOTE] [nvarchar](500) NULL,
	[PERIOD] int NULL,
	[SPENDING_AMOUNT] [decimal](38, 20) NULL,
	[RM_TYPE] [nvarchar](100) NULL,
	[LINE_FUNCTION_NAME] [nvarchar](100) NULL,
	[MACHINE] [nvarchar](100) NULL,
	[PM_CM_OVH] [nvarchar](100) NULL,
	[SPP_SERVICE] [nvarchar](100) NULL,
	
	[PLANT_NAME] [nvarchar](100) NULL,
	[FILE_PATH] [nvarchar](100) NULL,

	[W_INSERT_DT] [datetime2] NULL,
	[W_UPDATE_DT] [datetime2] NULL,
	[W_BATCH_ID] [bigint] NULL,
	[W_DATASOURCE_NUM_ID] [nvarchar](2) NULL,
	[W_DELETE_FLG] [nvarchar](2) NULL,
	[W_INTEGRATION_ID] [nvarchar](4000) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
