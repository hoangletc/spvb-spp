SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_EXCEL_SPP_MARKET_PRICE_F]
(
	[CODE] [nvarchar](100) NULL,
	[PLANT] [nvarchar](100) NULL,
	[SPP_CODE] [nvarchar](100) NULL,
	[DESCRIPTION] [nvarchar](1000) NULL,
	[BASE_UNIT] [nvarchar](100) NULL,
	[PRICE] DECIMAL(38, 20),
	
	[PLANT_NAME] [nvarchar](100) NULL,
	[PERIOD] [bigint] NULL,
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
