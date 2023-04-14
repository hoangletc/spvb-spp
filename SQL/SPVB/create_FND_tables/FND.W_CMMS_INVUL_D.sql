SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_INVUL_D]
(
	[INVUSELINE_ID] [bigint] NULL,
	[UNITCOST] [real] NULL,
	[SPVB_EXTREASONCODE] [nvarchar](100) NULL,
	[SPVB_EXTREASONCODE_DESCRIPTION] [nvarchar](1000) NULL,
	[SPVB_MUSTRETURN] [nvarchar](100) NULL,
	[SPVB_RETURNFROMISSUE] [nvarchar](100) NULL,
	[SPVB_MUSTRETURN_ORG] [nvarchar](100) NULL,
	[RETURNED_QTY] [real] NULL,
	[REMARK] [nvarchar](300) NULL,
	[LINE_TYPE] [nvarchar](100) NULL,
	[DESCRIPTION] [nvarchar](1000) NULL,
	[TO_SITEID] [nvarchar](100) NULL,
	[INVUSE_NUM] [nvarchar](100) NULL,
	[ACTUALDATE] DATETIME2 NULL,
	[RECEIVED_QTY] [real] NULL,
	[ASSET_NUM] [nvarchar](100) NULL,
	[COSTCENTER] [nvarchar](100) NULL,
	[FROM_STORELOC] [nvarchar](100) NULL,
	[REFWO] [nvarchar](100) NULL,
	[LINECOST] [real] NULL,
	[QUANTITY] [real] NULL,
	[COSTCENTER_DESCRIPTION] [nvarchar](1000) NULL,
	[INVUSELINE_NUM] [bigint] NULL,
	[ITEM_NUM] [nvarchar](100) NULL,
	[ITEMSETID] [nvarchar](100) NULL,
	[LOCATION] [nvarchar](100) NULL,
	[SPVB_SAPPO] [nvarchar](100) NULL,
	[USE_TYPE] [nvarchar](100) NULL,
	[ENTER_BY] [nvarchar](100) NULL,
	[SPVB_WONUMREF] [nvarchar](100) NULL,
	[SPVB_REASON] [nvarchar](100) NULL,
	[FROM] MVARCHAR(30) NULL,
	[FILE_NAME] NVARCHAR(100) NULL,

	[W_BATCH_ID] [bigint] NULL,
	[W_INSERT_DT] DATETIME2 NULL,
	[W_DELETE_FLG] VARCHAR(1) NULL,
    [W_DATASOURCE_NUM_ID] INT NULL,
    [W_INTEGRATION_ID] NVARCHAR(1000) NULL,
	[W_UPDATE_DT] DATETIME2 NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO

SELECT TOP 30 * FROM [FND].[W_CMMS_INVUL_D];


INSERT INTO FND.W_CMMS_INVUL_D( [INVUSELINE_ID], [UNITCOST], [SPVB_EXTREASONCODE], [SPVB_EXTREASONCODE_DESCRIPTION], [SPVB_MUSTRETURN], [SPVB_RETURNFROMISSUE], [SPVB_MUSTRETURN_ORG], [RETURNED_QTY], [REMARK], [LINE_TYPE], [DESCRIPTION], [TO_SITEID], [INVUSE_NUM], [ACTUALDATE], [RECEIVED_QTY], [ASSET_NUM], [COSTCENTER], [FROM_STORELOC], [REFWO], [LINECOST], [QUANTITY], [COSTCENTER_DESCRIPTION], [INVUSELINE_NUM], [ITEM_NUM], [ITEMSETID], [LOCATION], [SPVB_SAPPO], [USE_TYPE], [ENTER_BY], [SPVB_WONUMREF], [SPVB_REASON], [MATU_ID], [FILE_NAME], W_INSERT_DT, W_BATCH_ID ) 
 SELECT [INVUSELINE_ID], [UNITCOST], [SPVB_EXTREASONCODE], [SPVB_EXTREASONCODE_DESCRIPTION], [SPVB_MUSTRETURN], [SPVB_RETURNFROMISSUE], [SPVB_MUSTRETURN_ORG], [RETURNED_QTY], [REMARK], [LINE_TYPE], [DESCRIPTION], [TO_SITEID], [INVUSE_NUM], [ACTUALDATE], [RECEIVED_QTY], [ASSET_NUM], [COSTCENTER], [FROM_STORELOC], [REFWO], [LINECOST], [QUANTITY], [COSTCENTER_DESCRIPTION], [INVUSELINE_NUM], [ITEM_NUM], [ITEMSETID], [LOCATION], [SPVB_SAPPO], [USE_TYPE], [ENTER_BY], [SPVB_WONUMREF], [SPVB_REASON], [MATU_ID], [FILE_NAME],  DATEADD(HH, 7, GETDATE()), 20230225
 FROM STG.W_CMMS_INVUL_DS