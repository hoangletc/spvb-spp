SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_CMMS_TRANSACTION_F]
(
    [TRANSACTION_WID] INT NOT NULL IDENTITY(1, 1),

    [LOC_WID] INT NULL,
    [ASSET_WID] INT NULL,
    [INVU_WID] INT NULL,
    [INVUL_WID] INT NULL,
    [ITEM_WID] INT NULL,
    [WO_WID] INT NULL,

    [USAGE] [nvarchar](50) NULL,
    [WAREHOUSE] [nvarchar](100) NULL,
    [WAREHOUSE_NAME] [nvarchar](30) NULL,
    [TRANSACTION_TYPE] [varchar](30) NULL,
    [TRANSACTION_DATE] DATETIMEOFFSET NULL,
    [ACTUAL_DATE] DATETIMEOFFSET NULL,
    [ITEM_NO] [nvarchar](100) NULL,
    [DESCRIPTION] [nvarchar](300) NULL,
    [TRANSACTION_QUANT] [nvarchar](30) NULL,
    [TRANSACTION_UOM] [nvarchar](30) NULL,
    [BINNUM] [nvarchar](30) NULL,
    [OVERHAUL] [nvarchar](1) NULL,
    [MUST_RETURN_ORIGINAL] [nvarchar](1) NULL,
    [MUST_RETURN_USER_INPUT] [nvarchar](1) NULL,
    [MUST_RETURN_REMARK] [nvarchar](30) NULL,
    [PRICE] [real] NULL,
    [AMOUNT] [real] NULL,
    [MRNUM] [nvarchar](30) NULL,
    [WORK_ORDER] [nvarchar](30) NULL,
    [ASSET] [nvarchar](30) NULL,
    [LINE] [nvarchar](30) NULL,
    [WORK_TYPE] [nvarchar](30) NULL,
    [WORKORDER_STATUS] [nvarchar](30) NULL,
    [ACTUAL_FINISH] [nvarchar](30) NULL,
    [WO_LAST_STATUSDATE] DATETIMEOFFSET NULL,
    [WO_DONE_BY] [nvarchar](30) NULL,
    [USER_ID] [nvarchar](30) NULL,
    [REASON_CODE] [nvarchar](30) NULL,
    [JOURNAL_CMT_HEADER] [nvarchar](30) NULL,
    [PONUM] [nvarchar](30) NULL,
    [SAP_DND] [nvarchar](30) NULL,
    [RET_WONUM] [nvarchar](30) NULL,


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
