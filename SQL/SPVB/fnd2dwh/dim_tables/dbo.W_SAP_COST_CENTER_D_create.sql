SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_SAP_COST_CENTER_D]
(
    [COST_CENTER_WID] INT IDENTITY(1, 1),

    [MANDT] [nvarchar](100) NULL,
    [KOKRS] [nvarchar](100) NULL,
    [KOSTL] [nvarchar](100) NULL,
    [DATBI] [nvarchar](100) NULL,
    [DATAB] [nvarchar](100) NULL,
    [BKZKP] [nvarchar](100) NULL,
    [PKZKP] [nvarchar](100) NULL,
    [BUKRS] [nvarchar](100) NULL,
    [KTEXT] [nvarchar](100) NULL,

    [W_DELETE_FLG] VARCHAR(1) NULL,
    [W_DATASOURCE_NUM_ID] INT NULL,
    [W_INTEGRATION_ID] [nvarchar](300) NULL,
    [W_INSERT_DT] [datetime2] NULL,
    [W_UPDATE_DT] [datetime2] NULL,
    [W_BATCH_ID] [bigint] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO