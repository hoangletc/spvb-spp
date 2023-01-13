SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_SAP_T001L_D]
(
    [ASSET_WID] INT IDENTITY(1, 1),

    [MANDT] [nvarchar](300) NULL,
    [WERKS] [nvarchar](300) NULL,
    [LGORT] [nvarchar](300) NULL,
    [LGOBE] [nvarchar](1200) NULL,
    [SPART] [nvarchar](300) NULL,
    [XLONG] [nvarchar](300) NULL,
    [XBUFX] [nvarchar](300) NULL,
    [DISKZ] [nvarchar](300) NULL,
    [XBLGO] [nvarchar](300) NULL,
    [XRESS] [nvarchar](300) NULL,
    [XHUPF] [nvarchar](300) NULL,
    [PARLG] [nvarchar](300) NULL,
    [VKORG] [nvarchar](300) NULL,
    [VTWEG] [nvarchar](300) NULL,
    [VSTEL] [nvarchar](300) NULL,
    [LIFNR] [nvarchar](300) NULL,
    [KUNNR] [nvarchar](300) NULL,
    [MESBS] [nvarchar](300) NULL,
    [MESST] [nvarchar](300) NULL,
    [OIH_LICNO] [nvarchar](300) NULL,
    [OIG_ITRFL] [nvarchar](300) NULL,
    [OIB_TNKASSIGN] [nvarchar](300) NULL,

    [W_DELETE_FLG] VARCHAR(1) NULL,
    [W_DATASOURCE_NUM_ID] INT NULL,
    [W_INTEGRATION_ID] [nvarchar](300) NULL,
    [W_INSERT_DT] [datetime] NULL,
    [W_UPDATE_DT] [datetime] NULL,
    [W_BATCH_ID] [bigint] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
