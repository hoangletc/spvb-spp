select *
from dbo.W_SAP_SPP_BALANCE_F
where 1=1
    and MATERIAL_NUMBER = '60000108'
    and DATE_WID = '20230131'
;

SELECT TOP 10 *
FROM dbo.W_CMMS_INVE_D
WHERE 1=1
    AND ITEM_NUM = '60000108'
    AND SITE_ID = '170'
    AND [LOCATION] = '7S0.S1'
;

-- Result: OK
--------------------------------------------------------------------------------------

select *
from dbo.W_SAP_SPP_BALANCE_F
where 1=1
    and MATERIAL_NUMBER = '60000174'
    and DATE_WID = '20230131'
;

SELECT TOP 10 *
FROM dbo.W_CMMS_INVE_D
WHERE 1=1
    AND ITEM_NUM = '60000174'
    AND SITE_ID = '170'
    AND [LOCATION] = '7S0.S1'
;

-- Result: OK
--------------------------------------------------------------------------------------

select *
from dbo.W_SAP_SPP_BALANCE_F
where 1=1
    and MATERIAL_NUMBER = '60000183'
    and DATE_WID = '20230131'
;

SELECT TOP 10 *
FROM dbo.W_CMMS_INVE_D
WHERE 1=1
    AND ITEM_NUM = '60000183'
    AND SITE_ID = '170'
    AND [LOCATION] = '7S0.S1'
;

-- Result: OK
--------------------------------------------------------------------------------------

select *
from dbo.W_SAP_SPP_BALANCE_F
where 1=1
    and MATERIAL_NUMBER = '60000200'
    and DATE_WID = '20230131'
;

SELECT TOP 10 *
FROM dbo.W_CMMS_INVE_D
WHERE 1=1
    AND ITEM_NUM = '60000200'
    AND SITE_ID = '170'
    AND [LOCATION] = '7S0.S1'
;

-- Result: OK
--------------------------------------------------------------------------------------