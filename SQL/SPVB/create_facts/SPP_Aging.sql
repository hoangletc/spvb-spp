select 
    itemnum
    , itemsetid
    , storeloc
    , issuetype
    , transdate
    , actualdate
    , quantity
    , curbal
    , binnum
    , lotnum
    , siteid
    , ponum
    , refwo
    , gldebitacct
    , tablename
    , spvb_internal
    , rowstamp
from (           
    (
        select matusetrans.itemnum, itemsetid, matusetrans.storeloc, matusetrans.issuetype, matusetrans.transdate,
            matusetrans.actualdate, quantity, curbal, binnum, lotnum, matusetrans.siteid,
            ponum, refwo, gldebitacct, 'MATUSETRANS' tablename, invuse.spvb_internal, matusetrans.rowstamp
        from matusetrans
            left join invuse on matusetrans.invuseid = invuse.invuseid
        where   matusetrans.refwo is null and issuetype = 'RETURN'
    )
    union all
    (
        select itemnum, itemsetid, tostoreloc storeloc, issuetype, transdate, actualdate, quantity, curbal, tobin binnum, tolot lotnum, siteid, ponum, refwo, gldebitacct, 'MATRECTRANS' tablename, 0 spvb_internal , rowstamp
        from matrectrans
        where
            quantity <> 0
            and ((fromstoreloc <> tostoreloc) or (fromstoreloc is null or tostoreloc is null))
    )
    union all
    (
        select itemnum, itemsetid, storeloc, transtype issuetype, transdate, transdate actualdate, quantity, curbal, binnum, lotnum, siteid, null, null, gldebitacct, 'INVTRANS' tablename, 0 spvb_internal, rowstamp
        from invtrans
        where transtype not in ('STDCSTADJ','AVGCSTADJ','PCOUNTADJ') and quantity > 0
    )
)
order by itemnum,transdate,rowstamp desc
