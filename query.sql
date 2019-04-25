-- 专辑pk
select
a.actid,
a.publictime as "发行时间",
a.price as "单价(分)",
a.sold_album_cnt as "总销量",
a.soldamt as "总销售额(元)",
a.album_name as "专辑",
a.singername as "歌手",
a.songcnt as "歌曲数",
b.union_cnt as "工会数",
b.iscore_cnt as "工会销量",
a.price/100*b.iscore_cnt as "工会销售额(元)",
100.0*a.price/100*b.iscore_cnt/a.soldamt as "占总销售额",
b.acct_cnt as "工会人数",
a.sold_album_cnt-b.iscore_cnt as "路人销量",
a.price/100*(a.sold_album_cnt-b.iscore_cnt) as "路人销售额(元)",
100.0*a.price/100*(a.sold_album_cnt-b.iscore_cnt)/a.soldamt as "占总销售额",
c.cnt as "工会人数(可爬)"
from
(select * from qm_act) a
join
(select
a_actid,
count(1) as union_cnt,
sum(cnt) as acct_cnt,
sum(iscore) as iscore_cnt
from qm_union group by a_actid) b
on a.actid=b.a_actid
join
(select a_actid,count(1) as cnt from qm_acct  group by a_actid)c
on a.actid=c.a_actid;

-- 工会大pk
select a.*,b.* from
(select u_uuin,count(1) as crwal_cnt from qm_acct group by u_uuin) a
join
(select * from qm_union) b
on a.u_uuin=b.uuin
order by b.iscore desc;

-- TODO:销量阶梯pk
select a_actid,iscore,count(1) from qm_acct group by iscore,a_actid order by iscore,a_actid;

-- 多担
select uuin,
count(distinct a_actid)as c,
array_agg(a_actid)  ,
array_agg(iscore)
from qm_acct
group by uuin
having count(distinct a_actid) >1
