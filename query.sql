--截止12:19 1191088

-- 工会数
select count(1) as cnt from qm_union;

-- 工会账户数
select count(1) as cnt from qm_acct;
select sum(cnt) from qm_union;

-- 工会专辑数
select sum(iscore) from qm_union;


-- 按账户数排名
select a.*,b.* from
(select u_uuin,count(1) as crwal_cnt from qm_acct group by u_uuin) a
join
(select * from qm_union) b
on a.u_uuin=b.uuin
order by a.crwal_cnt desc;

select * from qm_acct where u_uuin='81211' order by iscore; --500后虽然取不到，但都是1了

-- 按专辑数累计账户数
select iscore,count(1) from qm_acct group by iscore order by iscore;


