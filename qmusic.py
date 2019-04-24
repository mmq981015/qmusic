# -*- coding: utf-8 -*-
"""
QQ音乐粉丝工会数据
"""
import json
import requests
import time
from sqlalchemy import Column, String, create_engine, Integer, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import sys

reload(sys)
sys.setdefaultencoding('utf8')

headers = {
    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1',
    'Accept': 'application/json',
    'Origin': 'https://y.qq.com',
    'Referer': 'https://y.qq.com/m/digitalbum/gold/index.html?mid=000etWPH2Sk6kG&g_f=share_music&_video=true&mod=index&skin=1'
}
pg_url = 'postgres://postgres:postgres@localhost:5432/cosmo'
g_tk = "5381"
qq = "10031199"  # 随便填
sleep_interval = 0  # 请求间隙

engine = create_engine(pg_url)
DBSession = sessionmaker(bind=engine)
session = DBSession()
Base = declarative_base()


class Union(Base):
    """
    工会
    """
    __tablename__ = 'qm_union'
    irank = Column(Integer)  # 销量排名
    uuin = Column(Integer, primary_key=True)  # 工会id
    unionname = Column(String(128))  # 工会名
    iscore = Column(Integer)  # 销量
    cnt = Column(Integer)  # 账户数


class Acct(Base):
    """
    账户
    """
    __tablename__ = 'qm_acct'
    irank = Column(Integer)  # 销量排名
    uuin = Column(String(128), primary_key=True)  # qq号一览无余
    strnick = Column(String(128))  # 昵称
    iscore = Column(Integer)  # 销量
    u_uuin = Column(Integer)  # 工会id


def crawl_union(begin=0, end=9, autoPage=True):
    """
    爬取工会信息
    :param begin:
    :param end:
    :param autoPage:
    :return:
    """
    rankname = "group_rank_peract_339"
    while True:
        gh_url = "https://c.y.qq.com/shop/fcgi-bin/fcg_album_rank?" \
                 "g_tk={}&uin={}&format=json&inCharset=utf-8&outCharset=utf-8&notice=0&platform=h5&needNewCode=1&begin={}&end={}&rankname={}&_={}" \
            .format(g_tk, qq, begin, end, rankname, int(time.time()))
        gh_req = requests.get(gh_url, headers=headers)
        js = json.loads(gh_req.text)
        cnt = js['data'][rankname + '_count']
        for a in js['data'][rankname]:
            item = Union(irank=a['iRank'], uuin=a['uUin'], unionname=a['unionName'], iscore=a['iScore'])
            print("----------{}---------".format(item.unionname))
            gh_cnt = crawl_acct(item.uuin)
            item.cnt = gh_cnt
            session.add(item)
            session.commit()
        if not autoPage:
            break
        begin += 10
        end += 10
        print(end)
        print(cnt)
        if end > cnt + 10:  # TODO 怀疑500后也爬不到，先忽视
            break
        time.sleep(sleep_interval)


def crawl_acct(uUin):
    """
    爬取工会内的账户信息
    :param uUin: 工会id
    :return:
    """
    begin = 0
    end = 9
    rankname = "group_{}_rank_peract_339".format(uUin)
    while True:
        gm_url = "https://c.y.qq.com/shop/fcgi-bin/fcg_album_rank?" \
                 "g_tk={}&uin={}&format=json&inCharset=utf-8&outCharset=utf-8&notice=0&platform=h5&needNewCode=1&rankname={}&begin={}&end={}&_={}" \
            .format(g_tk, qq, rankname, begin, end, int(time.time()))
        gm_req = requests.get(gm_url, headers=headers)
        js = json.loads(gm_req.text)
        print(gm_url)
        cnt = js['data'][rankname + '_count']
        for a in js['data'][rankname]:
            item = Acct(irank=a['iRank'], uuin=a['uUin'], strnick=a['strNick'], iscore=a['iScore'])
            item.u_uuin = uUin
            session.add(item)
            session.commit()
        begin += 10
        end += 10
        if end > cnt + 10 or end > 499:  # 注意！500后爬不到，数据为空
            break
        time.sleep(sleep_interval)
    return cnt


if __name__ == '__main__':
    # 重建表
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    # 爬取所有工会
    crawl_union()

    # 只爬取某个工会
    # crawl_union(begin=12, end=12, autoPage=False)
