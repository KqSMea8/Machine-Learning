#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import pyhs2
from pyhs2.error import Pyhs2Exception


class HiveHandler(object):

    db_conn = None

    def __init__(self, host, port, db, auth_mechanism='NOSASL'):

        self.hive_server2_node = host
        self.hive_server2_port = port
        self.database =  db
        # 需要与/etc/hive/conf/hive-site.xml中的配置hive.server2.authentication保持一致
        self.auth_mechanism = auth_mechanism
        # self.default_hive_field_separator = u'\t'  # 默认字段分隔符

        if self.db_conn is None:
            self.db_conn= pyhs2.connect(host=self.hive_server2_node,
                          port=self.hive_server2_port,
                          authMechanism=self.auth_mechanism,
                          database=self.database)

    def __del__(self):
        self.db_cusor.close()
        self.db_conn.close()

    @staticmethod
    def datas_merge(column_name_list, value_list):
        if all((column_name_list, value_list)):
            return zip(map(lambda x:x, column_name_list), value_list)
        return {}

    def query(self, hql, raise_exception=True):
        """
        执行sql语句的入口：传入sql语句，并返回json格式的数据。
        :param hql: hql 语句
        :param raise_exception: 
        :return: 状态,结果
        """
        try:
            self.db_cusor = self.db_conn.cursor()
            self.db_cusor.execute(hql)
            # print self.db_cusor.fetch()
            # 注意: fetch（）函数执行完成后，相当于把数据取出来了，再次执行时不会再有新的数据
            columnNameList = [colname.get('columnName') for colname in self.db_cusor.getSchema()]
            result = map(lambda item:dict(zip(columnNameList,item)), self.db_cusor.fetch())
            return True, result
        except Exception as e:
            if raise_exception:
                raise e
            msg = e.errorMessage if isinstance(e, Pyhs2Exception) else '%s' % e.message
            return False, '%s\nException: %s' % (hql, msg)

    def upsert(self, hql, raise_exception=True):
        try:
            self.db_cusor = self.db_conn.cursor()
            self.db_cusor.execute(hql)
            return True, u'数据更新成功.'
        except Exception as e:
            if raise_exception:
                raise e
            msg = e.errorMessage if isinstance(e, Pyhs2Exception) else '%s' % e.message
            return False, '%s\nException: %s' % (hql, msg)



if __name__ == '__main__':
    obj = HiveHandler('10.95.105.87', 10000, 'wxz_t')
    print obj.query("select * from wxz_table")
    print obj.upsert("insert into wxz_table values(7,'wangfu',35)")
    print obj.upsert("create table studen_bck (id int , name string) clustered by (id) sorted by (id asc,name desc) into 4 buckets row format delimited fields terminated by ','")