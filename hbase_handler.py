# !/usr/bin/env python
# _*_ coding:utf-8 _*_


import traceback
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation, IOError


"""
Hbase-thrift-api: 
    https://wiki.apache.org/hadoop/Hbase/ThriftApi

安装准备：
    使用 python 通过 Thrift 连接并操作 HBase 数据库
    prepare：
        1. 启动 ThriftServer 于 HBASE
        > hbase-deamn.sh start thrift/thrift2
        > 在此，HBASE提供两种 thrift/thrift2 由于种种原因，语法并不兼容，其中 2 的语法封装更优雅，但部分 DDL 操作
          不完善，而且 thrift API 资料相对多一些，所以先使用thrift 尝试
        2. jps 应该有 ThriftServer 进程
        3.Python 需要安装 thrift 和 hbase 模块，有网络的直接 pip，没有网络的把相同版本的模块代码下载下来用 
          sys.path.append('PATH') 引用,安装后的代码一般在 $PYTHON_HOME/Lib/site-packages
        > pip install thrift
          pip install hbase-thrift


相关小知识点：
1. 获取列族信息，返回map
    columnDescriptors = self.client.getColumnDescriptors(tablename)

2. 获取该表的所有Regions，包括起止key等信息，返回list
    tableRegions = client.getTableRegions(tableName)

3. 获取行(tableName,rowKey) return List<TRowResult>
    row = client.getRow(tableName,rowkey)

4. 获取 row 里的某一列
    client.get(tableName,rowkey,"person:name")

5. 获取 row 里的多列时间戳最新的，None 则为所有列
    client.getRowWithColumns(tableName,rowkey,["person:name","person:age"])
    
6. Hbase没有数据的修改，可以对row_key 重新插入数据
    
"""


class HbaseWriter(object):
    def __init__(self, hostname, port):
        self.transport = TTransport.TBufferedTransport(
            TSocket.TSocket(hostname, port))
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)
        self.transport.open()

    def __del__(self):
        self.transport.close()

    # 1.删除表
    def delete_table(self, tablename):
        """
        使用方式：
            obj = HbaseWriter('hdp87.bigdata.zzyc.360es.cn', 9090, 't_hbase_wxz')
            status, messages = obj.delete_table()
            if not status:
                print messages
            print messages
        """

        try:
            self.client.disableTable(tablename)
            self.client.deleteTable(tablename)
        except Exception as ex:
            # 记录log, traceback.print_exc()
            return False, ex.message
        else:
            return True, u'{}表删除成功'.format(tablename)

    # 2.创建表
    def create_table(self, tablename, cflist):
        """
            创建Hbase表,建议列族最多不超过三个,并指定列族信息.
            cflist: 以列表的形式传入列族及列的信息，如：["person:", "geo:"]
            
            使用方式：
                create_table_status, create_table_message = obj.create_table(["person:", "geo:"])
                if not create_table_status:
                    print create_table_message
                print create_table_message
        """
        try:
            # 首先判断是否存在要创建的表,如果存在则跳过,否则会创建
            # 获取所有的表
            tables = self.client.getTableNames()
            if tablename in tables:
                return False, u'{}表已存在'.format(tablename)

            # 用户可添加多个列族信息,并以列表的形式传参
            cloumn_family_list = map(lambda cf: ColumnDescriptor(name=cf, maxVersions=1), cflist)

            self.client.createTable(tablename, cloumn_family_list)
        except Exception as ex:
            return False, ex.message
        else:
            return True, u'{}表创建成功'.format(tablename)

    def _package_mutation(self, datas_list):
        """
        解析传入的数据格式
        :param datas_list: 
        :return: 
        """
        matations_dict = {}
        for datas_dict in datas_list:
            for rowkey, cells in datas_dict.iteritems():
                for cell in cells:
                    matations_dict.setdefault(rowkey, []).append(Mutation(column=cell.get('column'),
                                                                          value=cell.get('value')))
        return matations_dict

    # 3.插入数据(单行数据写入)
    def write_datas_to_table(self, tablename, datas_list):
        """
        :param datas_list: 数据列表, 需要按照固定格式封装
        :datas_format_json = [
        {"rowkey1": [
            {
                "column": "person:name",
                "value": "wangtao"
            },
            {
                "column": "person:age",
                "value": "12"
            },
            {
                "column": "person:sex",
                "value": "male"
            },
            {
                "column": "person:address",
                "value": "Shandong"
            }]
        },
        {"rowkey2": [
            {
                "column": "person:name",
                "value": "wangxuezhi"
            },
            {
                "column": "person:age",
                "value": "28"
            },
            {
                "column": "person:sex",
                "value": "female"
            },
            {
                "column": "person:address",
                "value": "Beijing"
            }]
        }
        ]
        
        使用例子： 
            obj = HbaseWriter('hdp87.bigdata.zzyc.360es.cn', 9090, 't_hbase_wxz')
            write_datas_status, write_datas_message = obj.write_datas_to_table(datas_format_json)
        """

        try:
            matations_dict = self._package_mutation(datas_list)
            # 封装多个rowkey 批量插入Hbase
            row_mutations = map(lambda (rowkey,batch_mutations): BatchMutation(rowkey,batch_mutations),
                                matations_dict.iteritems())

            self.client.mutateRows(tablename, row_mutations)
        except Exception as ex:
            # log
            traceback.print_exc()
            return False, ex.message
        else:
            return True, u'load datas into {tablename} success.'.format(tablename=tablename)

    # 4。删除行
    def delete_row(self, tablename, rowkey):
        try:
            self.client.deleteAllRow(tablename, rowkey)
        except:
            return False, u'删除行{}失败'.format(rowkey)
        else:
            return False, u'删除行{}成功'.format(rowkey)

    # 5. 查询数据-查询比较精细
    def scan_datas_from_table(self, tablename, rowkey, clomunfamilylist, nbrows):
        '''
        :param tablename: 要查询的表名称
        :param rowkey: 要查询的rowkey,rowkey参数为'',表示取出所有的rowkey的数据
        :param clomunlist: 要查询的列族名称
        :param nbrows: 数据索取的步长
        :return: 
        ：例子
            results = obj.scan_datas_from_table('t_hbase_wxz', 'rowkey_1', ['person'], 50)
            for res in results:
                print res
            
         也可以使用如下方式，一条一条的遍历:
         while True:
            r = self.client.scannerGet(scanner)
        '''

        scannerId = self.client.scannerOpen(tablename, rowkey, clomunfamilylist)
        while True:
            try:
                # 取出一条
                # result = self.client.scannerGet(scannerId)
                # 取出多条
                results = self.client.scannerGetList(scannerId, nbrows)
                if not results:
                    break
                yield results
            except:
                traceback.print_exc()
                break
        self.client.scannerClose(scannerId)

    #5. 获取某一列/行的数据（根据条件精准查询）
    def get_clomn_datas(self, tablename, rowkey, cloumn):
        """
        精确获取某行和某列的数据
        :param tablename: 表名
        :param rowkey: rowkey
        :param cloumn: 列 如: person:name
        :return: if clomun 返回某列的数据，else 返回某行数据
        """

        if cloumn:
            return self.client.get(tablename, rowkey, cloumn)
        return self.client.getRow(tablename, rowkey)




if __name__ == "__main__":
    obj = HbaseWriter('hdp87.bigdata.zzyc.360es.cn', 9090)


    status, messages = obj.delete_table('t_hbase_wxz')
    if not status:
        print messages
    print messages

    create_table_status, create_table_message = obj.create_table('t_hbase_wxz', ["person:", "geo:"])
    if not create_table_status:
        print create_table_message
    print create_table_message

    """
    datas_format_json = [
        {"rowkey1": [
            {
                "column": "person:name",
                "value": "wangtao"
            },
            {
                "column": "person:age",
                "value": "12"
            },
            {
                "column": "person:sex",
                "value": "male"
            },
            {
                "column": "person:address",
                "value": "Shandong"
            }]
        },
        {"rowkey2": [
            {
                "column": "person:name",
                "value": "wangxuezhi"
            },
            {
                "column": "person:age",
                "value": "28"
            },
            {
                "column": "person:sex",
                "value": "female"
            },
            {
                "column": "person:address",
                "value": "Beijing"
            }]
        }
    ]

    # datas_format_json = map(lambda x:x, [])
    datas_format_json = []
    for num in xrange(100):
        datas_format_json.append({"rowkey_%d"%num: [{"column": "person:name", "value": "test_%s"%num},
                                     {"column": "person:age", "value": "%s" % num},
                                     {"column": "person:sex", "value": "%s" %('male' if num % 2 ==0  else 'female')},
                                     ]})

    start_time =  time.time()

    write_datas_status, write_datas_message = obj.write_datas_to_table('t_hbase_wxz',datas_format_json)
    end_time = time.time()

    print 'left time:',(end_time -  start_time)
    # print write_datas_message


    results = obj.scan_datas_from_table('t_hbase_wxz', 'rowkey_1', ['person'], 50)
    # for res in results:
    #     print res
    
    """

    pass



