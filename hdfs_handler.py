#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import os

import pyhdfs
import traceback



"""
    hdfs相关操作的封装及代码片段，把所有的代码片段封装到类中。
    官方接口： https://pyhdfs.readthedocs.io/en/latest/pyhdfs.html
"""

class HdfsHandler(object):
    """
    本类封装HDFS的读、写
    """
    def __init__(self, hostname_list, user_name="root", randomize_hosts=True,timeout=20,max_tries=2, retry_delay=5):
        """
        初始化客户端信息
        :param hostname_list: 
        :param user_name: 
        :param randomize_hosts: 
        :param timeout: 
        :param max_tries: 
        :param retry_delay: 
        """
        try:
            self.fs = pyhdfs.HdfsClient(hosts=hostname_list, user_name=user_name,randomize_hosts=randomize_hosts,
                                    timeout=timeout,max_tries=max_tries, retry_delay=retry_delay)
        except Exception as ex:
            traceback.print_exc()
            raise False, ex.message

    def current_user_root_dir(self):
        """
        :return: 返回当前用户的根目录
        """
        try:
            home_directory = self.fs.get_home_directory()
            # return home_directory
        except Exception as ex:
            return False, ex.message
        else:
            return True, home_directory

    def get_active_namenode(self):
        """
        :return: 返回可用的namenode节点
        """
        try:
            active_namenode = self.fs.get_active_namenode()
        except Exception as ex:
            return False, ex.message
        else:
            return True, active_namenode

    def foreach_dirs(self, dirname):
        """
        遍历目录下所有的文件，包括子目录中的文件
        :param dirname: 
        :return: 
        """
        for root, subdir, filenames in self.fs.walk(dirname):
            for filename in filenames:
                yield os.path.join(root, filename)

    def put_file_to_hdfs(self, src, dst):
        """
        本地文件上传至hdfs中
        :param src: 
        :param dst: 
        :return: 
        """
        try:
            self.fs.copy_from_local(src, dst)
        except Exception as ex:
            return False, ex.message
        else:
            return True, u"文件上传成功"

    def pull_file_to_local(self, src, dst):
        """
        HDFS中的文件下载到本地
        :param src: 
        :param dst: 
        :return: 
        """
        try:
            self.fs.copy_to_local(src, dst)
        except Exception as ex:
            return False, ex.message
        else:
            return True, u"文件上传成功"


    def create_file_with_datas(self, filepath, datas):
        """
        直接在hdfs上创建文件,并把datas数据写入到文件中
        :param filepath: 
        :param datas: 
        :return: 
        """
        try:
            self.fs.create(filepath, datas)
        except Exception as ex:
            return False, ex.message
        else:
            return True, u'创建成功'


    def merge_files_from_hdfs(self, target, source):
        """
        HDFS中文件合并
        :param target: 目的文件
        :param source: 源文件， 列表的形式，如：["/user/work/sparklearn.txt"]
        :return: 
        """
        self.fs.concat(target, source)

    def makedir_hdfs(self, dirname):
        try:
            status = self.fs.mkdirs(dirname)
        except Exception, ex:
            return False, ex.message
        else:
            return status, u'{}创建成功'.format(dirname)

    def delete_file(self, path):
        """
        文件删除
        :param path: 
        :return: 
        """
        try:
            self.fs.delete(path)
        except Exception, ex:
            return False, ex.message
        else:
            return True, u'{}删除成功'.format(path)



if __name__ == "__main__":

    hostname_list = ["hdp88.bigdata.zzyc.360es.cn:50070","hdp87.bigdata.zzyc.360es.cn:50070"]

    hdfsObj = HdfsHandler(hostname_list, user_name="work")
    hdfsObj.current_user_root_dir()
    for fl in hdfsObj.foreach_dirs("/user/work"):
        print fl


