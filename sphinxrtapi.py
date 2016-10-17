# coding=utf-8
"""
功能: sphinx的实时索引增删改查的API 由于sphinx提供的官方API中不支持对实时索引增删操作，只能使用mysql的客户端
"""
import sphinxapi
import MySQLdb
from MySQLdb import Error


class SphinxAPI(sphinxapi.SphinxClient):
    """
    一个对sphinx服务进行实时操作的类，支持对实时索引的操作
    """

    def __init__(self, index_fields=None, index_rt=True, host="localhost", port=9312, rt_port=9306, index_name=None):
        sphinxapi.SphinxClient.__init__(self)
        # 实时索引的操作需要根据mysql41 进行直接操作 使用mysqldb进行底层连接 db随便写个名称就可以
        self._connect = MySQLdb.connect(host=host, port=rt_port, db="helloworld")
        self.rt = index_rt
        self.index = index_name
        # self.sphinx_client = sphinxapi.SphinxClient()
        self.SetServer(host, port)
        # 注意字段属性必须要按正确的顺序填入
        self.fields = self.get_index_fields() or index_fields

    def _execute_sql(self, cursor, sql):
        # 执行SQL语句
        try:
            cursor.execute(sql)
            self._connect.commit()
        except Error as e:
            print e
            cursor.close()
            return False
        else:
            cursor.close()
            return True

    def get_index_fields(self, index_name=None):
        cursor = self._connect.cursor()
        try:
            cursor.execute("desc %s" % self.index or index_name)
            ret = cursor.fetchall()
        except Error as e:
            print e
            cursor.close()
            return False
        else:
            cursor.close()
            fields = []
            for info in ret:
                if info[1] != "field":
                    fields.append(info[0])
            return fields

    @staticmethod
    def _conver_kwargs(kwargs):
        # 由于sphinx插入时 字段必须是字符格式
        for key in kwargs.keys():
            if key == "id":
                continue
            kwargs[key] = kwargs[key] if isinstance(kwargs[key], basestring) else str(kwargs[key])

    def insert(self, index_name=None, **kwargs):
        """
        功能: 向实时索引中插入一条数据
        @param kwargs: 插入实时索引的内容
        @param index_name: 表名
        @return: 成功:True 失败:False
        """
        table = index_name or self.index
        assert table
        self._conver_kwargs(kwargs)

        cursor = self._connect.cursor()
        field_name = kwargs.keys()

        # 构造SQL语句
        field_str = ''
        value_str = ''
        for field in field_name:
            field_str = '%s %s %s' % (field_str, field, ',')
            if isinstance(kwargs[field], str):
                value_str = "%s '%s' %s" % (value_str, kwargs[field], ',')
            else:
                value_str = "%s %s %s" % (value_str, kwargs[field], ',')
        # 清除最后的一个 ','字符
        field_str = field_str[0:-2]
        value_str = value_str[0:-2]
        sql = 'insert into %s (%s) values(%s)' % (table, field_str, value_str)

        # 执行SQL语句
        return self._execute_sql(cursor, sql)

    def delete_by_id(self, record_id, index_name=None):
        table = index_name or self.index
        assert table

        sql = "delete from %(table)s where id=%(id)s" % {"table": table, "id": record_id}
        cursor = self._connect.cursor()

        # 执行SQL语句
        return self._execute_sql(cursor, sql)

    def update_by_id(self, record_id, index_name=None, fields=None, **kwargs):
        """
        通过id更新索引内容 2.0.2版本的sphinx是不支持更新的 所以现在只能先查询出这条记录 然后删除 在插入
        @param index_name: 表名
        @param record_id: 更新内容的id
        @param fields: 字段名
        @param kwargs: 更新内容
        @return:
        """
        table = index_name or self.index
        fields_name = fields or self.fields
        assert table and fields_name
        self._conver_kwargs(kwargs)

        sql = "select * from %s where id=%s" % (table, record_id)
        cursor = self._connect.cursor()

        try:
            cursor.execute(sql)
            ret = cursor.fetchone()
        except Error:
            cursor.close()
            return False

        if not ret:
            return False
        # 根据字段属性 一一对应各自的值
        if len(fields_name) != len(ret):
            return False
        # py2.6 不支持这种写法 fuck 赶紧升级吧
        # values = {field: ret[fields_name.index(field)] for field in fields_name}
        i = 0
        values = {}
        # sphinx 的实时索引 在插入的时候 除了id 其他的都必须转换成字符格式插入 但是读取的时候 它又会根据你的配置自动给转过来
        for field in fields_name:
            values[field] = str(ret[i]) if not isinstance(ret[i], basestring) else ret[i]
            i += 1
        # 更新
        values.update(kwargs)
        values["id"] = record_id

        # 删除原有数据
        self.delete_by_id(record_id, index_name)
        # 重新插入
        return self.insert(index_name, **values)

    @staticmethod
    def _process_search_argument(table, **kw):
        """
        构造select 查询的SQL语句
        @param table: 表名
        @param kw: 过滤条件
        @return:
        """
        where = ''
        if kw:
            for arg in kw.keys():
                v = kw[arg]
                if isinstance(v, str):
                    s = arg + "='%s'" % v
                else:
                    s = arg + "=%s" % v
                where = s + " " + 'and' + ' ' + where
            # clear last 'and'
            where = where[0:-4]
            sql = 'select * from %s where %s' % (table, where)
        else:
            sql = 'select * from %s' % table
        return sql

    def is_exist_records(self, index_name=None, **kwargs):
        """
        验证该条记录是否存在于实时索引中
        @param kwargs:过滤条件
        @param index_name: 索引名
        @return:
        """
        table = index_name or self.index
        assert table

        sql = self._process_search_argument(table, **kwargs)
        cursor = self._connect.cursor()
        try:
            cursor.execute(sql)
            ret = cursor.fetchone()
        except Error:
            cursor.close()
            return False
        else:
            cursor.close()   # 其实可以不用关闭游标  因为cursor对connection使用的是弱引用 不影响
            return True if ret else False

    def create_by_id(self, record_id, index_name=None, **kwargs):
        """
        与insert的区别在于 如果存在这条记录则更新否则创建
        @param record_id: 记录的id
        @param index_name: 索引名称
        @param kwargs:
        @return:
        """
        if self.is_exist_records(index_name, id=record_id):
            return self.update_by_id(record_id, index_name, **kwargs)
        else:
            kwargs.update(id=record_id)
            return self.insert(index_name, **kwargs)

    def delete_all(self, index_name=None):
        """
        清空实时索引的内容
        @param index_name:索引名称
        @return:
        """
        table = index_name or self.index
        assert table
        sql = "delete from %s where 1=1" % table
        cursor = self._connect.cursor()
        return self._execute_sql(cursor, sql)

    def get_err_info(self):
        """
        返回最后一次执行出错的原因
        @return:
        """
        return self.GetLastError()

    def query(self, query, index_name=None, comment=''):
        table = index_name or self.index
        return self.Query(query, table, comment)

if __name__ == "__main__":
    client = SphinxAPI(host="120.26.129.217", index_name="delta_ifuwo_item")
    client.ResetFilters()
    # client.delete_by_id(4)

    # client.insert(id=7, rank_score="211", render_flag="1", is_public="1", no="gdahgdhagdhagdadjagda",
    #  product_name="皇家一号")
    # print client.is_exist_records(id=4)
    # print client.update_by_id(8, product_name="更新内容")
    # print client.create_by_id(6515, rank_score=9999, render_flag=0, is_public=0,
    #  no="abc78e88002f11e5a58d00163e0026b6",
    #                           product_name="帅华 灯饰 002")
    # client.create_by_id(55, is_public=1)
    # print client.get_index_fields()
    # client.UpdateAttributes("ifuwo_ifuwoitem_rt", ["no"], {3: ["gdhjgdhagdhag"]})
    client.SetMatchMode(sphinxapi.SPH_MATCH_ALL)
    client.SetLimits(0, 100)
    # client.SetSelect("select * from ifuwo_ifuwoitem_rt")
    client.SetFilter("is_public", [1])
    # client.delete_by_id(1)
    result = client.query("增量索引")
    if result:
        print len(result["matches"])
        print result
    else:
        print client.GetLastError()
