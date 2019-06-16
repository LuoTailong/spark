package com.itheima.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;


public class TestHbase {
    public static void main(String[] args) throws Exception {
        //HBase的两种连接方式：
        // 1读取配置文件 只需要配置zookeeper
        /*Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);*/

        //2 通过代码配置  每次需要改代码不建议
        /*Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        connection = ConnectionFactory.createConnection();*/

        // 1.连接HBase
        // 1.1 HBaseConfiguration.create();
        Configuration config = HBaseConfiguration.create();
        // 1.2 创建一个连接
        Connection connection = ConnectionFactory.createConnection(config);
        // 1.3 从连接中获得一个Admin对象
        Admin admin = connection.getAdmin();
        // 2.创建表
        // 2.1 判断表是否存在
        TableName tableName = TableName.valueOf("user");
        if (!admin.tableExists(tableName)) {
            //2.2 如果表不存在就创建一个表
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor("base_info"));
            admin.createTable(hTableDescriptor);
            System.out.println("创建表");
        }

        System.out.println("-------------------打印表的描述信息------------------------");
        // 1.定义表的名称
        TableName tableName1 = TableName.valueOf("user");
        // 2.获取表
        Table table1 = connection.getTable(tableName1);
        // 3.获取表的描述信息
        HTableDescriptor tableDescriptor = table1.getTableDescriptor();
        // 4.获取表的列簇信息
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        for (HColumnDescriptor columnFamily : columnFamilies) {
            // 5.获取表的columFamily的字节数组
            byte[] name = columnFamily.getName();
            // 6.使用hbase自带的bytes工具类转成string
            String value = Bytes.toString(name);
            // 7.打印
            System.out.println(value);
        }

        System.out.println("-------------------添加数据(PUT)------------------------");
        // 1.定义表的名称
        TableName tableName2 = TableName.valueOf("user");
        // 2.获取表
        Table table2 = connection.getTable(tableName2);
        // 3.准备数据
        String rowKey2 = "rowkey_10";
        Put zhangsan = new Put(Bytes.toBytes(rowKey2));
        zhangsan.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("张三"));
        zhangsan.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("sex"), Bytes.toBytes("1"));
        zhangsan.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("address"), Bytes.toBytes("北京市"));
        zhangsan.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("birthday"), Bytes.toBytes("2014-07-10"));
        // 4. 添加数据
        table2.put(zhangsan);
        table2.close();

        System.out.println("-------------------获取数据(Get)------------------------");
        // 1.定义表的名称
        TableName tableName3 = TableName.valueOf("user");
        // 2.获取表
        Table table3 = connection.getTable(tableName3);
        // 3.准备数据
        String rowKey3 = "rowkey_10";
        // 4.拼装查询条件
        Get get = new Get(Bytes.toBytes(rowKey3));
        // 5.查询数据
        Result result = table3.get(get);
        // 6.打印数据 获取所有的单元格
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            // 打印rowkey,family,qualifier,value
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                    + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                    + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
        }

        System.out.println("-------------------全盘扫描(Scan)-----------------------");
        // 1.定义表的名称
        TableName tableName4 = TableName.valueOf("user");
        // 2.获取表
        Table table4 = connection.getTable(tableName4);
        // 3.全表扫描
        Scan scan = new Scan();
        // 定义上下界
        scan.setStartRow(Bytes.toBytes("rowkey_10"));
        scan.setStopRow(Bytes.toBytes("rowkey_20"));
        // 4.获取扫描结果
        ResultScanner scanner = table4.getScanner(scan);
        Result result4 = null;
        // 5. 迭代数据
        while ((result4 = scanner.next()) != null) {
            // 6.打印数据 获取所有的单元格
            List<Cell> cells4 = result4.listCells();
            for (Cell cell : cells4) {
                // 打印rowkey,family,qualifier,value
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                        + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
            }
        }

        System.out.println("-------------------过滤查询-----------------------");
        //1、创建过滤器
        ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("张三".getBytes()));
        //2、创建扫描器
        Scan scan5 = new Scan();
        //3、将过滤器设置到扫描器中
        scan5.setFilter(filter);
        //4、获取HBase的表
        Table table5 = connection.getTable(TableName.valueOf("user"));
        //5、扫描HBase的表（注意过滤操作是在服务器进行的，也即是在regionServer进行的）
        ResultScanner scanner5 = table5.getScanner(scan5);
        for (Result result5 : scanner5) {
            // 6.打印数据 获取所有的单元格
            List<Cell> cells5 = result5.listCells();
            for (Cell cell : cells5) {
                // 打印rowkey,family,qualifier,value
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                        + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
            }
        }
    }
}
