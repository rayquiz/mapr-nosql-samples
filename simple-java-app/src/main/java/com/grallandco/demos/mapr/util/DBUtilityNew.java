package com.grallandco.demos.mapr.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Created by tgrall on 04/01/16.
 */
public class DBUtilityNew {

    private static Configuration configuration = null;

    static {
        // Create the configuration
        configuration = HBaseConfiguration.create();
    }

    // /**
    // * Create and return the table with specific column families
    // *
    // * @param tableName if the name starts with "/" is will be a MapRDB table if not an HBase table
    // * @param columnFamilies list of CF names
    // * @return the HTable itself
    // */
    // public static void createTable(final String tableName, final String[] columnFamilies) throws
    // IOException {
    // final TableName table = TableName.valueOf(tableName);
    // try (Connection connection = ConnectionFactory.createConnection(configuration);
    // Admin admin = connection.getAdmin()) {
    // if (admin.tableExists(table)) {
    // System.out.println("Table " + tableName + " exists");
    // } else {
    // final HTableDescriptor tableDescriptor = new HTableDescriptor(table);
    // for (int i = 0; i < columnFamilies.length; i++) {
    // tableDescriptor.addFamily(new HColumnDescriptor(columnFamilies[i]));
    // }
    // admin.createTable(tableDescriptor);
    // System.out.println("Table " + tableName + " created");
    // }
    // }
    //
    // }
    //
    // /**
    // * Delete the table passed as parameter
    // *
    // * @param tableName
    // */
    // public static void deleteTable(final String tableName) throws IOException {
    // final TableName table = TableName.valueOf(tableName);
    // try (Connection connection = ConnectionFactory.createConnection(configuration);
    // Admin admin = connection.getAdmin()) {
    // if (admin.tableExists(table)) {
    // admin.deleteTable(table);
    // System.out.println("Table " + tableName + " deleted");
    // } else {
    // System.out.println("Table " + tableName + " does not exists");
    // }
    // }
    //
    // }
    //
    // /**
    // * Insert/Update a new value
    // *
    // * @param tableName
    // * @param rowkey
    // * @param columnFamily
    // * @param column
    // * @param value support only String for now
    // * @throws IOException
    // */
    // public static void put(final String tableName, final String rowkey, final String columnFamily,
    // final String column, final String value) throws IOException {
    //
    // try (Connection connection = ConnectionFactory.createConnection(configuration)) {
    // final Table table = connection.getTable(TableName.valueOf(tableName));
    //
    // final Put put = new Put(Bytes.toBytes(rowkey));
    // put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
    // table.put(put);
    // System.out.println(
    // "Row/Col Inserted : " + rowkey + ":" + columnFamily + ":" + column + ":" + value);
    // }
    // }
    //
    // /**
    // * Delete a row
    // *
    // * @param tableName
    // * @param rowkey
    // * @throws IOException
    // */
    // public static void delete(final String tableName, final String rowkey) throws IOException {
    // try (Connection connection = ConnectionFactory.createConnection(configuration)) {
    // final Table table = connection.getTable(TableName.valueOf(tableName));
    // final Delete delete = new Delete(Bytes.toBytes(rowkey));
    // table.delete(delete);
    // System.out.println("Row Deleted: " + rowkey);
    // }
    // }
    //
    // /**
    // * Get a single row as Map using multi key (CF,C)
    // *
    // * @param tableName
    // * @param rowkey
    // * @return
    // * @throws IOException
    // */
    // public static Map<String, String> getRow(final String tableName, final String rowkey) throws
    // IOException {
    // try (Connection connection = ConnectionFactory.createConnection(configuration)) {
    // final Table table = connection.getTable(TableName.valueOf(tableName));
    // final Get get = new Get(Bytes.toBytes(rowkey));
    // final Result result = table.get(get);
    // return getResultAsMap(result);
    // }
    // }
    //
    // /**
    // *
    // * @param tableName
    // * @return
    // * @throws IOException
    // */
    // public static Map<String, Map<String, String>> scan(final String tableName) throws IOException {
    // final Map<String, Map<String, String>> returnValue = new TreeMap();
    // try (Connection connection = ConnectionFactory.createConnection(configuration)) {
    // final Table table = connection.getTable(TableName.valueOf(tableName));
    // final Scan scan = new Scan();
    // final ResultScanner scanner = table.getScanner(scan);
    // for (final Result result : scanner) {
    // returnValue.put(Bytes.toString(result.getRow()), getResultAsMap(result));
    // }
    // return returnValue;
    // }
    // }
    //
    // /**
    // *
    // * @param result
    // * @return
    // */
    // private static Map<String, String> getResultAsMap(final Result result) {
    // final Map<String, String> resultAsMap = new HashMap<String, String>();
    // for (final Cell cell : result.rawCells()) {
    // final String columnFamily = new String(CellUtil.cloneFamily(cell));
    // final String column = new String(CellUtil.cloneQualifier(cell));
    // final String value = new String(CellUtil.cloneValue(cell));
    // resultAsMap.put(columnFamily + "|" + column, value);
    // }
    // return resultAsMap;
    // }

}
