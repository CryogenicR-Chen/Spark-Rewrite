package org.rewrite;


import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.LegacyListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.apache.commons.cli.*;
import org.apache.spark.sql.connector.catalog.Identifier;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.iceberg.spark.Spark3Util.loadIcebergTable;

public class SparkRewrite {

//    private static String[] tables = new String[]{"customer","district","history","item","nation","new_order","oorder","order_line","region","stock","supplier","warehouse"};
    private static String[] tables = new String[]{"customer"};
    private static final String AMS_URL = "thrift://sloth-commerce-test2.jd.163.org:18150/";
    private static final String CATALOG = "native_iceberg_hive";
    private static final String DB = "db4511";
//    private static final String TABLE = "_iceberg";
    public static void main(String[] args) throws Exception {

        CommandLineParser parser = new DefaultParser();
        XMLConfiguration config = buildConfiguration(System.getProperty("user.dir")+"/config/option.xml");
        Options options = buildOption(config);

        CommandLine argsLine = parser.parse(options, args);

        String catalogName = argsLine.getOptionValue("c");
        String schemaName = argsLine.getOptionValue("s");
        Integer frequency = argsLine.hasOption("f") ? Integer.parseInt(argsLine.getOptionValue("f")) : -1;
        String tableName;
        long startTime = System.currentTimeMillis();
        int i = 1;
        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL Example")
                .config("spark.sql.session.timeZone", "Asia/Shanghai")
                .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
                .config("spark.sql.catalog.iceberg_catalog4", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg_catalog4.type", "hive")
                .config("spark.sql.catalog.iceberg_catalog4.uri", "thrift://hz11-trino-arctic-0.jd.163.org:9083")
                .master("local")
                .getOrCreate();
//        String sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('min-input-files','2'))", catalogName, schemaName, "sss");

        ExecutorService executorService = Executors.newFixedThreadPool(tables.length);

        while (true) {
            long startTimeTemp = System.currentTimeMillis();
            try {
                if(!argsLine.hasOption("a")){
                    tableName = argsLine.getOptionValue("t");
//                    String sql = String.format("CALL %s.system.rewrite_data_files('%s.%s')", catalogName, schemaName, tableName+"_iceberg");
                    String sql = String.format("CALL %s.system.rewrite_data_files('%s.%s')", catalogName, schemaName, tableName+"_iceberg");
                    spark.sql(sql);
                }else{
                    for(String table : tables){
                        executorService.submit(() -> {
                            try {
//                                SparkActions.get(spark).rewriteDataFiles(icebergTable).option("delete-file-threshold","1").execute();

                                String localTableName = table+"_iceberg";
//                                String sql = String.format("CALL %s.system.rewrite_data_files('%s.%s')", catalogName, schemaName, localTableName);
                                String sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('min-input-files','2'))", catalogName, schemaName, localTableName);
//                                String sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('delete-file-threshold','1'))", catalogName, schemaName, localTableName);
                                spark.sql(sql);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                    }
                }
                executorService.shutdown();
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

                if(frequency == -1) break;
                Thread.sleep(frequency * 1000);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            executorService = Executors.newFixedThreadPool(tables.length);
            long endTimeTemp = System.currentTimeMillis();
            long durationTemp = endTimeTemp - startTimeTemp;
            System.out.println(i + " execution time: " + durationTemp + " milliseconds");
            i++;
        }
        spark.stop();
        long endTime = System.currentTimeMillis();

        long duration = endTime - startTime;
        System.out.println("Total execution time: " + duration + " milliseconds");

    }
    public static void icebergRollBack(){

    }
    public static void getIcebergFileInfo(SparkSession spark) throws Exception {
        String warehouseLocation = "/path/to/your/warehouse";
        String dbName = "your_database";
        String tableName = "your_table";
        String resultFilePath = "/path/to/result.txt";


        HiveCatalog catalog = new HiveCatalog();
        catalog.setConf(spark.sparkContext().hadoopConfiguration());  // Configure using Spark's Hadoop configuration

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("warehouse", "hdfs://hz11-trino-arctic-0.jd.163.org:8020/user/warehouse");
        properties.put("uri", "thrift://hz11-trino-arctic-0.jd.163.org:9083");

        catalog.initialize("hive", properties);
        Table table = catalog.loadTable(TableIdentifier.of(dbName, tableName));


        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resultFilePath))) {
            // 获取表大小
            String tableSizeInBytes = table.currentSnapshot().summary().getOrDefault("total-data-size", "0");
            writer.write("Table size: " + tableSizeInBytes + " bytes\n");

            // 获取数据文件数量
            TableScan scan = table.newScan().filter(Expressions.alwaysTrue());
            long dataFileCount = 0L;
            for (FileScanTask task : scan.planFiles()) {
                dataFileCount++;
            }
            writer.write("Number of data files: " + dataFileCount + "\n");

            // 获取行数
            long recordCount = 0L;
            for (FileScanTask task : scan.planFiles()) {
                recordCount += task.file().recordCount();
            }
            writer.write("Number of records: " + recordCount + "\n");

            // 获取eq-delete files、pos-delete files的数量
            long eqDeleteFileCount = 0L;
            long posDeleteFileCount = 0L;

            writer.write("Number of eq-delete files: " + eqDeleteFileCount + "\n");
            writer.write("Number of pos-delete files: " + posDeleteFileCount + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Options  buildOption(XMLConfiguration config){
        Options options = new Options();
        List<HierarchicalConfiguration<ImmutableNode>> optionConfs = config.configurationsAt("option");

        for (HierarchicalConfiguration<ImmutableNode> optionConf : optionConfs) {
            String shortOpt = optionConf.getString("short");
            String longOpt = optionConf.getString("long");
            String description = optionConf.getString("description");
            boolean required = optionConf.getBoolean("required");
            boolean hasArg = optionConf.getBoolean("hasarg");
            options.addOption(Option.builder(shortOpt)
                    .longOpt(longOpt)
                    .desc(description)
                    .required(required)
                    .hasArg(hasArg)
                    .build());
        }
        return options;
    }
    private static XMLConfiguration buildConfiguration(String filename) throws ConfigurationException {

        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder = new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
                .configure(params.xml()
                        .setFileName(filename)
                        .setListDelimiterHandler(new LegacyListDelimiterHandler(','))
                        .setExpressionEngine(new XPathExpressionEngine()));
        return builder.getConfiguration();

    }
}