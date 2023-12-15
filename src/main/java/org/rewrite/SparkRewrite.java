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

    private static String[] tables = new String[]{"customer","district","history","item","nation","new_order","oorder","order_line","region","stock","supplier","warehouse"};
//    private static String[] tables = new String[]{"customer"};
    private static final String AMS_URL = "thrift://sloth-commerce-test2.jd.163.org:18150/";
    private static String CATALOG = "native_iceberg_hive";
    private static String DB = "db4511";
    private static ExecutorService executorService = Executors.newFixedThreadPool(tables.length);
    private static SparkSession spark;
    public static void main(String[] args) throws Exception {

        CommandLineParser parser = new DefaultParser();
        XMLConfiguration config = buildConfiguration(System.getProperty("user.dir")+"/config/option.xml");
        Options options = buildOption(config);
        CommandLine argsLine = parser.parse(options, args);

        CATALOG = argsLine.getOptionValue("c");
        DB = argsLine.getOptionValue("s");
        spark = SparkSession.builder()
                .appName("Spark SQL Example")
                .config("spark.sql.session.timeZone", "Asia/Shanghai")
                .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
                .getOrCreate();

        String method = argsLine.getOptionValue("m");
        if(method.equals("rollback")){
            rollBack(argsLine);
        } else if (method.equals("rewrite")) {
            rewrite(argsLine);
        }


    }
    public static void rollBack(CommandLine argsLine){
        String time = argsLine.getOptionValue("st");
        for(String table : tables){
            executorService.submit(() -> {
                try {
                    String localTableName = table+"_iceberg";
                    String sql = String.format("CALL %s.system.rollback_to_timestamp('%s.%s', TIMESTAMP '%s')", CATALOG, DB, localTableName,time);
                    spark.sql(sql);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        try {
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        spark.stop();
    }
    public static void rewrite(CommandLine argsLine){
        Integer frequency = argsLine.hasOption("f") ? Integer.parseInt(argsLine.getOptionValue("f")) : -1;
        String param = argsLine.getOptionValue("p");
        String tableName;
        long startTime = System.currentTimeMillis();
        int i = 1;
        while (true) {
            long startTimeTemp = System.currentTimeMillis();
            try {
                if(!argsLine.hasOption("a")){
                    tableName = argsLine.getOptionValue("t");
                    tableName = tableName + "_iceberg";
                    String sql = null;
                    sql = String.format("REFRESH TABLE %s.%s.%s", CATALOG, DB, tableName);
                    spark.sql(sql);
                    if(param.equals("0")){
                        sql = String.format("CALL %s.system.rewrite_data_files('%s.%s')", CATALOG, DB, tableName);
                    }else if(param.equals("1")){
                        sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('min-input-files','2','rewrite-all','true'))", CATALOG, DB, tableName);
                    }else if(param.equals("2")){
                        sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('min-input-files','2','rewrite-all','false'))", CATALOG, DB, tableName);
                    }else if(param.equals("3")){
                        sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('delete-file-threshold','5'))", CATALOG, DB, tableName);
                    }else if(param.equals("4")){
                        sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('delete-file-threshold','5','rewrite-all','true'))", CATALOG, DB, tableName);
                    }else if(param.equals("5")){
                        sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('rewrite-all','true'))", CATALOG, DB, tableName);
                    }
                    spark.sql(sql);
                }else{
                    for(String table : tables){
                        executorService.submit(() -> {
                            try {
//                                SparkActions.get(spark).rewriteDataFiles(icebergTable).option("delete-file-threshold","1").execute();
                                String localTableName = table+"_iceberg";
                                String sql = null;
                                sql = String.format("REFRESH TABLE %s.%s.%s", CATALOG, DB, localTableName);
                                spark.sql(sql);
                                if(param.equals("0")){
                                    sql = String.format("CALL %s.system.rewrite_data_files('%s.%s')", CATALOG, DB, localTableName);
                                }else if(param.equals("1")){
                                    sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('min-input-files','2','rewrite-all','true'))", CATALOG, DB, localTableName);
                                }else if(param.equals("2")){
                                        sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('min-input-files','2','rewrite-all','false'))", CATALOG, DB, localTableName);
                                }else if(param.equals("3")){
                                    sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('delete-file-threshold','5'))", CATALOG, DB, localTableName);
                                }else if(param.equals("4")){
                                    sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('delete-file-threshold','5','rewrite-all','true'))", CATALOG, DB, localTableName);
                                }else if(param.equals("5")){
                                    sql = String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('rewrite-all','true'))", CATALOG, DB, localTableName);
                                }
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