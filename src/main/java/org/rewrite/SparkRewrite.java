/*     */ package org.rewrite;
/*     */ 
/*     */ import java.io.BufferedWriter;
/*     */ import java.io.FileWriter;
/*     */ import java.io.IOException;
/*     */ import java.util.HashMap;
/*     */ import java.util.List;
/*     */ import java.util.Map;
/*     */ import java.util.concurrent.ExecutorService;
/*     */ import java.util.concurrent.Executors;
/*     */ import java.util.concurrent.TimeUnit;
/*     */ import org.apache.commons.cli.CommandLine;
/*     */ import org.apache.commons.cli.DefaultParser;
/*     */ import org.apache.commons.cli.Option;
/*     */ import org.apache.commons.cli.Options;
/*     */ import org.apache.commons.configuration2.HierarchicalConfiguration;
/*     */ import org.apache.commons.configuration2.XMLConfiguration;
/*     */ import org.apache.commons.configuration2.builder.BuilderParameters;
/*     */ import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
/*     */ import org.apache.commons.configuration2.builder.fluent.Parameters;
/*     */ import org.apache.commons.configuration2.builder.fluent.XMLBuilderParameters;
/*     */ import org.apache.commons.configuration2.convert.LegacyListDelimiterHandler;
/*     */ import org.apache.commons.configuration2.convert.ListDelimiterHandler;
/*     */ import org.apache.commons.configuration2.ex.ConfigurationException;
/*     */ import org.apache.commons.configuration2.tree.ExpressionEngine;
/*     */ import org.apache.commons.configuration2.tree.ImmutableNode;
/*     */ import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
/*     */ import org.apache.iceberg.FileScanTask;
/*     */ import org.apache.iceberg.Table;
/*     */ import org.apache.iceberg.TableScan;
/*     */ import org.apache.iceberg.catalog.TableIdentifier;
/*     */ import org.apache.iceberg.expressions.Expression;
/*     */ import org.apache.iceberg.expressions.Expressions;
/*     */ import org.apache.iceberg.hive.HiveCatalog;
/*     */ import org.apache.iceberg.io.CloseableIterator;
/*     */ import org.apache.spark.sql.SparkSession;
/*     */ 
/*     */ public class SparkRewrite {
/*  38 */   private static String[] tables = new String[] { 
/*  38 */       "customer", "district", "history", "item", "nation", "new_order", "oorder", "order_line", "region", "stock", 
/*  38 */       "supplier", "warehouse" };
/*     */   
/*     */   public static void main(String[] args) throws Exception {
/*  41 */     DefaultParser defaultParser = new DefaultParser();
/*  42 */     XMLConfiguration config = buildConfiguration(System.getProperty("user.dir") + "/config/option.xml");
/*  43 */     Options options = buildOption(config);
/*  45 */     CommandLine argsLine = defaultParser.parse(options, args);
/*  50 */     SparkSession spark = SparkSession.builder().appName("Spark SQL Example").config("spark.sql.session.timeZone", "Asia/Shanghai").config("spark.sql.iceberg.handle-timestamp-without-timezone", "true").getOrCreate();
/*  51 */     String catalogName = argsLine.getOptionValue("c");
/*  52 */     String schemaName = argsLine.getOptionValue("s");
/*  53 */     Integer frequency = Integer.valueOf(argsLine.hasOption("f") ? Integer.parseInt(argsLine.getOptionValue("f")) : -1);
/*  55 */     long startTime = System.currentTimeMillis();
/*  58 */     ExecutorService executorService = Executors.newFixedThreadPool(tables.length);
/*     */     while (true) {
/*     */       try {
/*  62 */         if (!argsLine.hasOption("a")) {
/*  63 */           String tableName = argsLine.getOptionValue("t");
/*  64 */           String sql = String.format("CALL %s.system.rewrite_data_files('%s.%s')", new Object[] { catalogName, schemaName, tableName + "_iceberg" });
/*  65 */           spark.sql(sql);
/*     */         } else {
/*  67 */           for (String table : tables) {
/*  68 */             executorService.submit(() -> {
/*     */                   try {
/*     */                     String localTableName = table + "_iceberg";
/*     */                     String sql = String.format("CALL %s.system.rewrite_data_files('%s.%s')", new Object[] { catalogName, schemaName, localTableName });
/*     */                     spark.sql(sql);
/*  73 */                   } catch (Exception e) {
/*     */                     e.printStackTrace();
/*     */                   } 
/*     */                 });
/*     */           } 
/*     */         } 
/*  79 */         executorService.shutdown();
/*  80 */         executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
/*  82 */         if (frequency.intValue() == -1)
/*     */           break; 
/*  83 */         Thread.sleep((frequency.intValue() * 1000));
/*  85 */       } catch (InterruptedException e) {
/*  86 */         e.printStackTrace();
/*     */       } 
/*  88 */       executorService = Executors.newFixedThreadPool(tables.length);
/*     */     } 
/*  90 */     spark.stop();
/*  91 */     long endTime = System.currentTimeMillis();
/*  93 */     long duration = endTime - startTime;
/*  94 */     System.out.println("Total execution time: " + duration + " milliseconds");
/*     */   }
/*     */   
/*     */   public static void getIcebergFileInfo(SparkSession spark) throws Exception {
/*  99 */     String warehouseLocation = "/path/to/your/warehouse";
/* 100 */     String dbName = "your_database";
/* 101 */     String tableName = "your_table";
/* 102 */     String resultFilePath = "/path/to/result.txt";
/* 105 */     HiveCatalog catalog = new HiveCatalog();
/* 106 */     catalog.setConf(spark.sparkContext().hadoopConfiguration());
/* 108 */     Map<String, String> properties = new HashMap<>();
/* 109 */     properties.put("warehouse", "hdfs://hz11-trino-arctic-0.jd.163.org:8020/user/warehouse");
/* 110 */     properties.put("uri", "thrift://hz11-trino-arctic-0.jd.163.org:9083");
/* 112 */     catalog.initialize("hive", properties);
/* 113 */     Table table = catalog.loadTable(TableIdentifier.of(new String[] { dbName, tableName }));
/* 116 */     try (BufferedWriter writer = new BufferedWriter(new FileWriter(resultFilePath))) {
/* 118 */       String tableSizeInBytes = (String)table.currentSnapshot().summary().getOrDefault("total-data-size", "0");
/* 119 */       writer.write("Table size: " + tableSizeInBytes + " bytes\n");
/* 122 */       TableScan scan = table.newScan().filter((Expression)Expressions.alwaysTrue());
/* 123 */       long dataFileCount = 0L;
/* 124 */       for (CloseableIterator<FileScanTask> closeableIterator1 = scan.planFiles().iterator(); closeableIterator1.hasNext(); ) {
/* 124 */         FileScanTask task = closeableIterator1.next();
/* 125 */         dataFileCount++;
/*     */       } 
/* 127 */       writer.write("Number of data files: " + dataFileCount + "\n");
/* 130 */       long recordCount = 0L;
/* 131 */       for (CloseableIterator<FileScanTask> closeableIterator2 = scan.planFiles().iterator(); closeableIterator2.hasNext(); ) {
/* 131 */         FileScanTask task = closeableIterator2.next();
/* 132 */         recordCount += task.file().recordCount();
/*     */       } 
/* 134 */       writer.write("Number of records: " + recordCount + "\n");
/* 137 */       long eqDeleteFileCount = 0L;
/* 138 */       long posDeleteFileCount = 0L;
/* 140 */       writer.write("Number of eq-delete files: " + eqDeleteFileCount + "\n");
/* 141 */       writer.write("Number of pos-delete files: " + posDeleteFileCount + "\n");
/* 142 */     } catch (IOException e) {
/* 143 */       e.printStackTrace();
/*     */     } 
/*     */   }
/*     */   
/*     */   public static Options buildOption(XMLConfiguration config) {
/* 147 */     Options options = new Options();
/* 148 */     List<HierarchicalConfiguration<ImmutableNode>> optionConfs = config.configurationsAt("option");
/* 150 */     for (HierarchicalConfiguration<ImmutableNode> optionConf : optionConfs) {
/* 151 */       String shortOpt = optionConf.getString("short");
/* 152 */       String longOpt = optionConf.getString("long");
/* 153 */       String description = optionConf.getString("description");
/* 154 */       boolean required = optionConf.getBoolean("required");
/* 155 */       boolean hasArg = optionConf.getBoolean("hasarg");
/* 156 */       options.addOption(Option.builder(shortOpt)
/* 157 */           .longOpt(longOpt)
/* 158 */           .desc(description)
/* 159 */           .required(required)
/* 160 */           .hasArg(hasArg)
/* 161 */           .build());
/*     */     } 
/* 163 */     return options;
/*     */   }
/*     */   
/*     */   private static XMLConfiguration buildConfiguration(String filename) throws ConfigurationException {
/* 167 */     Parameters params = new Parameters();
/* 169 */     FileBasedConfigurationBuilder<XMLConfiguration> builder = (new FileBasedConfigurationBuilder(XMLConfiguration.class)).configure(new BuilderParameters[] { (BuilderParameters)((XMLBuilderParameters)((XMLBuilderParameters)params.xml()
/* 170 */           .setFileName(filename))
/* 171 */           .setListDelimiterHandler((ListDelimiterHandler)new LegacyListDelimiterHandler(',')))
/* 172 */           .setExpressionEngine((ExpressionEngine)new XPathExpressionEngine()) });
/* 173 */     return (XMLConfiguration)builder.getConfiguration();
/*     */   }
/*     */ }


/* Location:              D:\spark-rewrite\target\spark-rewrite-1.0-SNAPSHOT2.jar!\org\rewrite\SparkRewrite.class
 * Java compiler version: 8 (52.0)
 * JD-Core Version:       1.1.3
 */