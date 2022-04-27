package edu.zju.gis.zyq.loader;

import edu.zju.gis.zyq.util.SchemaHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class TsvLoader {

    // 数据库设置
    private String url;
    private String username;
    private String password;
    private String schema;

    // Spark
    private SparkSession ss;

    // 初始化
    private Boolean initialized = false;

    public void initialize(String addr, String database, String schema, String username, String password) {
        this.url = "jdbc:postgresql://" + addr + "/" + database;
        this.schema = schema;
        this.username = username;
        this.password = password;
        SparkConf conf = new SparkConf();
        conf.set("spark.debug.maxToStringFields", "1000");
        this.ss = SparkSession.builder().config(new SparkConf()).master("local[*]").appName("TsvLoader").getOrCreate();
        this.initialized = true;
    }

    public void load(String path, String tableName, String header, String[] colNames, String[] types) {

        if (!this.initialized) {
            System.out.println("Error: TsvLoader hasn't initialized.");
            return;
        }

        Dataset<Row> df = ss.read()
                .format("csv")
                .option("header", header)
                .option("path", path)
                .option("sep", "\t")
                .option("encoding", "utf-8")
                .schema(SchemaHelper.getSchema(colNames, types))
                .load();

        df.write()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", url)
                .option("dbtable", schema + "." + tableName)
                .option("user", username)
                .option("password", password)
                .mode(SaveMode.Overwrite)
                .save();

    }

    public void close() {
        if (this.initialized) {
            ss.close();
            this.initialized = false;
        }
    }

}
