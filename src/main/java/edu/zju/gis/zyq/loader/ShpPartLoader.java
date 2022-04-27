package edu.zju.gis.zyq.loader;

import edu.zju.gis.zyq.util.SchemaHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;
import org.gdal.gdal.gdal;
import org.gdal.ogr.DataSource;
import org.gdal.ogr.Feature;
import org.gdal.ogr.FeatureDefn;
import org.gdal.ogr.ogr;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhou Yuquan
 * @version 1.0, 2021-12-09
 */
public class ShpPartLoader {

    // 数据库设置
    private String url;
    private String username;
    private String password;
    private String schema;

    // Spark
    private SparkSession ss;
    private JavaSparkContext jsc;

    // 分块要素数量
    private int partNum;

    // 初始化
    private Boolean initialized = false;

    public void initialize(String addr, String database, String schema, String username, String password, int partNum) {
        this.url = "jdbc:postgresql://" + addr + "/" + database;
        this.schema = schema;
        this.username = username;
        this.password = password;
        this.partNum = partNum;
        SparkConf conf = new SparkConf();
        conf.set("spark.debug.maxToStringFields", "1000");
        conf.set("spark.driver.memory", "64g");
        this.ss = SparkSession.builder().config(conf).master("local[*]").appName("ShpLoader").getOrCreate();
        this.jsc = JavaSparkContext.fromSparkContext(this.ss.sparkContext());
        this.initialized = true;
    }

    public void load(String path, String layerName, String tableName) {

        if (!this.initialized) {
            System.out.println("Error: TsvLoader hasn't initialized.");
            return;
        }

        // GDAL配置
        ogr.RegisterAll();
        gdal.SetConfigOption("MDB_DRIVER_TEMPLATE", "DRIVER=Microsoft Access Driver (*.mdb, *.accdb);DBQ=%s");
        gdal.SetConfigOption("PGEO_DRIVER_TEMPLATE", "DRIVER=Microsoft Access Driver (*.mdb, *.accdb);DBQ=%s");
        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "YES");
        gdal.SetConfigOption("SHAPE_ENCODING", "UTF8");

        // 打开Shp文件
        DataSource ds = ogr.Open(path, 0);

        // 读取layer
        org.gdal.ogr.Layer player = ds.GetLayerByName(layerName);
        if (player == null) {
            System.out.printf("Invalid layer reader for shp: %s[%s]%n", path, layerName);
            return;
        }

        // 读取并设置字段信息
        FeatureDefn featureDefn = player.GetLayerDefn();
        int fieldCount = featureDefn.GetFieldCount();
        String[] colNames = new String[fieldCount + 2];
        String[] types = new String[fieldCount + 2];
        colNames[0] = "FID";
        types[0] = "Integer64";
        colNames[fieldCount + 1] = "WKT";
        types[fieldCount + 1] = "String";

        for (int i = 0; i < fieldCount; i++) {
            colNames[i + 1] = featureDefn.GetFieldDefn(i).GetName();
            types[i + 1] = featureDefn.GetFieldDefn(i).GetTypeName();
        }

        StructType structType = SchemaHelper.getSchema(colNames, types);

        int num = 0;
        List<Row> rows = new ArrayList<>();
        while (true) {
            Feature feature = player.GetNextFeature();
            if (feature == null) {
                break;
            }
            List<Object> data = new ArrayList<>();
            data.add(feature.GetFID());
            for (int i = 0; i < fieldCount; i++) {
                switch (types[i + 1]) {
                    case "Integer64":
                        data.add(feature.GetFieldAsInteger64(i));
                        break;
                    case "Real":
                        data.add(feature.GetFieldAsDouble(i));
                        break;
                    case "String":
                        data.add(feature.GetFieldAsString(i));
                        break;
                    default:
                        data.add(feature.GetFieldAsString(i));
                }
            }
            data.add(feature.GetGeometryRef().ExportToWkt());
            rows.add(new GenericRow(data.toArray()));
            num += 1;
            if (num == partNum) {
                saveToPg(rows, tableName, structType);
                num = 0;
                rows = new ArrayList<>();
            }
        }
        saveToPg(rows, tableName, structType);
    }

    private void saveToPg(List<Row> rows, String tableName, StructType structType) {
        JavaRDD<Row> rdd = this.jsc.parallelize(rows);
        Dataset<Row> df = this.ss.createDataFrame(rdd, structType);
        df.write()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", url)
                .option("dbtable", this.schema + "." + tableName)
                .option("user", username)
                .option("password", password)
                .mode(SaveMode.Append)
                .save();
    }

    public void close() {
        if (this.initialized) {
            ss.close();
            this.initialized = false;
        }
    }

}
