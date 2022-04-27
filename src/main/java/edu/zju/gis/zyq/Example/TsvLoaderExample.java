package edu.zju.gis.zyq.Example;

import edu.zju.gis.zyq.loader.TsvLoader;
import edu.zju.gis.zyq.util.PGHelper;

public class TsvLoaderExample {

    public static void main(String[] args) {
        // pg地址 10.79.231.84:5432
        String addr = args[0];
        // pg数据库
        String database = args[1];
        // pg模式
        String schema = args[2];
        // pg用户名密码
        String username = args[3];
        String password = args[4];
        // 数据表名
        String tableName = args[5];
        // 数据路径
        String path = args[6];
        // 是否有表头
        String header = "false";
        // 列名
        String[] colNames = args[7].split(",");
        // 字段类型
        String[] types = args[8].split(",");
        // 数据坐标系
        String crs = args[9];

        TsvLoader tsvLoader = new TsvLoader();
        tsvLoader.initialize(addr, database, schema, username, password);
        tsvLoader.load(path, tableName, header, colNames, types);
        tsvLoader.close();

        // 创建几何字段
        PGHelper pgHelper = new PGHelper("jdbc:postgresql://" + addr + "/" + database, username, password);
        pgHelper.run(String.format("select addgeometrycolumn('vector', '%s', 'geom3857', 3857, 'GEOMETRY', 2)", tableName));
        pgHelper.run(String.format("update vector.%s set geom3857 = st_transform(st_geomfromtext(\"WKT\", %s), 3857)", tableName, crs));
        pgHelper.run(String.format("select addgeometrycolumn('vector', '%s', 'geom4326', 4326, 'GEOMETRY', 2)", tableName));
        pgHelper.run(String.format("update vector.%s set geom4326 = st_transform(st_geomfromtext(\"WKT\", %s), 4326)", tableName, crs));
    }

}
