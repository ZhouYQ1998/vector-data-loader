package edu.zju.gis.zyq.Example;

import edu.zju.gis.zyq.loader.GdbPartLoader;
import edu.zju.gis.zyq.util.PGHelper;

/**
 * @author Zhou Yuquan
 * @version 1.0, 2021-12-09
 */
public class GdbPartLoaderExample {

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
        // 图层名
        String layerName = args[7];
        // 数据坐标系
        String crs = args[8];
        // 分块要素数量
        int partNum = Integer.parseInt(args[9]);

        GdbPartLoader gdbPartLoader = new GdbPartLoader();
        gdbPartLoader.initialize(addr, database, schema, username, password, partNum);
        gdbPartLoader.load(path, layerName, tableName);
        gdbPartLoader.close();

        // 创建几何字段
        PGHelper pgHelper = new PGHelper("jdbc:postgresql://" + addr + "/" + database, username, password);
        pgHelper.run(String.format("select addgeometrycolumn('vector', '%s', 'geom3857', 3857, 'GEOMETRY', 2)", tableName));
        pgHelper.run(String.format("update vector.%s set geom3857 = st_transform(st_geomfromtext(\"WKT\", %s), 3857)", tableName, crs));
        pgHelper.run(String.format("select addgeometrycolumn('vector', '%s', 'geom4326', 4326, 'GEOMETRY', 2)", tableName));
        pgHelper.run(String.format("update vector.%s set geom4326 = st_transform(st_geomfromtext(\"WKT\", %s), 4326)", tableName, crs));
    }
}
