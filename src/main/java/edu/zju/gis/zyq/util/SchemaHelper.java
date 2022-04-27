package edu.zju.gis.zyq.util;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhou Yuquan
 * @version 1.0, 2021-11-20
 */
public class SchemaHelper {

    public static StructType getSchema(String[] colNames, String[] types) {
        List<StructField> inputFields = new ArrayList<>();
        for (int i = 0; i < colNames.length; i++) {
            DataType t = DataTypes.StringType;
            switch (types[i]) {
                case "str":
                case "String":
                    t = DataTypes.StringType;
                    break;
                case "int":
                    t = DataTypes.IntegerType;
                    break;
                case "Integer64":
                    t = DataTypes.LongType;
                    break;
                case "double":
                case "Real":
                    t = DataTypes.DoubleType;
                    break;
            }
            inputFields.add(DataTypes.createStructField(colNames[i], t, true));
        }
        return DataTypes.createStructType(inputFields);
    }

}
