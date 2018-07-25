package utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;

/**
 * Created by 1 on 2018/7/20.
 */
public class JdbcUtils {

    public static DataSource getDataSource_HiveMeta() {
        return new ComboPooledDataSource("hive_meta");
    }

    public static DataSource getDataSource_My_Hive_Meate() {

        return new ComboPooledDataSource("mysql");
    }

    public static DataSource getHive() {
        return new ComboPooledDataSource("hive");
    }
}
