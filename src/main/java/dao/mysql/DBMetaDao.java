package dao.mysql;

import utils.JdbcUtils;
import model.HiveDB;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.SQLException;
import java.util.Map;

/**
 * Created by 1 on 2018/7/23.
 */
public class DBMetaDao {
    private QueryRunner qr = new QueryRunner(JdbcUtils.getDataSource_My_Hive_Meate());

    public HiveDB selectDBbyName(String database) {
        String sql = "select * from hivedb where db_name = ?";

        HiveDB hiveDB = new HiveDB();
        try {
            Map map = qr.query(sql,new MapHandler(),database);
            if (map == null) {
                return null;
            }
            hiveDB.setDb_id((Integer) map.get("db_id"));
            hiveDB.setDb_name((String) map.get("db_name"));
            hiveDB.setDb_url((String) map.get("db_url"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return hiveDB;
    }

    public int selectDbIdByName(String database) {
        String sql = "select db_id from hivedb where db_name = ?";
        int id = 0;
        try {
             id = qr.query(sql,new ScalarHandler<Integer>(),database);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public void insetDBInfo(HiveDB hiveDB) {
        String sql = "insert into hivedb values(?,?,?)";
        try {
            qr.update(sql,null,hiveDB.getDb_name(),hiveDB.getDb_url());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
