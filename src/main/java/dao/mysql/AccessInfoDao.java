package dao.mysql;

import utils.JdbcUtils;
import model.AccessInfo;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.SQLException;

/**
 * Created by 1 on 2018/7/24.
 */
public class AccessInfoDao {
    private QueryRunner qr = new QueryRunner(JdbcUtils.getDataSource_My_Hive_Meate());


    public void insertAccessInfo(AccessInfo ai) {
        String sql = "insert into access_info values(?,?,?)";
        try {
            qr.update(sql,ai.getHiveTable().getId(),ai.getAccess_time(),ai.getUsername());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public int countAccessInfo(AccessInfo ai) {
        String sql = "select count(*) from access_info where t_id = ?";
        try {
            Number number = (Number) qr.query(sql, new ScalarHandler(),ai.getHiveTable().getId());
            return number.intValue();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteEarliestRecord(AccessInfo ai) {
        //删除第一条
        String sql = "delete from access_info where t_id = ? limit 1";
        try {
            qr.update(sql,ai.getHiveTable().getId());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
