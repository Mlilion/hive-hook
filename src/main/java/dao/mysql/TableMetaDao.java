package dao.mysql;

import utils.JdbcUtils;
import model.HiveTable;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.SQLException;

/**
 * Created by 1 on 2018/7/23.
 */
public class TableMetaDao {
    private QueryRunner qr = new QueryRunner(JdbcUtils.getDataSource_My_Hive_Meate());

    public void insertTable(HiveTable hiveTable) {
        String sql = "insert into hivetable values(?,?,?,?,?,?,?,?)";
        Object[] params = {null, hiveTable.getName(), hiveTable.getDb_id(),
                hiveTable.getCreate_time(), hiveTable.getOwner(),
                hiveTable.getType(), hiveTable.getSource(), hiveTable.getComment()};
        try {
            qr.update(sql, params);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public HiveTable queryTableByName(String tableName, int db_id) {
        String sql = "select * from hivetable where name = ? && db_id = ?";
        HiveTable table = null;
        try {
            table = qr.query(sql, new BeanHandler<HiveTable>(HiveTable.class), tableName, db_id);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return table;
    }

    public void insertLineageInfo(int f_id, int c_id) {
        String sql = "insert into lineage_info values(?,?)";
        try {
            qr.update(sql, f_id, c_id);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public int queryTableIDByName(String tableName, int db_id) {
        String sql = "select * from hivetable where name = ? && db_id = ?";
        int id = 0;
        try {
            id = qr.query(sql, new ScalarHandler<Integer>(), tableName, db_id);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public void deleteTable(String dropTableName, int db_id) {
        String sql = "delete from hivetable where name = ? && db_id = ?";
        try {
            qr.update(sql,dropTableName,db_id);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
