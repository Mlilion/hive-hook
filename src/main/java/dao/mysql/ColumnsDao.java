package dao.mysql;

import utils.JdbcUtils;
import model.TableColumn;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.SQLException;

/**
 * Created by 1 on 2018/7/24.
 */
public class ColumnsDao {
    private QueryRunner qr = new QueryRunner(JdbcUtils.getDataSource_My_Hive_Meate());

    public void insertColumn(TableColumn c) {
        String sql = "insert into columns values(?,?,?,?)";
        try {
            qr.update(sql, c.getHiveTable().getId(), c.getComment(), c.getColumn_name(), c.getType_name());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
