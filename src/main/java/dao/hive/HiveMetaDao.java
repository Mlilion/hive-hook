package dao.hive;

import utils.JdbcUtils;
import model.HiveDB;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.SQLException;
import java.util.Map;

/**
 * 可以查询hive的mysql元数据库的DAO对象
 * Created by 1 on 2018/7/23.
 */
public class HiveMetaDao {
    private QueryRunner qr = new QueryRunner(JdbcUtils.getDataSource_HiveMeta());

    /**
     * 根据表id在hive的mysql元数据库中查询该表的创建人
     * @param id
     * @return
     */
    public String queryOwnerName(int id) {
        String sql = "select OWNER from TBLS where TBL_ID = ?";
        String name = null;
        try {
            name = qr.query(sql,new ScalarHandler<String>(),id);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return name;
    }
}
