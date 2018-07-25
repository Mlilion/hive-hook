package dao.hive;

import utils.JdbcUtils;
import model.HiveTable;
import model.TableColumn;

import javax.sql.DataSource;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 通过beeline jdbc连接hive的DAO
 * 用于补全元hive表的数据信息
 * Created by 1 on 2018/7/20.
 */
public class HiveDao {
    private DataSource dataSource = JdbcUtils.getHive();

    public HiveDao() {
    }


    /**
     * 从利用hiveJDBC补全hiveTable信息
     * REMARKS:表的描述
     * table_type：表类型
     * @return
     */
    public HiveTable fillTableInfo(HiveTable hiveTable) {
        DatabaseMetaData dbmd = null;

        try {
            dbmd = dataSource.getConnection().getMetaData();
            ResultSet tablers = dbmd.getTables(null,"%",hiveTable.getName(),new String[]{"TABLE"});
            tablers.next();

            hiveTable.setComment(tablers.getString("REMARKS"));
            hiveTable.setType(tablers.getString("table_type"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return hiveTable;
    }

    public List<TableColumn> getColumns(String tableName, HiveTable hiveTable) {
        DatabaseMetaData dbmd = null;
        ArrayList<TableColumn> list = new ArrayList<TableColumn>();
        try {
            dbmd = dataSource.getConnection().getMetaData();
            ResultSet rs = dbmd.getColumns(null, "%", tableName, "%");
            while (rs.next()) {
                TableColumn tableColumn = new TableColumn();
                String columnName = rs.getString("COLUMN_NAME");
                String columnType = rs.getString("TYPE_NAME");
                String comment = rs.getString("REMARKS");

                tableColumn.setColumn_name(columnName);
                tableColumn.setComment(comment);
                tableColumn.setType_name(columnType);
                tableColumn.setHiveTable(hiveTable);

                System.out.println(columnName + " " + columnType + " " + comment);
                list.add(tableColumn);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}
