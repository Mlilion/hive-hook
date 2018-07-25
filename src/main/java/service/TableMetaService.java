package service;

import dao.hive.HiveDao;
import dao.mysql.DBMetaDao;
import dao.mysql.TableMetaDao;
import model.HiveTable;
import org.apache.hadoop.hive.ql.session.SessionState;
import utils.CommonUtils;

/**
 * Created by 1 on 2018/7/23.
 */
public class TableMetaService {
    private HiveDao hiveDao = new HiveDao();
    private TableMetaDao tableMetaDao = new TableMetaDao();
    private DBMetaDao dbMetaDao = new DBMetaDao();

    public TableMetaService() {

    }


    public void insertMyHiveTable(String tableName, SessionState ss) {

        HiveTable hiveTable = new HiveTable();

        hiveTable.setName(tableName);
        hiveTable.setOwner(ss.getUserName());
        hiveTable.setCreate_time(CommonUtils.getSqlTime());
        hiveTable.setSource("hive");

        hiveDao.fillTableInfo(hiveTable);

        int db_id = dbMetaDao.selectDbIdByName(ss.getCurrentDatabase());
        hiveTable.setDb_id(db_id);

        tableMetaDao.insertTable(hiveTable);
    }

    public void insertLineageInfo(String createTableName, String in_tableName, String dbName) {
        int db_id = dbMetaDao.selectDbIdByName(dbName);

        int f_id = tableMetaDao.queryTableIDByName(in_tableName, db_id);
        int c_id = tableMetaDao.queryTableIDByName(createTableName, db_id);
        tableMetaDao.insertLineageInfo(f_id,c_id);
    }

    public HiveTable queryTableByName(String tableName, String dbName) {
        int db_id = dbMetaDao.selectDbIdByName(dbName);
        return tableMetaDao.queryTableByName(tableName, db_id);
    }

    public void deleteTable(String dropTableName, String dbName) {
        int db_id = dbMetaDao.selectDbIdByName(dbName);
        tableMetaDao.deleteTable(dropTableName,db_id);
    }
}
