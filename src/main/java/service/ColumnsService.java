package service;

import dao.hive.HiveDao;
import dao.mysql.ColumnsDao;
import dao.mysql.TableMetaDao;
import model.HiveTable;
import model.TableColumn;

import java.util.List;

/**
 * Created by 1 on 2018/7/24.
 */
public class ColumnsService {
    private TableMetaDao tmd = new TableMetaDao();
    private HiveDao hiveDao = new HiveDao();
    private TableMetaService tms = new TableMetaService();
    private ColumnsDao cd = new ColumnsDao();

    public void insertColumns(String tableName, String dbName) {
        HiveTable table = tms.queryTableByName(tableName, dbName);
        List<TableColumn> list = hiveDao.getColumns(tableName, table);
        for (TableColumn c :
                list) {
            cd.insertColumn(c);
        }
    }
}
