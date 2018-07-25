package service;

import dao.mysql.DBMetaDao;
import model.HiveDB;

/**
 * Created by 1 on 2018/7/23.
 */
public class DBMetaService {
    private DBMetaDao mdbd = new DBMetaDao();

    public void checkDataBase(String database, String db_url) {
        if (mdbd.selectDBbyName(database) == null) {
            HiveDB hiveDB = new HiveDB();
            hiveDB.setDb_name(database);
            hiveDB.setDb_url(db_url);
            mdbd.insetDBInfo(hiveDB);
        }
    }
}
