package service;

import dao.mysql.AccessInfoDao;
import dao.mysql.DBMetaDao;
import dao.mysql.TableMetaDao;
import model.AccessInfo;
import model.HiveTable;
import utils.CommonUtils;

/**
 * Created by 1 on 2018/7/24.
 */
public class AccessInfoService {
    private AccessInfoDao aidao = new AccessInfoDao();
    private TableMetaDao tableMetaDao = new TableMetaDao();
    private DBMetaDao dbMetaDao = new DBMetaDao();

    public void checkAccessInfo(AccessInfo ai) {
        int count = aidao.countAccessInfo(ai);
        //如果超出或等于10条记录，删除最早的一条
        if (count >= 10) {
            aidao.deleteEarliestRecord(ai);
        }
    }

    public void insertAccessInfo(String tableName, String dbName, String userName) {
        int db_id = dbMetaDao.selectDbIdByName(dbName);
        HiveTable table = tableMetaDao.queryTableByName(tableName,db_id);

        AccessInfo ai = new AccessInfo();
        ai.setHiveTable(table);
        ai.setAccess_time(CommonUtils.getSqlTime());
        ai.setUsername(userName);

        //检测访问信息数量，同一张表的访问信息最多保留近10条
        checkAccessInfo(ai);
        aidao.insertAccessInfo(ai);
    }
}
