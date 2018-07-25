import dao.mysql.DBMetaDao;
import dao.mysql.TableMetaDao;
import model.HiveTable;
import org.junit.Test;
import service.AccessInfoService;
import service.TableMetaService;
import utils.CommonUtils;

/**
 * Created by 1 on 2018/7/23.
 */
public class DaoTest {


//    @Test
    public void testQueryID() {
        DBMetaDao hiveDBDao = new DBMetaDao();
        int id = hiveDBDao.selectDbIdByName("ods");
        System.out.println(id);
    }

//    @Test
    public void testDB_id() {
        DBMetaDao metaDao = new DBMetaDao();
    }

//    @Test
    public void testAccess_info() {
        AccessInfoService aiService = new AccessInfoService();
        aiService.insertAccessInfo("ods_order", "ods", "wangjian");
    }

//    @Test
    public void testInsertTable() {
        TableMetaDao tmd = new TableMetaDao();
        HiveTable table = new HiveTable();
        table.setName("test1");
        table.setOwner("wangjian");
        table.setType("table");
        table.setSource("hive");
        table.setComment("测试一下");
        table.setDb_id(1);
        table.setCreate_time(CommonUtils.getSqlTime());

        tmd.insertTable(table);
    }

//    @Test
    public void testdelete() {
        TableMetaService tms = new TableMetaService();
        tms.deleteTable("test1", "ods");
    }
}
