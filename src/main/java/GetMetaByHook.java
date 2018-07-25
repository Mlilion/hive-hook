import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import service.AccessInfoService;
import service.ColumnsService;
import service.DBMetaService;
import service.TableMetaService;

import java.util.TreeSet;

/**
 * Created by 1 on 2018/7/20.
 */
public class GetMetaByHook implements HiveDriverRunHook {
    private DBMetaService dbService = new DBMetaService();
    private TableMetaService tableService = new TableMetaService();
    private AccessInfoService accessInfoService = new AccessInfoService();
    private ColumnsService columnsService = new ColumnsService();

    public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception {

    }

    public void postDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
        String sql = hookContext.getCommand().toLowerCase();
        SQLProcess sqlProcess = new SQLProcess();
        sqlProcess.getLineageInfo(sql);

        SessionState ss = SessionState.get();

        String dbName = ss.getCurrentDatabase();
        String dbURI = ss.getHdfsScratchDirURIString();

        System.out.println("database:" + dbName);
        System.out.println("sql: " + sql);

        dbService.checkDataBase(dbName, dbURI);

        TreeSet<String> outputTableList = sqlProcess.getOutputTableList();
        TreeSet<String> inputTableList = sqlProcess.getInputTableList();

        if (sql.startsWith("create") && sqlProcess.isCreate()) {
            String createTableName = null;
            if (!outputTableList.isEmpty()) {
                //生成的表的名字 ,一般情况下生成表只有一张，可能对应有多张来源表
                for (String out_tableName :
                        outputTableList) {
                    createTableName = out_tableName;
                    System.out.println("tableName: " + out_tableName);
                    tableService.insertMyHiveTable(out_tableName, ss);
                    //插入表字段
                    columnsService.insertColumns(out_tableName, dbName);
                    //遍历输入的表名
                    if (!inputTableList.isEmpty()) {
                        for (String in_tableName :
                                inputTableList) {
                            System.out.println("in------tableName: " + in_tableName);
                            tableService.insertLineageInfo(createTableName, in_tableName, dbName);
                        }
                    }
                }
            }
        }

        //当标记为select，且不为create时或者sql以select 前缀开始的情况
        if ((sqlProcess.isSelect() && !sqlProcess.isCreate()) || sql.startsWith("select")) {
            System.out.println("查询语句执行！！！！！！！！");
            for (String tableName :
                    inputTableList) {
                accessInfoService.insertAccessInfo(tableName, dbName, ss.getUserName());
            }
            return;
        }

        //删除表操作
        if (sqlProcess.isDropTable()) {
            String dropTableName = sqlProcess.getDropTableName();
            tableService.deleteTable(dropTableName,dbName);
            System.out.println("表被删除了、、、、、、、、、、、、、、、、");
        }

        if (sqlProcess.isAlterTable()) {
            System.out.println("修改表...........");
        }

        System.out.println("select: " + sqlProcess.isSelect());
        System.out.println("create: " + sqlProcess.isCreate());
        System.out.println("drop: " + sqlProcess.isDropTable());
        System.out.println("alter: " + sqlProcess.isAlterTable());

    }
}
