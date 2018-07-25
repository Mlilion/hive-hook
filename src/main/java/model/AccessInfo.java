package model;

import java.sql.Timestamp;

/**
 * Created by 1 on 2018/7/23.
 */
public class AccessInfo {
    //访问信息所属的hive表或kafka.topic
    private HiveTable hiveTable;

    //访问时间
    private Timestamp access_time;

    //访问人
    private String username;

    public AccessInfo() {
    }

    public HiveTable getHiveTable() {

        return hiveTable;
    }

    public void setHiveTable(HiveTable hiveTable) {
        this.hiveTable = hiveTable;
    }

    public Timestamp getAccess_time() {
        return access_time;
    }

    public void setAccess_time(Timestamp access_time) {
        this.access_time = access_time;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

}
