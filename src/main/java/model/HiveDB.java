package model;

/**
 * Created by 1 on 2018/7/23.
 */
public class HiveDB {
    private int db_id;
    private String db_name;
    private String db_url;

    public HiveDB() {
    }

    public int getDb_id() {

        return db_id;
    }

    public void setDb_id(int db_id) {
        this.db_id = db_id;
    }

    public String getDb_name() {
        return db_name;
    }

    public void setDb_name(String db_name) {
        this.db_name = db_name;
    }

    public String getDb_url() {
        return db_url;
    }

    public void setDb_url(String db_url) {
        this.db_url = db_url;
    }
}
