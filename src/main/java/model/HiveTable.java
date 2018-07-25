package model;

import java.sql.Timestamp;

/**
 * Created by 1 on 2018/7/23.
 * hive 中表元数据信息以及kafka topic信息
 */
public class HiveTable {
    //主键ID
    private int id;

    //表名或topic名
    private String name;

    //表所属数据库ID
    private int db_id;

    //创建时间
    private Timestamp create_time;

    //创建人
    private String owner;

    //表类型：MANAGED_TABLE 内部表 ，EXTERNAL_TABLE 外部表 ，INDEX_TABLE 索引图 ，VIRTUAL_VIEW 视图
    private String type;

    //hive 或者 kafka
    private String source;

    //表或topic描述，注释
    private String comment;

    public HiveTable() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getDb_id() {
        return db_id;
    }

    public void setDb_id(int db_id) {
        this.db_id = db_id;
    }

    public Timestamp getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Timestamp create_time) {
        this.create_time = create_time;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

}
