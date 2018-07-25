package model;

/**
 * Created by 1 on 2018/7/23.
 */
public class TableColumn {
    private HiveTable hiveTable;
    private String comment;
    private String column_name;
    private String type_name;

    public TableColumn() {
    }

    public HiveTable getHiveTable() {
        return hiveTable;
    }

    public void setHiveTable(HiveTable hiveTable) {
        this.hiveTable = hiveTable;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getColumn_name() {
        return column_name;
    }

    public void setColumn_name(String column_name) {
        this.column_name = column_name;
    }

    public String getType_name() {
        return type_name;
    }

    public void setType_name(String type_name) {
        this.type_name = type_name;
    }

}
