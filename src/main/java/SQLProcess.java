/**
 * Created by 1 on 2018/7/20.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * This class prints out the lineage info. It takes sql as input and prints
 * lineage info. Currently this prints only input and output tables for a given
 * sql. Later we can expand to add join tables etc.
 */
public class SQLProcess implements NodeProcessor {

    /**
     * Stores input tables in sql.
     */
    TreeSet<String> inputTableList = new TreeSet<String>();
    /**
     * Stores output tables in sql.
     */
    TreeSet<String> OutputTableList = new TreeSet<String>();

    private String dropTableName;
    private String dropDatabaseName;
    private String alterTableName;

    private boolean isSelect;
    private boolean isCreate;
    private boolean isAlterTable;
    private boolean isInsert;
    private boolean isDropDataBase;
    private boolean isDropTable;

    public boolean isCreate() {
        return isCreate;
    }

    public boolean isAlterTable() {
        return isAlterTable;
    }

    public boolean isInsert() {
        return isInsert;
    }

    public boolean isDropDataBase() {
        return isDropDataBase;
    }

    public boolean isDropTable() {
        return isDropTable;
    }

    public boolean isSelect() {
        return isSelect;
    }

    public String getDropTableName() {
        return dropTableName;
    }

    public String getDropDatabaseName() {
        return dropDatabaseName;
    }

    public String getAlterTableName() {
        return alterTableName;
    }

    /**
     * @return java.util.TreeSet
     */
    public TreeSet<String> getInputTableList() {
        return inputTableList;
    }

    /**
     * @return java.util.TreeSet
     */
    public TreeSet<String> getOutputTableList() {
        return OutputTableList;
    }

    /**
     * Implements the process method for the NodeProcessor interface.
     */
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {
        ASTNode pt = (ASTNode) nd;

        switch (pt.getToken().getType()) {

            case HiveParser.TOK_CREATETABLE:
                // 这里是 CREATE 语句
                isCreate = true;
                OutputTableList.add(getName(pt));
                break;

            case HiveParser.TOK_TAB:
                // 这里是 insert 语句
                isInsert = true;
                OutputTableList.add(getName(pt));
                break;

            case HiveParser.TOK_TABREF:
                // 这里是 FROM 语句后面的内容
                ASTNode tabTree = (ASTNode) pt.getChild(0);
                String table_name = (tabTree.getChildCount() == 1) ?
                        getName(tabTree) :
                        getName(tabTree) + "." + tabTree.getChild(1);
                inputTableList.add(table_name);
                break;

            //此处对CTE模式的处理
            case HiveParser.TOK_CTE:
                for (int i = 0; i < pt.getChildCount(); i++) {
                    ASTNode temp = (ASTNode) pt.getChild(i);
                    String cteName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) temp.getChild(1));
                    //删除CTE别名
                    inputTableList.remove(cteName);
                }
                break;

            case HiveParser.TOK_ALTERTABLE:
                // 修改table
                isAlterTable = true;
                alterTableName = getName(pt);
                break;

            case HiveParser.TOK_DROPDATABASE:
                //删除数据库
                isDropDataBase = true;
                dropDatabaseName = getName(pt);
                break;

            case HiveParser.TOK_DROPTABLE:
                //删除表
                isDropTable = true;
                dropTableName = getName(pt);
                break;

            case HiveParser.TOK_SELECT:
                isSelect = true;
                break;

        }
        return null;
    }

    private String getName(ASTNode pt) {
        return BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
    }

    /**
     * parses given query and gets the lineage info.
     *
     * @param query
     * @throws ParseException
     */
    public void getLineageInfo(String query) throws ParseException,
            SemanticException {

    /*
     * Get the AST tree
     */
        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(query);

        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }

    /*
     * initialize Event Processor and dispatcher.
     */
        inputTableList.clear();
        OutputTableList.clear();

        // create a walker which walks the tree in a DFS manner while maintaining
        // the operator stack. The dispatcher
        // generates the plan from the operator tree
        Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();

        // The dispatcher fires the processor corresponding to the closest matching
        // rule and passes the context along
        Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);

        // Create a list of topop nodes
        ArrayList<Node> topNodes = new ArrayList<Node>();
        topNodes.add(tree);
        ogw.startWalking(topNodes, null);
    }

    public static void main(String[] args) throws IOException, ParseException,
            SemanticException {

        String query = "WITH data1 AS (SELECT id, age FROM table1 WHERE id > 0),\n" +
                "data2 AS (SELECT id, name FROM table2 WHERE id > 0) " +
                "select * from data1 join data2 on data1.i = data2.i";

        SQLProcess lep = new SQLProcess();

        lep.getLineageInfo(query);

        for (String tab : lep.getInputTableList()) {
            System.out.println("InputTable=" + tab);
        }

        for (String tab : lep.getOutputTableList()) {
            System.out.println("OutputTable=" + tab);
        }

        System.out.println("select: " + lep.isSelect());
        System.out.println("create: " + lep.isCreate());
        System.out.println("drop: " + lep.isDropTable());
        System.out.println("alter: " + lep.isAlterTable());


    }
}