package test.hive;
/*
 * Date: 13-8-12
 */

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.EJoinType;
import gudusoft.gsqlparser.TBaseType;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TJoin;
import gudusoft.gsqlparser.nodes.TJoinItem;
import gudusoft.gsqlparser.nodes.hive.EHiveInsertType;
import gudusoft.gsqlparser.nodes.hive.THiveHintClause;
import gudusoft.gsqlparser.stmt.TInsertSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import junit.framework.TestCase;

public class testInsert extends TestCase {

    public void test1(){
        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvhive);
        sqlparser.sqltext = "INSERT OVERWRITE DIRECTORY 's3://bucketname/path/subpath/' SELECT * \n" +
                "FROM hiveTableName;";
          assertTrue(sqlparser.parse() == 0);

        TInsertSqlStatement insert = (TInsertSqlStatement)sqlparser.sqlstatements.get(0);
        assertTrue(insert.getHiveInsertType() == EHiveInsertType.overwriteDirectory);
        assertTrue(insert.getDirectoryName().toString().equalsIgnoreCase("'s3://bucketname/path/subpath/'"));

        TSelectSqlStatement select = insert.getSubQuery();
        assertTrue(select.getResultColumnList().getResultColumn(0).toString().equalsIgnoreCase("*"));
        assertTrue(select.tables.getTable(0).toString().equalsIgnoreCase("hiveTableName"));
     }

    public void test2(){
        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvhive);
        sqlparser.sqltext = "INSERT OVERWRITE TABLE hiveTableName SELECT * FROM s3_import;";
          assertTrue(sqlparser.parse() == 0);

        TInsertSqlStatement insert = (TInsertSqlStatement)sqlparser.sqlstatements.get(0);
        assertTrue(insert.getHiveInsertType() == EHiveInsertType.overwriteTable);
        assertTrue(insert.tables.getTable(0).toString().equalsIgnoreCase("hiveTableName"));

        TSelectSqlStatement select = insert.getSubQuery();
        assertTrue(select.getResultColumnList().getResultColumn(0).toString().equalsIgnoreCase("*"));
        assertTrue(select.tables.getTable(0).toString().equalsIgnoreCase("s3_import"));
     }

    public void test3(){
        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvhive);
        sqlparser.sqltext = "INSERT OVERWRITE TABLE pv_users\n" +
                "SELECT pv.*, u.gender, u.age\n" +
                "FROM user FULL OUTER JOIN page_view pv ON (pv.userid = u.id)\n"+
                "WHERE pv.date = '2008-03-03';";
          assertTrue(sqlparser.parse() == 0);

        TInsertSqlStatement insert = (TInsertSqlStatement)sqlparser.sqlstatements.get(0);
        assertTrue(insert.getHiveInsertType() == EHiveInsertType.overwriteTable);
        assertTrue(insert.tables.getTable(0).toString().equalsIgnoreCase("pv_users"));

        TSelectSqlStatement select = insert.getSubQuery();
        TJoin join = select.joins.getJoin(0);
        assertTrue(join.getKind() == TBaseType.join_source_table);
        assertTrue(join.getTable().getTableName().toString().equalsIgnoreCase("user"));
        assertTrue(select.toString().equalsIgnoreCase("SELECT pv.*, u.gender, u.age\n" +
                "FROM user FULL OUTER JOIN page_view pv ON (pv.userid = u.id)\n" +
                "WHERE pv.date = '2008-03-03'"));
        TJoinItem joinItem = join.getJoinItems().getJoinItem(0);
        assertTrue(joinItem.getJoinType() == EJoinType.fullouter);
        assertTrue(joinItem.getTable().toString().equalsIgnoreCase("page_view"));
        assertTrue(joinItem.getOnCondition().toString().equalsIgnoreCase("(pv.userid = u.id)"));
     }

}
