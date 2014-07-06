package test.hive;

import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.nodes.*;
import gudusoft.gsqlparser.nodes.hive.THiveClusterBy;
import gudusoft.gsqlparser.nodes.hive.THiveHintClause;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.stmt.hive.THiveFromQuery;
import junit.framework.TestCase;

/**
 * Created by IntelliJ IDEA.
 * User: tako
 * Date: 13-8-11
 * Time: 下午6:32
 * To change this template use File | Settings | File Templates.
 */
public class testUnion extends TestCase {

    public void testUnionInSubquery(){
          TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvhive);
          sqlparser.sqltext ="SELECT t3.col\n" +
                  "FROM (\n" +
                  "  SELECT a+b AS col\n" +
                  "  FROM t1\n" +
                  "  UNION ALL\n" +
                  "  SELECT c-d AS col\n" +
                  "  FROM t2\n" +
                  ") t3;";
          assertTrue(sqlparser.parse() == 0);

          TSelectSqlStatement select = (TSelectSqlStatement)sqlparser.sqlstatements.get(0);
          assertTrue(select.getResultColumnList().size() == 1);

        assertTrue(select.joins.size() == 1);
        TJoin join = select.joins.getJoin(0);
        assertTrue(join.getKind() == TBaseType.join_source_fake);

        TTable table = join.getTable();
        assertTrue(table.getTableType() == ETableSource.subquery);

        TSelectSqlStatement subquery  = table.getSubquery();
        assertTrue(subquery.isCombinedQuery());
        TSelectSqlStatement left = subquery.getLeftStmt();
        TSelectSqlStatement right = subquery.getRightStmt();

        TResultColumn rs =  left.getResultColumnList().getResultColumn(0);
        TExpression expr = rs.getExpr();
        assertTrue(expr.getExpressionType() == EExpressionType.arithmetic_plus_t);

        TAliasClause aliasClause = rs.getAliasClause();
       // System.out.println(aliasClause.getAliasName().toString());
        assertTrue(aliasClause.getAliasName().toString().equalsIgnoreCase("col"));

        rs =  right.getResultColumnList().getResultColumn(0);
        expr = rs.getExpr();
        assertTrue(expr.getExpressionType() == EExpressionType.arithmetic_minus_t);


      }


        public void test1(){
        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvhive);
        sqlparser.sqltext = "SELECT u.id, actions.date\n" +
                "FROM (\n" +
                "    SELECT av.uid AS uid \n" +
                "    FROM action_video av \n" +
                "    WHERE av.date = '2008-06-03'\n" +
                "    UNION ALL \n" +
                "    SELECT ac.uid AS uid \n" +
                "    FROM action_comment ac \n" +
                "    WHERE ac.date = '2008-06-03'\n" +
                " ) actions JOIN users u ON (u.id = actions.uid) ;";
          assertTrue(sqlparser.parse() == 0);

            TSelectSqlStatement select = (TSelectSqlStatement)sqlparser.sqlstatements.get(0);
            assertTrue(select.joins.size() == 1);
            TJoin join = select.joins.getJoin(0);
            assertTrue(join.getKind() == TBaseType.join_source_table);

            TTable table = join.getTable();
            assertTrue(table.getTableType() == ETableSource.subquery);
            TJoinItem joinItem = join.getJoinItems().getJoinItem(0);
            assertTrue(joinItem.getJoinType() == EJoinType.inner);
            assertTrue(joinItem.getTable().getFullName().equalsIgnoreCase("users"));

            TSelectSqlStatement subquery = table.getSubquery();
            assertTrue(subquery.isCombinedQuery());

            TSelectSqlStatement left = subquery.getLeftStmt();
            TSelectSqlStatement right = subquery.getRightStmt();

            TResultColumn rs =  left.getResultColumnList().getResultColumn(0);
            TExpression expr = rs.getExpr();
            assertTrue(expr.getExpressionType() == EExpressionType.simple_object_name_t);
            assertTrue(expr.toString().equalsIgnoreCase("av.uid"));
            assertTrue(rs.getAliasClause().getAliasName().toString().equalsIgnoreCase("uid"));

            rs =  right.getResultColumnList().getResultColumn(0);
            expr = rs.getExpr();
            assertTrue(expr.getExpressionType() == EExpressionType.simple_object_name_t);
            assertTrue(expr.toString().equalsIgnoreCase("ac.uid"));
            assertTrue(rs.getAliasClause().getAliasName().toString().equalsIgnoreCase("uid"));


        }

        public void test2(){
        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvhive);
        sqlparser.sqltext = "FROM (\n" +
                "     FROM (\n" +
                "             FROM action_video av\n" +
                "             SELECT av.uid AS uid, av.id AS id, av.date AS date\n" +
                " \n" +
                "            UNION ALL\n" +
                " \n" +
                "             FROM action_comment ac\n" +
                "             SELECT ac.uid AS uid, ac.id AS id, ac.date AS date\n" +
                "     ) union_actions\n" +
                "     SELECT union_actions.uid, union_actions.id, union_actions.date\n" +
                "     CLUSTER BY union_actions.uid)  map;";
          assertTrue(sqlparser.parse() == 0);

          assertTrue(sqlparser.sqlstatements.get(0).sqlstatementtype == ESqlStatementType.ssthiveFromQuery);
          THiveFromQuery fromQuery = (THiveFromQuery)sqlparser.sqlstatements.get(0);
          TTable table = fromQuery.tables.getTable(0);
          assertTrue(table.getTableType() == ETableSource.hiveFromQuery);
          assertTrue(table.getAliasClause().getAliasName().toString().equalsIgnoreCase("map"));
          assertTrue(fromQuery.getHiveBodyList().size() == 0);

          fromQuery = table.getHiveFromQuery();
          TSelectSqlStatement select = (TSelectSqlStatement)fromQuery.getHiveBodyList().get(0);
          assertTrue(select.getResultColumnList().size() == 3);
          THiveClusterBy clusterBy = select.getHiveClusterBy();
          assertTrue(clusterBy.getExpressionList().getExpression(0).toString().equalsIgnoreCase("union_actions.uid"));

          table = fromQuery.tables.getTable(0);
          assertTrue(table.getTableType() == ETableSource.subquery);
          assertTrue(table.getAliasClause().getAliasName().toString().equalsIgnoreCase("union_actions"));
          select = table.getSubquery();
          assertTrue(select.isCombinedQuery());
          assertTrue(select.getSetOperator() == TSelectSqlStatement.setOperator_unionall);
          // hive from query: from...select will be translate into select statement in union operation
          TSelectSqlStatement left = select.getLeftStmt();
          assertTrue(left.tables.getTable(0).getTableName().toString().equalsIgnoreCase("action_video"));
          assertTrue(left.tables.getTable(0).getAliasClause().getAliasName().toString().equalsIgnoreCase("av"));

            // hive from query: from...select will be translate into select statement in union operation
            TSelectSqlStatement right = select.getRightStmt();
            assertTrue(right.tables.getTable(0).getTableName().toString().equalsIgnoreCase("action_comment"));
            assertTrue(right.tables.getTable(0).getAliasClause().getAliasName().toString().equalsIgnoreCase("ac"));

          //assertTrue(fromQuery.getHiveBodyList().size() == 0);

      }
}
