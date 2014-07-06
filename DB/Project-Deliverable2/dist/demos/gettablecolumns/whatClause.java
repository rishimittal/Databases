package demos.gettablecolumns;


import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.nodes.TObjectName;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.nodes.TTable;
import gudusoft.gsqlparser.stmt.TDeleteSqlStatement;
import gudusoft.gsqlparser.stmt.TInsertSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.stmt.TUpdateSqlStatement;

public class whatClause  {

    public static void main(String args[])
     {
         TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvmssql);
         sqlparser.sqltext = "select employee_id,last_name\n" +
                 "from employees\n" +
                 "where department_id = 90\n" +
                 "group by employee_id\n" +
                 "order by last_name;";

         sqlparser.parse();

         TSelectSqlStatement select = (TSelectSqlStatement)sqlparser.sqlstatements.get(0);
         TTable table = select.tables.getTable(0);
         TObjectName o;
         System.out.println("Select statement, find out what clause a TObjectName belongs to:");
         for(int i=0;i<table.getObjectNameReferences().size();i++){
             o = table.getObjectNameReferences().getObjectName(i);
             System.out.println(o.toString()+"\t\t\tlocation:"+o.getLocation());
         }

         sqlparser.sqltext = "insert into emp e1 (e1.lastname,job) values('scott',10);";
         sqlparser.parse();

         TInsertSqlStatement insert = (TInsertSqlStatement)sqlparser.sqlstatements.get(0);
         table = insert.tables.getTable(0);

         System.out.println("\n\nInsert statement, find out what clause a TObjectName belongs to:");
         for(int i=0;i<table.getObjectNameReferences().size();i++){
             o = table.getObjectNameReferences().getObjectName(i);
             System.out.println(o.toString()+"\t\t\tlocation:"+o.getLocation());
         }

         sqlparser.sqltext = "update employees\n" +
                 "set department_ID = 70\n" +
                 "where employee_id = 113;";
         sqlparser.parse();


         TUpdateSqlStatement update = (TUpdateSqlStatement)sqlparser.sqlstatements.get(0);
         table = update.tables.getTable(0);
         System.out.println("\n\nUpdate statement, find out what clause a TObjectName belongs to:");
         for(int i=0;i<table.getObjectNameReferences().size();i++){
             o = table.getObjectNameReferences().getObjectName(i);
             System.out.println(o.toString()+"\t\t\tlocation:"+o.getLocation());
         }

         sqlparser.sqltext = "delete from employees E\n" +
                 "where employee_id = \n" +
                 "(select employee_sal\n" +
                 "from emp_history\n" +
                 "where employee_id = e.employee_id);";
         sqlparser.parse();


         TDeleteSqlStatement delete = (TDeleteSqlStatement)sqlparser.sqlstatements.get(0);
         table = delete.tables.getTable(0);

         System.out.println("\n\nDelete statement, find out what clause a TObjectName belongs to:");
         for(int i=0;i<table.getObjectNameReferences().size();i++){
             o = table.getObjectNameReferences().getObjectName(i);
             System.out.println(o.toString()+"\t\t\tlocation:"+o.getLocation());
         }


         // subquery in where clause
         select = (TSelectSqlStatement)delete.getStatements().get(0);
         TTable table1 = select.tables.getTable(0);
         System.out.println("\nSubquery in delete statement, find out what clause a TObjectName belongs to:");
         for(int i=0;i<table1.getObjectNameReferences().size();i++){
             o = table1.getObjectNameReferences().getObjectName(i);
             System.out.println(o.toString()+"\t\t\tlocation:"+o.getLocation());
         }

     }

}