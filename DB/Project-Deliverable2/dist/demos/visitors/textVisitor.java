package demos.visitors;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.TStatementList;
import gudusoft.gsqlparser.nodes.*;
import gudusoft.gsqlparser.stmt.*;
import gudusoft.gsqlparser.stmt.TCreateSequenceStmt;
import gudusoft.gsqlparser.stmt.TAssignStmt;
import gudusoft.gsqlparser.stmt.TBasicStmt;
import gudusoft.gsqlparser.stmt.TCommonBlock;
import gudusoft.gsqlparser.stmt.TCaseStmt;
import gudusoft.gsqlparser.stmt.TCloseStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreateFunction;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreatePackage;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreateProcedure;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreateTrigger;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreateType;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreateTypeBody;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreateType_Placeholder;
import gudusoft.gsqlparser.stmt.TCursorDeclStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlDummyStmt;
import gudusoft.gsqlparser.stmt.TElsifStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlExecImmeStmt;
import gudusoft.gsqlparser.stmt.TExitStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlForallStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlGotoStmt;
import gudusoft.gsqlparser.stmt.TIfStmt;
import gudusoft.gsqlparser.stmt.TLoopStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlNullStmt;
import gudusoft.gsqlparser.stmt.TOpenStmt;
import gudusoft.gsqlparser.stmt.TOpenforStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlPipeRowStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlPragmaDeclStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlProcedureSpecStmt;
import gudusoft.gsqlparser.stmt.TRaiseStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlRecordTypeDefStmt;
import gudusoft.gsqlparser.stmt.TReturnStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlSqlStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlSubProgram;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlTableTypeDefStmt;
import gudusoft.gsqlparser.nodes.TVarDeclStmt;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlVarrayTypeDefStmt;
import gudusoft.gsqlparser.stmt.oracle.TSqlplusCmdStatement;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class textVisitor {

    public static void main(String args[])
    {
        long t;
        t = System.currentTimeMillis();

        if (args.length != 1){
            System.out.println("Usage: java textVisitor sqlfile.sql");
            return;
        }
        File file=new File(args[0]);
        if (!file.exists()){
            System.out.println("File not exists:"+args[0]);
            return;
        }

        EDbVendor dbVendor = EDbVendor.dbvoracle;
        String msg = "Please select SQL dialect: 1: SQL Server, 2: Oralce, 3: MySQL, 4: DB2, 5: PostGRESQL, 6: Teradta, default is 2: Oracle";
        System.out.println(msg);

        BufferedReader br=new   BufferedReader(new InputStreamReader(System.in));
        try{
            int db = Integer.parseInt(br.readLine());
            if (db == 1){
                dbVendor = EDbVendor.dbvmssql;
            }else if(db == 2){
                dbVendor = EDbVendor.dbvoracle;
            }else if(db == 3){
                dbVendor = EDbVendor.dbvmysql;
            }else if(db == 4){
                dbVendor = EDbVendor.dbvdb2;
            }else if(db == 5){
                dbVendor = EDbVendor.dbvpostgresql;
            }else if(db == 6){
                dbVendor = EDbVendor.dbvteradata;
            }
        }catch(IOException i) {
        }catch (NumberFormatException numberFormatException){
        }

        System.out.println("Selected SQL dialect: "+dbVendor.toString());

        TGSqlParser sqlparser = new TGSqlParser(dbVendor);

        sqlparser.sqlfilename  = args[0];

        int ret = sqlparser.parse();
        if (ret == 0){
            TtextVisitor tv = new TtextVisitor();
           for(int i=0;i<sqlparser.sqlstatements.size();i++){
               sqlparser.sqlstatements.get(i).accept(tv);
           }
        }else{
            System.out.println(sqlparser.getErrormessage());
        }

        System.out.println("Time Escaped: "+ (System.currentTimeMillis() - t) );
    }

}

class TtextVisitor extends TParseTreeVisitor{
    void show(TCustomSqlStatement stmt){
        System.out.println("sql type: "+stmt.sqlstatementtype);
        //System.out.println("sql text: "+stmt.toString());
    }

    void showNode(TParseTreeNode node){
        System.out.println("node type: "+node.getNodeType());
    }
    public void preVisit(TSelectSqlStatement stmt){
        show(stmt);
    }

    public void postVisit(TStatementList node){
        System.out.println("stmt list: "+node.size());
    }
    public void postVisit(TCommonBlock node){show(node);}
    public void postVisit(TExceptionClause node){showNode(node);}
    public void postVisit(TExceptionHandler node){showNode(node);}
    public void postVisit(TCreateSequenceStmt node){show(node);}
    public void postVisit(TAssignStmt node){show(node);}
    public void postVisit(TBasicStmt node){show(node);}
    public void postVisit(TCaseStmt node){show(node);}
    public void postVisit(TCloseStmt node){show(node);}
    public void postVisit(TPlsqlCreateFunction node){show(node);}
    public void postVisit(TPlsqlCreatePackage node){show(node);}
    public void postVisit(TPlsqlCreateProcedure node){show(node);}
    public void postVisit(TPlsqlCreateTrigger node){show(node);}
    public void postVisit(TPlsqlCreateType node){show(node);}
    public void postVisit(TPlsqlCreateType_Placeholder node){show(node);}
    public void postVisit(TPlsqlCreateTypeBody node){show(node);}
    public void visit(TCursorDeclStmt node){show(node);}
    public void visit(TPlsqlDummyStmt node){show(node);}
    public void visit(TElsifStmt node){show(node);}
    public void visit(TPlsqlExecImmeStmt node){show(node);}
    public void visit(TExitStmt node){show(node);}
    public void visit(TFetchStmt node){show(node);}
    public void visit(TPlsqlForallStmt node){show(node);}
    public void visit(TPlsqlGotoStmt node){show(node);}
    public void visit(TIfStmt node){show(node);}
    public void visit(TLoopStmt node){show(node);}
    public void visit(TPlsqlNullStmt node){show(node);}
    public void visit(TOpenforStmt node){show(node);}
    public void visit(TOpenStmt node){show(node);}
    public void visit(TPlsqlPipeRowStmt node){show(node);}
    public void visit(TPlsqlPragmaDeclStmt node){show(node);}
    public void visit(TPlsqlProcedureSpecStmt node){show(node);}
    public void visit(TRaiseStmt node){show(node);}
    public void visit(TPlsqlRecordTypeDefStmt node){show(node);}
    public void visit(TReturnStmt node){show(node);}
    public void visit(TPlsqlSqlStmt node){show(node);}
    public void visit(TPlsqlSubProgram node){show(node);}
    public void visit(TPlsqlTableTypeDefStmt node){show(node);}
    public void visit(TVarDeclStmt node){show(node);}
    public void visit(TPlsqlVarrayTypeDefStmt node){show(node);}
    public void visit(TSqlplusCmdStatement node){show(node);}

    public void postVisit(TAlterTableStatement stmt){show(stmt);}
    public void postVisit(TCreateIndexSqlStatement stmt){show(stmt);}
    public void postVisit(TCreateTableSqlStatement stmt){show(stmt);}
    public void postVisit(TCreateViewSqlStatement stmt){show(stmt);}
    public void postVisit(TDeleteSqlStatement stmt){show(stmt);}
    public void postVisit(TDropIndexSqlStatement stmt){show(stmt);}
    public void postVisit(TDropTableSqlStatement stmt){show(stmt);}
    public void postVisit(TDropViewSqlStatement stmt){show(stmt);}
    public void postVisit(TInsertSqlStatement stmt){show(stmt);}
    public void postVisit(TMergeSqlStatement stmt){show(stmt);}
    public void postVisit(TUpdateSqlStatement stmt){show(stmt);}
    public void postVisit(TUnknownSqlStatement stmt){show(stmt);}


}

