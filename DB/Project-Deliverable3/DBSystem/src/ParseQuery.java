	import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.*;
import gudusoft.gsqlparser.pp.utils.SourceTokenConstant;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

    import java.io.*;
    import java.util.*;

/**
 * Created by rishimittal on 12/2/14.
 */

public class ParseQuery {

    private String tab_name = " ";
    private StringBuilder sb = new StringBuilder();

    public void queryType(String query, DBSystem dbSystem){
        if(query.substring(0,query.indexOf(' ')).equalsIgnoreCase("select")){
            selectCommand(query,dbSystem);
        }else if(query.substring(0,query.indexOf(' ')).equalsIgnoreCase("select")){
            createCommand(query,dbSystem);
        }else{
            System.out.println("Query Invalid\n");
        }
    }
	
    public int validateAndDisplayCreateCommandParameters(String query){ //incomplete
        int rValue = 0;
        TGSqlParser tgSqlParser=new TGSqlParser(EDbVendor.dbvoracle);
        tgSqlParser.sqltext=query;
        tgSqlParser.parse();
        TCreateTableSqlStatement tCreateTableSqlStatement=(TCreateTableSqlStatement)tgSqlParser.sqlstatements.get(0);

        try{
            // Also validate that if that table is present
            // by searching for that table in the tablelist
            // populated by the readConfig method.

            for(Table t : DBSystem.tableList){
                if(t.getName().equalsIgnoreCase(tCreateTableSqlStatement.getTableName().toString())){
                   return 0;
                }
            }
            System.out.println("Querytype:create");
            System.out.println("Tablename:" + tCreateTableSqlStatement.getTableName());
            tab_name = tCreateTableSqlStatement.getTableName().toString().toLowerCase();
            //System.out.println(tCreateTableSqlStatement.getTableName());        //table name
            int numberOfCols = tCreateTableSqlStatement.getColumnList().size();
            //System.out.println(numberOfCols);
            System.out.print("Attributes:");
            for(int k = 0 ; k < numberOfCols ; k++){
                String CName = tCreateTableSqlStatement.getColumnList().getColumn(k).getColumnName().toString();
                String dType = tCreateTableSqlStatement.getColumnList().getColumn(k).getDatatype().toString();

                System.out.print(CName);      //get 1st column name
                System.out.print(" ");
                System.out.print(dType);        //get 1st column data type
                if(k != numberOfCols - 1 ) {
                    sb.append(CName).append(":").append(dType).append(",");
                    System.out.print(",");
                }else{
                    sb.append(CName).append(":").append(dType);
                }
            }
            System.out.println();
            rValue = 1;

        }catch(NullPointerException ne){
            //ne.printStackTrace();
        }
        return rValue;
    }

    public void createCommand(String query, DBSystem dbSystem){

        int isValid = validateAndDisplayCreateCommandParameters(query);
        PrintWriter pow = null;
        if(isValid == 1){
            //System.out.println(DBSystem.PATH_FOR_DATA);
            try {
                //code to Create the tablename.data and tablename.csv file.
                File fdataFile = new File(DBSystem.PATH_FOR_DATA + "/" + tab_name + ".data");
                new File(DBSystem.PATH_FOR_DATA + "/" + tab_name + ".csv").createNewFile();
                //Add the details in the .data file
                pow = new PrintWriter( new FileWriter(fdataFile));
                pow.write(sb.toString());
                pow.flush();
                //Add the details in the config.txt.
                pow = new PrintWriter(new FileWriter(DBSystem.CONFIG_FILE_PATH , true));
                pow.write("BEGIN\n");
                pow.write(tab_name +"\n");
                String []ar = sb.toString().split(",");
                for(int j = 0 ; j < ar.length ; j++ ){
                   String [] at = ar[j].split(":");
                    pow.write(at[0]);
                    pow.write(",");
                    pow.write(at[1] + "\n");
                }
                pow.write("END\n");
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                    pow.close();
            }
        }else {
            System.out.println("Query Invalid\n");
        }
        DBSystem.readConfig(DBSystem.CONFIG_FILE_PATH);
    }

    public void selectCommand(String query, DBSystem dbSystem){
        int invalid=0;
        String tableNames="",whereClause="",orderClause="",projectList="";
        String details="Querytype:select\n";
        TGSqlParser tgSqlParser=new TGSqlParser(EDbVendor.dbvoracle);
        tgSqlParser.sqltext=query;
        tgSqlParser.parse();
        TSelectSqlStatement tSelectSqlStatement=(TSelectSqlStatement)tgSqlParser.sqlstatements.get(0);

        try{
            //Find number of tables in join. Invalid if no tables specified
            int noTabs=tSelectSqlStatement.joins.size();
            if(noTabs>0){
                if(!validateTables(tSelectSqlStatement))            //check for existing tables and its columns
                    throw new NullPointerException();
                details=details+"Tablename:"+tSelectSqlStatement.joins.getJoin(0);
                tableNames=tableNames+tSelectSqlStatement.joins.getJoin(0);
                for(int i=1;i<noTabs;i++){
                    details=details+","+tSelectSqlStatement.joins.getJoin(i);
                    tableNames=tableNames+tSelectSqlStatement.joins.getJoin(i);
                }
                details=details+"\n";
            }
            //find columns involved in select query

            int noCols=tSelectSqlStatement.getResultColumnList().size();
            String columnList="";
            if(noCols>0){
                String column=tSelectSqlStatement.getResultColumnList().getResultColumn(0).toString();
                details=details+"Columns:";
                if(column.equals("*")){
                    columnList=getColumnName(tSelectSqlStatement.joins);
                    details=details+columnList;
                    projectList=projectList+columnList;
                }else {
                    columnList=tSelectSqlStatement.getResultColumnList().getResultColumn(0).toString().replace("(", "").replace(")", "");
                    for(int i=1;i<noCols;i++){
                        columnList=columnList+","+tSelectSqlStatement.getResultColumnList().getResultColumn(i).toString().replace("(","").replace(")","");
                    }
                    details=details+columnList;
                    projectList=projectList+columnList;
                }
                details=details+"\n";
            }

            //Evaluate for distinct
            TSelectDistinct distinctClause=tSelectSqlStatement.getSelectDistinct();
            if(distinctClause!=null){
                details=details+"Distinct:"+columnList+"\n";
            }else
                details=details+"Distinct:NA\n";

            //Evaluate where clause
            TWhereClause wClause = tSelectSqlStatement.getWhereClause();
            if(wClause!=null){
                //System.out.println(wClause.getCondition().getLeftOperand());
                if(!validateColumnsRecurse(wClause.getCondition(), tSelectSqlStatement.joins))
                    throw new NullPointerException();
                details=details+"Condition:"+wClause.getCondition().toString()+"\n";
                whereClause=whereClause+wClause.getCondition().toString();
            }
            else
                details=details+"Condition:NA\n";


            //evaluate order by
            TOrderBy orderBy=tSelectSqlStatement.getOrderbyClause();
            if(orderBy!=null){
                String result=parseAndValidateColumns(orderBy.getItems().toString(), tSelectSqlStatement.joins);
                if(result.equals("invalid"))
                    throw new NullPointerException();
                details=details+"OrderBy:"+orderBy.getItems().toString()+"\n";
                orderClause=orderClause+orderBy.getItems().toString();
            }
            else
                details=details+"OrderBy:NA\n";


            //evaluate group by
            TGroupBy groupBy=tSelectSqlStatement.getGroupByClause();
            if(groupBy!=null){
                String result=validateColumns(groupBy.getItems().toString(), tSelectSqlStatement.joins);
                if(result.equals("int") || result.equals("invalid"))
                    throw new NullPointerException();
                details=details+"GroupBy:"+groupBy.getItems().toString()+"\n";

                //evaluate having clause {having occurs only with group by}
                TExpression havingClause = groupBy.getHavingClause();
                if(havingClause!=null){
                    //result=validateColumns(havingClause.getLeftOperand().toString(),tSelectSqlStatement.joins);
                    if(!validateColumnsRecurse(havingClause,tSelectSqlStatement.joins))
                        throw new NullPointerException();
                    details=details+"Having:"+havingClause.toString()+"\n";
                }else {
                    details=details+"Having:NA\n";
                }
            }
            else
                details=details+"GroupBy:NA\nHaving:NA\n";

            ProcessSelect processSelect=new ProcessSelect(projectList,whereClause,orderClause,tableNames,dbSystem);
            //System.out.println(projectList+" "+whereClause+" "+orderClause+" "+tableNames);
            //System.out.print(details);
        }catch (NullPointerException e){
            System.out.println("Query Invalid\n");
            //e.printStackTrace();
        }

    }

    private String parseAndValidateColumns(String s, TJoinList joins) {
        String result;
        StringTokenizer st =new StringTokenizer(s,",");
        while (st.hasMoreTokens())
        {
            result=validateColumns(st.nextToken().trim(),joins);
            if(result.equalsIgnoreCase("invalid"))
                return "invalid";
        }
        return "valid";

    }

    private boolean validateColumnsRecurse(TExpression condition, TJoinList joins) {
        if(condition.getLeftOperand().getLeftOperand()==null && condition.getRightOperand().getLeftOperand()==null){
            return validateColumns(condition.getLeftOperand().toString(),joins).equals(validateColumns(condition.getRightOperand().toString(), joins));
        }
        else {
            return (validateColumnsRecurse(condition.getLeftOperand(),joins) && validateColumnsRecurse(condition.getRightOperand(), joins));
        }
    }

    private String validateColumns(String condition, TJoinList joins) {
        try{
            Integer.parseInt(condition);
            return "int";
        }catch (NumberFormatException e){

        }

        try {
            Double.parseDouble(condition);
            return "float";
        }catch (NumberFormatException e){

        }

        if(condition.contains("'")||condition.contains("\""))
            return "varchar";
        else {
            int j=joins.size();
            for(int i=0;i<j;i++){
                Iterator<Table> it=DBSystem.tableList.iterator();
                Table table=null;
                while (it.hasNext()){
                    table=it.next();
                    if(table.getName().equalsIgnoreCase(joins.getJoin(i).toString())){

                        Iterator it1=table.getColumnData().entrySet().iterator();

                        while (it1.hasNext()){
                            Map.Entry pairs=(Map.Entry)it1.next();
                            String colName=pairs.getValue().toString();
                            if(pairs.getKey().toString().equalsIgnoreCase(condition)){
                                if(colName.equalsIgnoreCase("varchar")||colName.equalsIgnoreCase("string"))
                                    return "varchar";
                                else if(colName.equalsIgnoreCase("int")||colName.equalsIgnoreCase("integer"))
                                    return "int";
                                return pairs.getValue().toString().toLowerCase();
                            }
                        }

                    }
                }
            }
        }
        return "invalid";
    }

    private boolean validateTables(TSelectSqlStatement stmt) {
        HashMap<String,Integer> present=new HashMap<String, Integer>();
        if(!stmt.getResultColumnList().getResultColumn(0).toString().equals("*")){

            int cols=stmt.getResultColumnList().size();
            for(int a=0;a<cols;a++){
                present.put(stmt.getResultColumnList().getResultColumn(a).toString().toLowerCase().replace("(", "").replace(")",""),0);
            }
        }

        TJoinList joins = stmt.joins;
        int j=joins.size();
        int invalid=1;
        for(int i=0;i<j;i++){
            Iterator<Table> it=DBSystem.tableList.iterator();
            Table table=null;
            while (it.hasNext()){
                table=it.next();
                if(table.getName().equalsIgnoreCase(joins.getJoin(i).toString())){
                    invalid=0;
                    if(!stmt.getResultColumnList().getResultColumn(0).toString().equals("*")){
                        Iterator it1=table.getColumnData().entrySet().iterator();

                        while (it1.hasNext()){
                            Map.Entry pairs=(Map.Entry)it1.next();
                            if(present.get(pairs.getKey().toString().toLowerCase())!=null && present.get(pairs.getKey().toString().toLowerCase())==0){
                                present.put(pairs.getKey().toString(),1);
			                }else if(present.get(pairs.getKey().toString().toLowerCase())!=null && present.get(pairs.getKey().toString().toLowerCase())==1)
                                return false;
                        }
                    }
                }
            }
            if(invalid==1)
                return false;
            else{
                invalid=1;
            }
        }
        if(!stmt.getResultColumnList().getResultColumn(0).toString().equals("*")){
            Iterator it2=present.entrySet().iterator();
            while (it2.hasNext()){
                Map.Entry pairs=(Map.Entry)it2.next();
                if((Integer)pairs.getValue()==0){
                    return false;
                }
            }
        }
        return true;
    }

    private String getColumnName(TJoinList joins) {
        int j=joins.size();
        Set<String> columnName=new LinkedHashSet<String>();
        for(int i=0;i<j;i++){
            Iterator<Table> it=DBSystem.tableList.iterator();
            Table table=null;
            while (it.hasNext()){
                table=it.next();
                if(table.getName().equalsIgnoreCase(joins.getJoin(i).toString())){
                    Iterator it1=table.getColumnNum().entrySet().iterator();
                    while (it1.hasNext()){
                        Map.Entry pairs=(Map.Entry)it1.next();
                        columnName.add((String)pairs.getKey());
                    }
                    break;
                }
            }

        }
        return columnName.toString().substring(1,columnName.toString().length()-1).replaceAll(" ","").toLowerCase();
    }


    public static void main(String arr[]){
        ParseQuery pq = new ParseQuery();
        //DBSystem.readConfig("/tmp/config.txt");
        String configFilePath = arr[0];
        DBSystem dbSystem=new DBSystem();
        dbSystem.readConfig(configFilePath);
        dbSystem.populateDBInfo();
        //dbSystem.test();
        //String query = "Select distinct * from countries where id=123 and code like 'jkl' group by continent having code like 'antartic' order by name";
        //Uncomment Following lines when the input file is used.
        /*
        String inputFile = arr[1];
        try {
            BufferedReader br = new BufferedReader(new FileReader(inputFile));
            String line ;
            while ( (line = br.readLine())  != null ){
                pq.queryType(line,dbSystem);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Input file not found");
            //e.printStackTrace();
        } catch (IOException e) {
            System.out.println("I/O Exception");
            //e.printStackTrace();
        }
        */

        int testcases = 0;
        BufferedReader br = new BufferedReader( new InputStreamReader(System.in));
        try {
            testcases = Integer.parseInt(br.readLine());

        //System.out.println( testcases);
        String line;
        while(testcases > 0 ){
            line = br.readLine();
            pq.queryType(line, dbSystem);

           testcases--;
        }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
