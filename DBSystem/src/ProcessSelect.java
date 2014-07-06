import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by neel on 2/3/14.
 */
public class ProcessSelect {

    int MAX_MEM_TO_USE = DBSystem.NUM_PAGES * DBSystem.PAGE_SIZE;
    String tempFileFolderPath = DBSystem.PATH_FOR_DATA;

    public ProcessSelect(String projectList, String whereClause, String orderClause, String tableNames, DBSystem dbSystem) {
        String delim="";
        String delim1="";

        //set delimicompareter according to condition
        if(whereClause.toLowerCase().contains(" and ")){
            delim="(?i) and ";
            delim1="and";
        }
        else{
            delim="(?i) or ";
            delim1="or";
        }
        String[] conditions=null;
        Integer[] columnNum=null;
        String[] type=null;
        String[] operator=null;
        String[] compara=null;
        if(!whereClause.equals(""))
        {
            conditions=whereClause.split(delim);
            /*for(int i=0;i<conditions.length;i++)
                System.out.println(conditions[i].trim());*/

            columnNum=new Integer[conditions.length];
            type=new String[conditions.length];
            operator=new String[conditions.length];
            compara=new String[conditions.length];
        }
        //get table object for particular table name
        Iterator<Table> it=DBSystem.tableList.iterator();
        Table t=null;
        while (it.hasNext()){
            t=it.next();
            if(t.getName().equalsIgnoreCase(tableNames))
                break;
        }

        //get details related to column i.e. type and number
        HashMap<String,String> columnData=t.getColumnData();
        LinkedHashMap<String,Integer> colNum=t.getColumnNum();

        //extract where condition details (eg: operand operator operand)
        if(conditions!=null){
            for(int i=0;i<conditions.length;i++){
                StringTokenizer stringTokenizer=new StringTokenizer(conditions[i],"=><!");
                //handling condtions with operators
                if(stringTokenizer.countTokens()>1){
                    String column=stringTokenizer.nextToken();

                    type[i]=columnData.get(column.trim());
                    if(type[i].equals("varchar"))
                        type[i]="varchar";
                    columnNum[i]=colNum.get(column.trim());
                    compara[i]=stringTokenizer.nextToken();
                    operator[i]=conditions[i].substring(column.length(), conditions[i].length() - compara[i].length());
                }else if(stringTokenizer.countTokens()==1){

                    String[] column=conditions[i].split(" ");
                    type[i]=columnData.get(column[0].trim());
                    if(type[i].equals("varchar"))
                        type[i]="varchar";
                    columnNum[i]=colNum.get(column[0].trim());
                    operator[i]="like";
                    compara[i]=column[2];

                }
            }
        }

        //logic to check and print
        int lines=t.getLines();
        String records="";

        //System.out.println(tableNames);

        delim=delim.trim();
        String[] tableColumn=projectList.split(",");
        Integer[] projectColumnNum=new Integer[tableColumn.length];
        for(int i=0;i<tableColumn.length;i++)
        {
            projectColumnNum[i]=colNum.get(tableColumn[i]);
            System.out.print("\""+tableColumn[i]+"\"");
            if(i!=tableColumn.length-1)
                System.out.print(",");
        }
        System.out.println();
        if(orderClause.equals("")){


            for(int i=0;i<lines;i++)
            {
                records=dbSystem.getRecord(tableNames,i);
                if(checkCondition(conditions,columnNum,type,records,operator,compara,delim1)){
                    String[] toPrint=records.split(",");
                    for (int j=0;j<tableColumn.length;j++){
                        System.out.print(toPrint[projectColumnNum[j]]);
                        if(j!=tableColumn.length-1)
                            System.out.print(",");
                    }
                    System.out.println();
                }
            }
            System.out.println();
        }else{

            //Insert the filtered Content into the Priority Queue , which sorts on the
            //basis of the Order by arguments , and if the size of Content is more than
            //the max memory to use , then dump it as the files and count the such files
            //naming convention for those file is  PATH_FOR_DATA/temp_<i>.dat and then call
            // the external merge method and if not then poll the priority queue and display
            // the specific columns required from the polled String.
            //Alert : use zero based index

            //colNum => Mapping of Columns to their Positions
            //Mapping of column name an their type to perform order by
            String[] orderBysplit=orderClause.split(",");
            Map<String, String> orderByColumnList = new LinkedHashMap<String, String>();
            for(int i=0;i<orderBysplit.length;i++){
                orderByColumnList.put(orderBysplit[i].trim(),columnData.get(orderBysplit[i].trim()));
            }
            //System.out.println(orderByColumnList);

            //ArrayList of columns to print projectList
            ArrayList<String> printList=new ArrayList<String>();
            String[] projectListSplit=projectList.split(",");
            for(int i=0;i<projectListSplit.length;i++){
                printList.add(projectListSplit[i]);
            }

            Comparator<String> comp = new InternalOrderByComparator(orderByColumnList , colNum );
            PriorityQueue<String> priorityQueue = new PriorityQueue<String>(10 , comp);

            int fileCount = 1;
            int remainingBufferSize = MAX_MEM_TO_USE;
            int inOutFlag = 0;
            for(int i=0;i<lines;i++)  {

                records=dbSystem.getRecord(tableNames,i);
                if(checkCondition(conditions,columnNum,type,records,operator,compara,delim1)){
                    //System.out.println(records);
                    remainingBufferSize -=  records.getBytes().length;

                    if(remainingBufferSize >= 0) {
                        priorityQueue.add(records);
                    }else{
                        //write to file
                        FileWriter fileWriter = null;
                        try {
                            fileWriter = new FileWriter(new File(setBlockFile(fileCount)));
                            int siz=priorityQueue.size();
                            //System.out.println(siz);
                            for(int j = 0 ; j < siz ; j++ ){
                                fileWriter.write(priorityQueue.poll().toString());
                                //System.out.println(priorityQueue.poll().toString());
                                fileWriter.write("\n");
                            }
                            fileCount++;
                            inOutFlag = 1;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }finally {
                            try {
                                fileWriter.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        //set remainingSize back to MAX_MEM_TO_USE
                        remainingBufferSize = MAX_MEM_TO_USE;
                        //current read line
                        priorityQueue.add(records);
                        remainingBufferSize -=  records.getBytes().length;
                    }
                }
            }

            int nsiz = priorityQueue.size();
            FileWriter fileW = null;
            try {
                if(inOutFlag != 0 ){
                    fileW =  new FileWriter(new File(setBlockFile(fileCount)));
                }
                for(int j = 0 ; j < nsiz ; j++ ){

                    if(inOutFlag == 0 ){
                        //Prints the required Output
                        String outputLine = priorityQueue.poll().toString();
                        //System.out.println(outputLine);
                        String []oprv = outputLine.split(",");
                        for(int i = 0 ; i < printList.size() ; i++ ) {
                            System.out.print(oprv[colNum.get(printList.get(i))]);
                            if(i != printList.size() - 1 )    System.out.print(",");
                        }
                        System.out.println();

                    }else {

                        fileW.write(priorityQueue.poll().toString());
                        //System.out.println(priorityQueue.poll().toString());
                        fileW.write("\n");
                    }
                }
		System.out.println();
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                try {
                    if(inOutFlag != 0 ) {
                        fileW.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            //Performing the external Merge sort
            if(inOutFlag != 0 ){
                TwoPhaseMergeSort tpms = new TwoPhaseMergeSort(printList);
                tpms.externalMerge(colNum, orderByColumnList, fileCount);
            }
        }
    }

    private String setBlockFile(int block_Index_Number){
        return tempFileFolderPath + "/temp_" + block_Index_Number + ".dat";
    }

    //check record for where conditions
    private boolean checkCondition(String[] conditions, Integer[] columnNum, String[] type, String records, String[] operator, String[] compara, String delim) {
        String[] splitstring=records.split(",");
        boolean retType=true;
        if(conditions==null)
            return retType;
        if(delim.equals("and"))
            retType=true;
        else
            retType=false;
        for(int i=0;i<conditions.length;i++)
        {
            if(operator[i].trim().equals("=")){
                if(type[i].equals("float")){
                    float lval=Float.parseFloat(splitstring[columnNum[i]].replace("\"",""));
                    float rval=Float.parseFloat(compara[i].trim());
                    if(lval==rval){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }else if(type[i].equals("int")){
                    int lval=Integer.parseInt(splitstring[columnNum[i]].replace("\"", ""));
                    int rval=Integer.parseInt(compara[i].trim());
                    if(lval==rval){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }else if(type[i].equals("string")){
                    String lval=splitstring[columnNum[i]].replace("\"","");
                    String rval=compara[i].trim().replace("'", "");
                    if(lval.equals(rval)){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }


            }else if(operator[i].trim().equals(">=")){
                if(type[i].equals("float")){
                    float lval=Float.parseFloat(splitstring[columnNum[i]].replace("\"",""));
                    float rval=Float.parseFloat(compara[i].trim());
                    if(lval>=rval){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }else if(type[i].equals("int")){
                    int lval=Integer.parseInt(splitstring[columnNum[i]].replace("\"", ""));
                    int rval=Integer.parseInt(compara[i].trim());
                    if(lval>=rval){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }

            }else if(operator[i].trim().equals("<=")){
                if(type[i].equals("float")){
                    float lval=Float.parseFloat(splitstring[columnNum[i]].replace("\"",""));
                    float rval=Float.parseFloat(compara[i].trim());
                    if(lval<=rval){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }else if(type[i].equals("int")){
                    int lval=Integer.parseInt(splitstring[columnNum[i]].replace("\"", ""));
                    int rval=Integer.parseInt(compara[i].trim());
                    if(lval<=rval){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }

            }else if(operator[i].trim().equals("!=")){
                if(type[i].equals("float")){
                    float lval=Float.parseFloat(splitstring[columnNum[i]].replace("\"",""));
                    float rval=Float.parseFloat(compara[i].trim());
                    if(lval!=rval){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }else if(type[i].equals("int")){
                    int lval=Integer.parseInt(splitstring[columnNum[i]].replace("\"", ""));
                    int rval=Integer.parseInt(compara[i].trim());
                    if(lval!=rval){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }

            }else if(operator[i].trim().equals(">")){
                if(type[i].equals("float")){
                    float lval=Float.parseFloat(splitstring[columnNum[i]].replace("\"",""));
                    float rval=Float.parseFloat(compara[i].trim());
                    if(lval>rval){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }else if(type[i].equals("int")){
                    int lval=Integer.parseInt(splitstring[columnNum[i]].replace("\"", ""));
                    int rval=Integer.parseInt(compara[i].trim());
                    if(lval>rval){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }

            }else if(operator[i].trim().equals("<")){
                if(type[i].equals("float")){
                    float lval=Float.parseFloat(splitstring[columnNum[i]].replace("\"",""));
                    float rval=Float.parseFloat(compara[i].trim());
                    if(lval<rval){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }else if(type[i].equals("int")){
                    int lval=Integer.parseInt(splitstring[columnNum[i]].replace("\"", ""));
                    int rval=Integer.parseInt(compara[i].trim());
                    if(lval<rval){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }

            }else if(operator[i].trim().equals("like")){

                if(type[i].equals("string")){
                    String lval=splitstring[columnNum[i]].replace("\"","");
                    String rval=compara[i].trim().replace("'","");
                    if(lval.equalsIgnoreCase(rval)){
                        if(delim.equals("and"))
                            retType=retType&true;
                        else
                            retType=retType|true;
                    }
                    else{
                        if(delim.equals("and"))
                            retType=retType&false;
                        else
                            retType=retType|false;
                    }
                }
            }
        }
        return retType;
    }
    }
