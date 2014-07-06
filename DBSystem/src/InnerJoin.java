import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * Created by rishimittal on 8/4/14.
 */
public class InnerJoin {

    public static int MAX_MEM_TO_USE = DBSystem.NUM_PAGES * DBSystem.PAGE_SIZE;
    public static String tempFileFolderPath = DBSystem.PATH_FOR_DATA;
    public static int table1_tempfileCount;
    public static int table2_tempfileCount;
    public static String tab1;
    public static String tab2;
    public static int table1JoinColIndex;
    public static String table1JoinColType;
    public static int table2JoinColIndex ;
    public static String table2JoinColType;
    public static int table1Lines;
    public static int table2Lines;
    public static Table t1;
    public static Table t2;

    //Takes the input file from the database and sorts the file
    // store that file on the disk for further usage

    private static String setBlockFile(int block_Index_Number, String t_name){
        return tempFileFolderPath + "/tem_"+ t_name + block_Index_Number + ".dat";
    }

    private static String getSortedFile(String tab_name){
        return tempFileFolderPath + "/Sorted_"+ tab_name.toLowerCase() + ".dat";
    }

    private static int compareTabLines(String tab1_line, String tab2_line){

        String f_arr[] = tab1_line.split(",");
        String s_arr[] = tab2_line.split(",");

        // for String /varchar type
        if(table1JoinColType.equalsIgnoreCase("varchar") || table1JoinColType.equalsIgnoreCase("string")){

            return f_arr[table1JoinColIndex].compareToIgnoreCase(s_arr[table2JoinColIndex]);

        }else{
            //for numeric type
            //System.out.println("ppp");
            //System.out.println(f_arr[table1JoinColIndex].replaceAll("\"", "").trim() + " ---" + s_arr[table2JoinColIndex].replaceAll("\"", "").trim());
            Float ft = Float.parseFloat(f_arr[table1JoinColIndex].replaceAll("\"", "").trim()) - Float.parseFloat(s_arr[table2JoinColIndex].replaceAll("\"", "").trim());
            if(ft > 0) {
                //System.out.println("kkk");
                return 1;
            }else if(ft < 0){
                return -1;
            }else{
                return 0;
            }
        }
    }

    private static int compareTab1Line(String t_line1, String t_line2){
        String f_arr[] = t_line1.split(",");
        String g_arr[] = t_line2.split(",");

        // for String /varchar type
        if(table1JoinColType.equalsIgnoreCase("varchar") || table1JoinColType.equalsIgnoreCase("string")){

            return f_arr[table1JoinColIndex].compareToIgnoreCase(g_arr[table1JoinColIndex]);

        }else{
            //for numeric type
            Float ft = Float.parseFloat(f_arr[table1JoinColIndex].replaceAll("\"", "").trim()) - Float.parseFloat(g_arr[table1JoinColIndex].replaceAll("\"", "").trim());
            if(ft > 0) {
                return 1;
            }else if (ft < 0){
                return -1;
            }else{
                return 0;
            }
        }
    }

    private static int compareTab2Line(String t_line1, String t_line2){
        String f_arr[] = t_line1.split(",");
        String g_arr[] = t_line2.split(",");
        // for String /varchar type
        if(table1JoinColType.equalsIgnoreCase("varchar") || table1JoinColType.equalsIgnoreCase("string")){

            return f_arr[table2JoinColIndex].compareToIgnoreCase(g_arr[table2JoinColIndex]);

        }else{
            //for numeric type
            Float ft = Float.parseFloat(f_arr[table2JoinColIndex].replaceAll("\"", "").trim()) - Float.parseFloat(g_arr[table2JoinColIndex].replaceAll("\"", "").trim());
            if(ft > 0) {
                return 1;
            }else if(ft < 0){
                return -1;
            }else{
                return 0;
            }
        }

    }

    //Sorts Both the file , individually
    //irrespective of their size.
    public static void initialize(String table1,String table1_join_col , String table2, String table2_join_col){

        //System.out.println(DBSystem.PATH_FOR_DATA);
        //System.out.println(table1);

        tab1 = table1;
        tab2 = table2;

        for(Table t : DBSystem.tableList){

            if(t.getName().equalsIgnoreCase(table1)){
                table1JoinColIndex = t.getColumnNum().get(table1_join_col);
                table1JoinColType = t.getColumnData().get(table1_join_col);
                table1Lines = t.getLines();
                t1 = t;
            }else if(t.getName().equalsIgnoreCase(table2)){
                table2JoinColIndex = t.getColumnNum().get(table2_join_col);
                table2JoinColType = t.getColumnData().get(table2_join_col);
                table2Lines = t.getLines();
                t2 = t;
            }

        }
        //System.out.println(table1 + " - " + table1_join_col + " - " + table1JoinColIndex + " - " + table1JoinColType);
        //System.out.println(table2 + " - " + table2_join_col + " - " + table2JoinColIndex + " - " + table2JoinColType);

        if(!table1JoinColType.equalsIgnoreCase(table2JoinColType)){
            System.out.println("Both the tables must have similar join types");
            System.exit(1);
        }

        int numberOfcolumnsForJoin = 2; // For Joining Two tables

        //Sort the columns used for join
        for(int k = 0 ; k < numberOfcolumnsForJoin ;k++ ) {
            String tab = null;
            String tabColType = null;
            int tabColIndex = 0;
            int tLines = 0;

            if(k == 0 ) {
                //Sorting table 1.
                tab = table1;
                tabColType = table1JoinColType;
                tabColIndex = table1JoinColIndex;
                tLines = table1Lines;
            }else if(k == 1){
                //Sorting table 2
                tab = table2;
                tabColType = table2JoinColType;
                tabColIndex = table2JoinColIndex;
                tLines = table2Lines;
            }

            Comparator<String> cmp = new SortingComparator(tabColIndex , tabColType);
            PriorityQueue<String> priorityQueue = new PriorityQueue<String>(10, cmp);

            int remainingBufferSize = MAX_MEM_TO_USE;
            int fileCount = 1;
            String line ;
            int inOutFlag = 0;

                for(int i=0;i<tLines;i++)
                {
                    line=DBSystem.getRecord(tab,i);


                    //System.out.println(line);
                    //String cols [] = line.split(",");

                    remainingBufferSize -= line.getBytes().length;
                    if(remainingBufferSize >= 0) {
                        priorityQueue.add(line);
                        inOutFlag = 1;
                    }else{
                        //write to file
                        FileWriter fileWriter = null;
                        //System.out.println("Written");
                        try {
                            //String fileName = setBlockFile(fileCount,table1);
                            //System.out.println(fileName);
                            fileWriter = new FileWriter(new File(setBlockFile(fileCount, tab)));
                            int siz=priorityQueue.size();
                            //System.out.println(siz);
                            for(int j = 0 ; j < siz ; j++ ){
                                fileWriter.write(priorityQueue.poll().toString());
                                //System.out.println(priorityQueue.poll().toString());
                                fileWriter.write("\n");
                            }
                            fileCount++;
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
                        priorityQueue.add(line);
                        remainingBufferSize -=  line.getBytes().length;
                        inOutFlag = 0;
                    }
                }

            if(inOutFlag == 1) {
                FileWriter fw = null;

                try {
                    fw = new FileWriter(new File(setBlockFile(fileCount, tab)));
                    int psiz=priorityQueue.size();
                    for(int j = 0 ; j < psiz ; j++ ){
                        fw.write(priorityQueue.poll().toString());
                        //System.out.println(priorityQueue.poll().toString());
                        fw.write("\n");
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }finally {
                    try {
                        fw.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            if(k == 0 ) table1_tempfileCount = fileCount;
            if(k == 1) table2_tempfileCount = fileCount ;

        }

        // Begin the merging process for both the files.

        //System.out.println(table1_tempfileCount);
        //System.out.println(table2_tempfileCount);

        //Merging Process
        SortedMergeJoin sortedMergeJoin = new SortedMergeJoin(table1, table1_tempfileCount , table1JoinColIndex , table1JoinColType ,table2, table2_tempfileCount,table2JoinColIndex , table2JoinColType);
        sortedMergeJoin.execute();

    }

    //Now finding the common lines from both the files.
    //public static void printJoinResult(List<String> tab1_attr, List<String> tab2_attr){
    public static void printJoinResult(List<String> tab1_attr, List<String> tab2_attr){
        int i = 0;
        int j = 0;

        BufferedReader br_tab1 = null;
        BufferedReader br_tab2 = null;


            try {
                br_tab1 = new BufferedReader(new FileReader(new File(getSortedFile(tab1))));
                br_tab2 = new BufferedReader(new FileReader(new File(getSortedFile(tab2))));

                String tab1_line = null;
                String tab2_line = null;

                tab1_line = br_tab1.readLine();
                tab2_line = br_tab2.readLine();


                while (tab1_line != null && tab2_line != null ){
                    //System.out.println("PO");

                    //System.out.println(tab1_line + " with " + tab2_line );
                    int op = compareTabLines(tab1_line, tab2_line);

                    if(op == 0 )  {//both are equal
                         //System.out.println(tab1_line + " --" + tab2_line);
                        List<String> ltab1 = new ArrayList<String>();
                        List<String> ltab2 = new ArrayList<String>();

                        ltab1.add(tab1_line);
                        ltab2.add(tab2_line);

                        String t1_line = br_tab1.readLine();
                        String t2_line = br_tab2.readLine();

                        if(t1_line != null ) {

                            int y;
                            //System.out.println(tab1_line + "=========" + t1_line);
                            while( (y = compareTab1Line(tab1_line, t1_line)) == 0 ){
                                ltab1.add(t1_line);
                                t1_line = br_tab1.readLine();
                            }

                        }
                        if(t2_line != null ){
                            int u;
                            //System.out.println(tab2_line + "++++++++++++" + t2_line + compareTabLines(tab2_line, t2_line));
                            while((u = compareTab2Line(tab2_line, t2_line)) == 0 ){
                                ltab2.add(t2_line);
                                t2_line = br_tab2.readLine();
                            }
                        }
                        HashMap<String,Integer> colNum1=null,colNum2=null;
                        Iterator<Table> it=DBSystem.tableList.iterator();
                        while (it.hasNext()){
                            Table t=it.next();
                            if(t.getName().equalsIgnoreCase(tab1))
                            {
                                colNum1=t.getColumnNum();
                            }
                            if(t.getName().equalsIgnoreCase(tab2))
                            {
                                colNum2=t.getColumnNum();
                            }
                        }


                        /*Iterator<String> it1=tab1_attr.iterator();
                        while (it1.hasNext()){
                            System.out.print(tab1 + "." + it1.next());
                            if(it1.hasNext())
                                System.out.print(",");
                        }
                        Iterator<String> it2=tab2_attr.iterator();
                        while (it2.hasNext()){
                            System.out.print(tab2 + "." + it2.next());
                            if(it2.hasNext())
                                System.out.print(",");
                        }*/
                        for(int q = 0 ; q < ltab1.size() ; q++ ) {
                            for(int w = 0 ; w < ltab2.size() ; w++ ){
                                //System.out.println(ltab1.get(q) + " -- " + ltab2.get(w));
                                String[] tab1_result=ltab1.get(q).split(",");
                                String[] tab2_result=ltab2.get(w).split(",");
                                Iterator<String> it1=tab1_attr.iterator();
                                while (it1.hasNext()){
                                    int c=colNum1.get(it1.next());
                                    System.out.print(tab1_result[c]+",");

                                }
                                Iterator<String> it2=tab2_attr.iterator();
                                while (it2.hasNext()){
                                    int c=colNum2.get(it2.next());
                                    System.out.print(tab2_result[c]);
                                    if(it2.hasNext())
                                        System.out.print(",");
                                }
                                System.out.println();
                            }
                        }

                        tab1_line = t1_line;
                        tab2_line = t2_line;
                    }else if(op >= 1 ) { // table1 has bigger value

                        tab2_line = br_tab2.readLine();

                    }else if(op <= -1) { // table2 has bigger value

                        tab1_line = br_tab1.readLine();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
}
