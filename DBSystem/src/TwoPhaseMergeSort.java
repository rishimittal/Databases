import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by rishimittal on 2/3/14.
 */
public class TwoPhaseMergeSort {

    String tempFileFolderPath = DBSystem.PATH_FOR_DATA;
    private ArrayList<String> projectList;

    public TwoPhaseMergeSort(ArrayList<String> projectList) {
        this.projectList = projectList;
    }

    //Input :
     // 1. Map of columns and column types <Column name> , <column type> after applying where  condition *
     // 2. Number of files to merge. *
     // 3. Files in proper named convention to merge.
     // 4. Path of the folder having those files *
     // 5. Maximum size of the block to be read , before dumped in file. *
     // 6. Map of columns and their index *
     // Output :
     // 1.Merged output line by line

    private String getBlockFile(int block_Index_Number){
        return tempFileFolderPath + "/temp_" + block_Index_Number + ".dat";
    }

    /*
     public void merge(){

        //Alert : use zero based index
        Map<String, Integer> columnIndex = new HashMap();
        columnIndex.put("id", 0);
        columnIndex.put("code", 1);
        columnIndex.put("name", 2);
        columnIndex.put("continent",3);

        Map<String, String> orderByColumnList = new LinkedHashMap<String, String>();
        orderByColumnList.put("code", "string");
        orderByColumnList.put("id", "int");
        orderByColumnList.put("continent", "string");

        Comparator<String> comp = new InternalOrderByComparator(orderByColumnList , columnIndex);
        PriorityQueue<String> open = new PriorityQueue<String>(10 , comp);
        open.add("\"302673.34\",\"AA\",\"Albania\",\"EU\"");
        open.add("\"302673.34\",\"AL\",\"Armenia\",\"AS\"");
        open.add("\"302556.34\",\"AL\",\"Angola\",\"AF\"");

         System.out.println(open.poll());
         System.out.println(open.poll());
         System.out.println(open.poll());
     }

    */

     //columnIndex : Map having columns mapped with the index(zero based)
     //orderByColumnList : Map having column arguments of order by with their type
     // Before applying this method , first sort the data according to the order by comparator and dump them in multiple files.

     public int externalMerge(LinkedHashMap<String, Integer> columnIndex , Map<String, String> orderByColumnList, int noOfFiles){

         Comparator<FileBuffer> comp = new OrderByComparator(orderByColumnList , columnIndex);
         PriorityQueue<FileBuffer> priorityQueue = new PriorityQueue<FileBuffer>(10 , comp);

         try{
             for ( int i = 1 ; i <= noOfFiles ; i++ ) {
                 FileBuffer fb = new FileBuffer( new File( getBlockFile(i)));
                 priorityQueue.add(fb);
             }

             while(priorityQueue.size() > 0){

                    FileBuffer fbb = priorityQueue.poll();
                    String rv = fbb.pop();

                    //Prints the required Output
                    //Print only specific cols
                    String []oprv = rv.split(",");
                    for(int i = 0 ; i < projectList.size() ; i++ ) {
                        System.out.print(oprv[columnIndex.get(projectList.get(i))]);
                        if(i != projectList.size() - 1 )    System.out.print(",");
                    }
                    System.out.println();
                    //System.out.println(rv);

                    if(fbb.empty()){
                        fbb.fbr.close();
                        fbb.temp_file.delete();
                    }else{
                        priorityQueue.add(fbb);
                    }
             }
             System.out.println();
             return 1;
         }catch (IOException e) {
             e.printStackTrace();
         }finally {
             for(FileBuffer fbb : priorityQueue)
                 try {
                     fbb.close();
                 } catch (IOException e) {
                     e.printStackTrace();
                 }
         }
         return 0;
     }

    /*
    public static void main(String arr[]){

        ArrayList<String> projectList = new ArrayList<String>();
        projectList.add("name");
        projectList.add("id");

        TwoPhaseMergeSort tpms = new TwoPhaseMergeSort(projectList);
        tpms.merge();
    }
    */

}
