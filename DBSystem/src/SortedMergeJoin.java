import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Created by rishimittal on 8/4/14.
 */

public class SortedMergeJoin {


    String tempFileFolderPath = DBSystem.PATH_FOR_DATA;

    String table1;
    int table1_chunks;
    int table1JoinColIndex;
    String table1JoinColType;
    int table2JoinColIndex ;
    String table2JoinColType;
    String table2;
    int table2_chunks;

    public SortedMergeJoin(String table1, int table1_chunks, int table1JoinColIndex, String table1JoinColType, String table2 , int table2_chunks, int table2JoinColIndex, String table2JoinColType ) {
        this.table1 = table1;
        this.table1_chunks = table1_chunks;
        this.table1JoinColIndex = table1JoinColIndex;
        this.table1JoinColType = table1JoinColType;
        this.table2JoinColIndex = table2JoinColIndex;
        this.table2JoinColType = table2JoinColType;
        this.table2 = table2;
        this.table2_chunks = table2_chunks;
    }


    public String getBlockFile(String tb_name , int i){

        return tempFileFolderPath + "/tem_"+ tb_name + i + ".dat";
    }

    public String getSortedFileName(String tb_name){

        return tempFileFolderPath + "/Sorted_"+ tb_name.toLowerCase() + ".dat";
    }

    public void execute(){

        int numberOfcolumnsForJoin = 2;

        for(int k = 0 ; k < numberOfcolumnsForJoin ;k++ ) {
                    String tab = null;
                    String tabColType = null;
                    int tabColIndex = 0;
                    int tab_chunks = 0;

                    if(k == 0 ) {
                        //Sorting table 1.
                        tab = table1;
                        tabColType = table1JoinColType;
                        tabColIndex = table1JoinColIndex;
                        tab_chunks = table1_chunks;
                    }else if(k == 1){
                        //Sorting table 2
                        tab = table2;
                        tabColType = table2JoinColType;
                        tabColIndex = table2JoinColIndex;
                        tab_chunks = table2_chunks;
                    }




                //FOR TABLE 1
                Comparator<FileBuffer> comp = new MultipleFileSortingComparator(tabColIndex ,tabColType );
                PriorityQueue<FileBuffer> priorityQueue = new PriorityQueue<FileBuffer>(10 , comp);
                FileWriter fileWriter = null;

                try{
                    for ( int i = 1 ; i <= tab_chunks ; i++ ) {
                        FileBuffer fb = new FileBuffer( new File( getBlockFile(tab, i)));
                        priorityQueue.add(fb);
                    }

                    fileWriter = new FileWriter(new File(getSortedFileName(tab)));

                    while(priorityQueue.size() > 0){

                        FileBuffer fbb = priorityQueue.poll();
                        String rv = fbb.pop();

                        fileWriter.write(rv);
                        fileWriter.write("\n");
                        if(fbb.empty()){
                            fbb.fbr.close();
                            fbb.temp_file.delete();
                        }else{
                            priorityQueue.add(fbb);
                        }
                    }
                    //System.out.println("file merged");
                }catch (IOException e) {
                    e.printStackTrace();
                }finally {
                    try {
                        for(FileBuffer fbb : priorityQueue) {
                                fbb.close();
                         }
                        fileWriter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
    }
}
