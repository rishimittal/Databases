import java.util.*;

/**
 * Created by rishimittal on 3/3/14.
 */
public class OrderByComparator implements Comparator<FileBuffer> {

    //<col-name> , <col-type>
    private Map<String, String> orderByColumns;
    // <col-name>, <col-index>
    private Map<String,Integer > columnIndex;

    public OrderByComparator(Map<String, String> vp, Map<String, Integer> columnIndex) {
         this.orderByColumns = vp;
         this.columnIndex = columnIndex;
    }

    //Returns positive if col in str1 > col in str2
    //Return negative if col in str1 < col in str2

    @Override
    public int compare(FileBuffer str1, FileBuffer str2) {

        boolean trip = false;

        String fArray[] =  str1.peek().split(",");
        String sArray[] =  str2.peek().split(",");


        //System.out.println(fArray[].toString());
        //System.out.println(sArray[0].toString());

        for (Map.Entry<String, String> entry : orderByColumns.entrySet()) {
                String colName = entry.getKey();
                String colType = entry.getValue();
                //returns the index on the col value is stored
                int index = columnIndex.get(colName);
                //System.out.println(fArray[index].replaceAll("\"", "").trim());

                if(colType.equalsIgnoreCase("string") || colType.equalsIgnoreCase("varchar")){
                    //string/varchar type
                    if(fArray[index].compareTo(sArray[index]) == 0){
                        //Do Nothing, and wait for next comparison
                    }else{
                        return fArray[index].compareTo(sArray[index]);
                    }

                }else{
                    //numeric type
                    if(Float.parseFloat(fArray[index].replaceAll("\"", "").trim()) - Float.parseFloat(sArray[index].replaceAll("\"","").trim()) == 0){
                        //Do Nothing, and wait for next comparison
                    }else{

                        Float f = Float.parseFloat(fArray[index].replaceAll("\"", "").trim()) - Float.parseFloat(sArray[index].replaceAll("\"", "").trim());
                        if(f > 0) {
                            return 1;
                        }else{
                            return -1;
                        }

                    }

                }
        }

        return 0;
    }



}
