import java.util.Comparator;
import java.util.Map;

/**
 * Created by rishimittal on 3/3/14.
 */


// Used when the external Merge sort is not required
// Used when the sorting inside the blocks

public class InternalOrderByComparator implements Comparator<String>{

    private Map<String, String> orderByColumns;
    // <col-name>, <col-index>
    private Map<String,Integer > columnIndex;


    public InternalOrderByComparator(Map<String, String> vp, Map<String, Integer> columnIndex) {
        this.orderByColumns = vp;
        this.columnIndex = columnIndex;
    }

    @Override
    public int compare(String str1, String str2) {

        boolean trip = false;

        String fArray[] =  str1.split(",");
        String sArray[] =  str2.split(",");


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
