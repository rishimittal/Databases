import java.util.Comparator;

/**
 * Created by rishimittal on 8/4/14.
 */
public class SortingComparator implements Comparator<String>{

    int table1JoinColIndex;
    String table1JoinColType;

    public SortingComparator(int table1JoinColIndex, String table1JoinColType) {
        this.table1JoinColIndex = table1JoinColIndex;
        this.table1JoinColType = table1JoinColType;
    }

    @Override
    public int compare(String o1, String o2) {

        String f_array[] = o1.split(",");
        String s_array[] = o2.split(",");
        // for String /varchar type
        if(table1JoinColType.equalsIgnoreCase("varchar") || table1JoinColType.equalsIgnoreCase("string")){

            return f_array[table1JoinColIndex].compareToIgnoreCase(s_array[table1JoinColIndex]);

        }else{
        //for numeric type
            Float ft = Float.parseFloat(f_array[table1JoinColIndex].replaceAll("\"", "").trim()) - Float.parseFloat(s_array[table1JoinColIndex].replaceAll("\"", "").trim());
            if(ft > 0) {
                return 1;
            }else if (ft < 0){
                return -1;
            }else{
                return 0;
            }
        }
    }
}
