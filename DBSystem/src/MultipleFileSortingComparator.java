import java.util.Comparator;

/**
 * Created by rishimittal on 8/4/14.
 */
public class MultipleFileSortingComparator implements Comparator<FileBuffer> {

    int table1JoinColIndex;
    String table1JoinColType;

    public MultipleFileSortingComparator(int table1JoinColIndex, String table1JoinColType) {
        this.table1JoinColIndex = table1JoinColIndex;
        this.table1JoinColType = table1JoinColType;
    }

    @Override
    public int compare(FileBuffer o1, FileBuffer o2) {
        String f_array[] = o1.peek().split(",");
        String s_array[] = o2.peek().split(",");

        // for String /varchar type
        if(table1JoinColType.equalsIgnoreCase("varchar") || table1JoinColType.equalsIgnoreCase("string")){

            return f_array[table1JoinColIndex].compareToIgnoreCase(s_array[table1JoinColIndex]);

        }else{
            //for numeric type
            Float ft = Float.parseFloat(f_array[table1JoinColIndex].replaceAll("\"", "").trim()) - Float.parseFloat(s_array[table1JoinColIndex].replaceAll("\"", "").trim());
            if(ft > 0) {
                return 1;
            }else if(ft < 0){
                return -1;
            }else{
                return 0;
            }
        }
    }
}
