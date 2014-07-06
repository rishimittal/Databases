import java.util.HashMap;

/**
 * Created by neel on 12/2/14.
 */
public class Table {

    private String name;
    private HashMap<String,String> columnData;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public HashMap<String, String> getColumnData() {
        return columnData;
    }

    public void setColumnData(HashMap<String, String> columnData) {
        this.columnData = columnData;
    }
}
