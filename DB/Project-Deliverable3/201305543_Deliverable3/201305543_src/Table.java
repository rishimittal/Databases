import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * Created by neel on 12/2/14.
 */
public class Table {

    private String name;
    private String primary_key;
    private HashMap<String,String> columnData;

    public int getLines() {
        return lines;
    }

    public void setLines(int lines) {
        this.lines = lines;
    }

    private int lines;
    public LinkedHashMap<String, Integer> getColumnNum() {
        return columnNum;
    }

    public void setColumnNum(LinkedHashMap<String, Integer> columnNum) {
        this.columnNum = columnNum;
    }

    private LinkedHashMap<String,Integer> columnNum;
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

    public String getPrimary_key() {
        return primary_key;
    }

    public void setPrimary_key(String primary_key) {
        this.primary_key = primary_key;
    }
}
