import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by rishimittal on 24/1/14.
 */
public class DBSystem {


    public static String PAGE_SIZE = null;
    public static Integer NUM_PAGES = 0;
    public static String PATH_FOR_DATA = null;
    private static List<String> tables = null;
    private static HashMap<Page, Integer> prPage = null;
    public static HashMap<String, HashMap<Page, Integer>> pageInfo = null;
    private static LRUCache lruCache = null;

    public DBSystem() {
    }

    public static void readConfig(String configFilePath) {

        tables = new ArrayList<String>();

        try {

            FileInputStream fread = new FileInputStream(configFilePath);
            StringBuilder line = new StringBuilder();
            int count = 0;
            boolean flag = false;
            boolean tab =false;
            while(true) {

                int ch = fread.read();
                if (ch == -1) break;
                char c = (char)ch;
                //System.out.println(c);

                if(c == '\n') {
                    count++;
                    //System.out.println(line);
                    if(count == 1 ) {
                        //We have pagesize
                        String ar1[] = line.toString().split(" ");
                        //System.out.println("Page size : " + ar1[1]);
                        PAGE_SIZE = new String(ar1[1]);
                    }else if(count == 2 ) {
                        //we have number of pages
                        String ar1[] = line.toString().split(" ");
                        //System.out.println("Number of Pages : " + ar1[1]);
                        NUM_PAGES = Integer.parseInt(ar1[1]);
                    }else if(count == 3){
                        //we have data folder path
                        String ar1[] = line.toString().split(" ");
                        //System.out.println("Data path : " + ar1[1]);
                        PATH_FOR_DATA = new String(ar1[1]);
                    }

                    if(flag && !line.toString().equalsIgnoreCase("END")){
                        if(tab) {
                            tables.add(line.toString());
                            tab = false;
                        }
                    }

                    if(line.toString().equalsIgnoreCase("BEGIN")){
                        flag = true;
                        tab = true;
                    }else if(line.toString().equalsIgnoreCase("END")){
                        flag = false;
                    }

                    line.setLength(0);

                    continue;
                }

                line.append(c);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //For testing purpose :
        /*
        System.out.println(PAGE_SIZE);
        System.out.println(NUM_PAGES);
        System.out.println(PATH_FOR_DATA);

        for(String n : tables){
            System.out.println(n);
        }*/
    }

    public static void populateDBInfo() {

        pageInfo = new HashMap<String, HashMap<Page, Integer>>();
        FileInputStream fis = null;

        int pageCount = 0;
        int h = 0;
        int page_id=0;
        try {
        for(h = 0 ; h < tables.size() ;h++ ){
            String filename = PATH_FOR_DATA + "/" +  tables.get(h) + ".csv";
            //System.out.println(filename);
            prPage = new HashMap<Page, Integer>();
            int count = 0;
            fis = new FileInputStream(filename);
            int lineNumber = 0;
            int stLine = 1;
            int endLine = 0;
            int byteCount = 0;
            int offset=0,offset1=0;
            StringBuilder pageLine = new StringBuilder();
            int pageCapacity=Integer.parseInt(PAGE_SIZE);


            Page page=new Page();
            page.setFileOffset(0);
            page.setPageId(page_id);
            page.setStartRecordId(1);

            while(true){

                int ch = fis.read();

                if(ch == -1 ) {
                    endLine=lineNumber;
                    page.setEndRecordId(endLine);
                    prPage.put(page, 0);
                    page_id++;
                    break;
                }

                char c=(char)ch;
                pageCapacity=pageCapacity-byteCount;
                byteCount=0;
                while(c != '\n' && ch!=-1) {
                    byteCount++;
                    offset++;
                    pageLine.append(c);
                    ch=fis.read();
                    c=(char)ch;
                }
                offset++;
                lineNumber++;
                if(pageLine.length()<=pageCapacity){
                    //System.out.println(pageLine.toString());

                }else{
                    endLine=lineNumber-1;
                    page.setEndRecordId(endLine);
                    prPage.put(page, 0);
                    page=new Page();
                    //System.out.println(endLine);
                    stLine=lineNumber;
                    //System.out.println("newPage: "+pageLine.toString()+" "+stLine+" "+offset1);
                    pageCapacity=Integer.parseInt(PAGE_SIZE);
                    page_id++;
                    page.setFileOffset(offset1);
                    page.setPageId(page_id);
                    page.setStartRecordId(stLine);

                    //byteCount=0;
                }
                pageLine.setLength(0);
                offset1=offset;
            }

            lineNumber = 0;
            pageLine.setLength(0);
            byteCount = 0;
            fis.close();
            pageInfo.put(tables.get(h),prPage);

        }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        //For testing purpose

        /*
        HashMap<Page,Integer> hh=pageInfo.get("countries");
        Iterator iterator=hh.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry pairs=(Map.Entry) iterator.next();
            Page pp=(Page)pairs.getKey();
            System.out.print(" PageId : "+ pp.getPageId());
            System.out.print(" Starting Line no. " + pp.getStartRecordId());
            System.out.print(" Ending lIne no. " + pp.getEndRecordId());
            System.out.print(" File offset " + pp.getFileOffset());
            System.out.println(" InCache " + pairs.getValue());
        }
        */

        lruCache = new LRUCache(NUM_PAGES, tables, pageInfo);

    }

    public static String getRecord(String tableName, int recordId) {

        /* Iterate over the map based on the given table Name & check
        *  if the page number having the given record Id of that table
        *  is in the buffer of not , by checking the Integer value in the
        *  inner hashmap , if it is 1 that means it is present in the buffer
        *  In such cases print HIT  and if is 0 then print MISS <Page Number>
        *  and bring that block into the buffer memory.
        * */

            HashMap<Page,Integer> hh=pageInfo.get(tableName);
            Iterator iterator=hh.entrySet().iterator();
            Page rPage = null;
            int rValue = 0;
            boolean flag = false;
            //This finds the page belonging to given tablename and recordId.
            while (iterator.hasNext()){
                Map.Entry pairs=(Map.Entry) iterator.next();
                rPage= (Page)pairs.getKey();
                rValue = (Integer)pairs.getValue();
                System.out.println(rPage.getPageId() + " " + rValue);
                if(rPage.getStartRecordId() <= recordId && rPage.getEndRecordId() >= recordId){
                    flag = true;
                    break;
                }
            }

            if(!flag){
                System.out.println("N/A");
                return "string";

            }

            int i = lruCache.pageReferred(rPage, rValue, tableName);

            if(i == 0){
                //Page was not present, bring that in memory.
                hh.put(rPage, 1);
                pageInfo.put(tableName, hh);

            }else{
                /*hh.put(rPage, 0);
                System.out.println(rPage.getPageId());
                pageInfo.put(tableName, hh);*/
            }

        return "record";
    }


}
