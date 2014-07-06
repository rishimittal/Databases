import java.io.*;
import java.util.*;

/**
 * Created by neel on 26/1/14.
 */
public class DBSystem {

    private static ArrayList<String> tables=null;
    private static int PAGE_SIZE;
    private static int NUM_PAGES;
    public static String PATH_FOR_DATA;
    public static String CONFIG_FILE_PATH;

    private static HashMap<Page,Integer> pageDetail=null;
    private static HashMap<String,HashMap<Page,Integer>> pageInfo=null;
    private static List<Page> buffer=null;

    public static List<Table> tableList;

    /*void readConfig(String configFilePath)
    {
        You need to read the configuration file and extract the page size
        and number of pages (these two parameter together define the
        maximum main memory you can use). Values are in number of
        bytes.
        You should read the names of the tables from the configuration file.
        You can assume that the table data exists in a file named
        <table_name>.csv at the path pointed by the config parameter
        PATH_FOR_DATA.
        You will need other metadata information given in config file for
        future deliverables.
    }
*/
    public static void readConfig(String configFilePath){
        CONFIG_FILE_PATH = configFilePath;
        tables = new ArrayList<String>();

        try {

            FileInputStream fread = new FileInputStream(configFilePath);
            StringBuilder line = new StringBuilder();
            int count = 0;
            boolean flag = false;
            boolean tab =false;
            Table table=null;
            HashMap<String,String> columnData=null;
            tableList=new ArrayList<Table>();
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
                        PAGE_SIZE = Integer.parseInt(ar1[1]);
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
                            table.setName(line.toString());
                        }else {
                            String name=line.toString().substring(0,line.toString().indexOf(','));
                            String type=line.toString().substring(line.toString().indexOf(',')+1).trim();
                            if(type.contains("(")){
                                type=type.substring(0,type.indexOf('('));
                            }
                            columnData.put(name.toLowerCase(),type.toLowerCase());
                        }
                    }
                    //Reads between the begin and End lines
                    if(line.toString().equalsIgnoreCase("BEGIN")){
                        flag = true;
                        tab = true;
                        table=new Table();
                        columnData=new HashMap<String, String>();
                    }else if(line.toString().equalsIgnoreCase("END")){
                        flag = false;
                        table.setColumnData(columnData);
                        tableList.add(table);
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
        }
        System.out.println(tables.toString());*/
    }


    /*  The data present in each table needs to be represented in pages.
        Read the file corresponding to each table line by line (for now
        assume 1 line = 1 record)
        Maintain a mapping from PageNumber to (StartingRecordId,
        EndingRecordId) in memory.
        You can assume unspanned file organisation and record length will
        not be greater than page size. */

    public static void populateDBInfo(){
        pageInfo=new HashMap<String, HashMap<Page, Integer>>();
        buffer=new LinkedList<Page>();
        for(String n : tables){
            try {
                pageDetail=new HashMap<Page,Integer>();
                Page page=new Page();
                FileInputStream fread = new FileInputStream(PATH_FOR_DATA+"/"+n+".csv");
                int inp=0, len=0, currentSize=PAGE_SIZE+1, lineNo=-1, pageID=0, offs=0;
                initializePage(page,pageID,lineNo+1,offs,n);
                do{
                    do{
                        inp=fread.read();
                        len++;
                    }while (inp!=10 && inp!=-1);

                    offs=offs+len;
                    lineNo++;

                    if(len<=currentSize){
                        currentSize=currentSize-len;
                        if(inp==-1){
                            initializePage(page,lineNo-1, currentSize);
                            pageDetail.put(page,0);
                        }
                        //Change this implementation to PAGE_SIZE+1
                    }
                    else{
                        initializePage(page,lineNo-1, currentSize);
                        pageDetail.put(page,0);
                        page=new Page();
                        pageID++;
                        initializePage(page,pageID,lineNo,offs-len,n);
                        currentSize=PAGE_SIZE-len;
                    }
                    len=0;
                }while (inp!=-1);
                fread.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            pageInfo.put(n, pageDetail);
        }

    }

    /* Get the corresponding record of the specified table.
        DO NOT perform I/O every time. Each time a request is received, if
        the page containing the record is already in memory, return that
        record else bring corresponding page in memory. You are supposed
        to implement LRU page replacement algorithm for the same. Print
        HIT if the page is in memory, else print
        MISS <pageNumber> where <pageNumber> is the page number of
        memory page which is to be replaced. (You can assume page
        numbers starting from 0. So, you have total 0 to <NUM_PAGES ­ 1>
        pages.) */

    public static String getRecord(String tableName, int recordld){
        HashMap<Page,Integer> hm=pageInfo.get(tableName);
        Iterator it = hm.entrySet().iterator();
        Page p;
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
            p= (Page) pairs.getKey();
            if(p.getStartRecord()<=recordld && p.getEndRecord()>=recordld){
                int exist= hm.get(p);
                if(exist==1){
                    System.out.println("HIT");
                    int pageAddBuff=0;
                    for(Page p1: buffer){
                        if(p1.getPageID()==p.getPageID()){
                            break;
                        }
                        pageAddBuff++;
                    }
                    Page existingPage=buffer.get(pageAddBuff);
                    buffer.remove(pageAddBuff);
                    buffer.add(0,existingPage);
                    List<String> recContent = existingPage.getPageContent();
                    return recContent.get(recordld-p.getStartRecord());
                }else{

                    if(buffer.size()==NUM_PAGES){

                        Page p1=buffer.get(buffer.size()-1);
                        int fNo=p1.getFrameNo();
                        p.setFrameNo(fNo);
                        p1.setFrameNo(-1);
                        HashMap<Page,Integer> hm1=pageInfo.get(p1.getTableName());
                        hm1.put(p1,0);
                        hm.put(p,1);
                        buffer.remove(buffer.size() - 1);

                        Page fp=addContentsToPage(p,tableName);

                        buffer.add(0,fp);
                        System.out.println("MISS "+fNo);
                        List<String> recContent = fp.getPageContent();
                        return recContent.get(recordld-p.getStartRecord());

                    }else{

                        int fNo=buffer.size();
                        p.setFrameNo(fNo);
                        Page fp=addContentsToPage(p,tableName);
                        buffer.add(0,fp);

                        System.out.println("MISS "+fNo);
                        hm.put(p,1);
                        List<String> recContent = fp.getPageContent();
                        return recContent.get(recordld-p.getStartRecord());
                    }
                }

            }
        }
        return "REC";
    }

    public static void insertRecord(String tableName, String record){
        HashMap<Page,Integer> hm=pageInfo.get(tableName);
        Iterator it = hm.entrySet().iterator();
        Page p,p1 = null;
        int max=0;
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
            p= (Page) pairs.getKey();
            if(p.getPageID()>=max){
                p1=p;
                max=p.getPageID();
            }
        }
        //System.out.println(p1.getPageID());
        int endR=p1.getEndRecord();
        if(p1.getCurrentSize()>=record.length()){
            p1.setEndRecord(endR+1);
            if(p1.getCurrentSize()==record.length())
                p1.setCurrentSize(p1.getCurrentSize()-record.length());
            else
                p1.setCurrentSize(p1.getCurrentSize()-record.length()-1);
        }else{
            int pageId=getMaxPageID();
            Page page=new Page();
            page.setPageID(pageId+1);
            page.setStartRecord(endR+1);
            page.setEndRecord(endR+1);
            page.setFileOffset(p1.getFileOffset()+PAGE_SIZE-p1.getCurrentSize());
            page.setTableName(tableName);
            if(record.length()==PAGE_SIZE)
                page.setCurrentSize(PAGE_SIZE-record.length());
            else
                page.setCurrentSize(PAGE_SIZE-record.length()-1);
            hm.put(page,0);     //DEBUG if failure
        }

        try {
            FileWriter fileWriter=new FileWriter(PATH_FOR_DATA+"/"+tableName+".csv",true);
            fileWriter.write(record+"\n");
            fileWriter.flush();
            fileWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        updateRecordBuffer(tableName,endR+1);
    }

    public static void test(){
        Iterator<Table> it=tableList.iterator();
        while (it.hasNext()){
            Table t=it.next();
            System.out.println(t.getName() + " " + t.getColumnData());
        }
        
        /*HashMap<Page,Integer> hm=pageInfo.get("countries");
        Iterator it = hm.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
            Page p= (Page) pairs.getKey();
            System.out.println("Pageid "+p.getPageID()+" Start rec "+p.getStartRecord()+" End rec "+p.getEndRecord()+" Offset "+p.getFileOffset()+" Size remaining "+p.getCurrentSize());
        }*/
    }

    private static void initializePage(Page page, int endR, int curr) {
        page.setEndRecord(endR);
        page.setCurrentSize(curr);
    }

    private static void initializePage(Page page, int pageID, int startR, int fileO, String tab) {
        page.setPageID(pageID);
        page.setStartRecord(startR);
        page.setFileOffset(fileO);
        page.setTableName(tab);
    }

    /* This method receives the page without the String contents
     * and returns the page with contents.
    */
    public static Page addContentsToPage(Page p, String tableName){
        int bytePage = PAGE_SIZE;
        try {
            RandomAccessFile rfile = new RandomAccessFile(DBSystem.PATH_FOR_DATA + "/" + tableName + ".csv","r");
            rfile.seek(p.getFileOffset());
            List<String> pageRecords = new ArrayList<String>();
            StringBuilder recordLine = new StringBuilder();
            while(bytePage > 0){
                int ch = rfile.read();
                char c = (char)ch;
                //System.out.println((char)ch);
                if(c == '\n'){
                    pageRecords.add(recordLine.toString());
                    //System.out.println(recordLine);
                    bytePage--;
                    recordLine.setLength(0);
                    continue;
                }
                recordLine.append(c);
                bytePage--;
                if(bytePage == 0){
                    pageRecords.add(recordLine.toString());
                    // System.out.println(recordLine);
                    recordLine.setLength(0);
                }
            }
            p.setPageContent(pageRecords);
            //p.setFrameNo(i);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return p;
    }

    public static void updateRecordBuffer(String tableName, int recordld){
        HashMap<Page,Integer> hm=pageInfo.get(tableName);
        Iterator it = hm.entrySet().iterator();
        Page p;
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
            p= (Page) pairs.getKey();
            if(p.getStartRecord()<=recordld && p.getEndRecord()>=recordld){
                int exist= hm.get(p);
                if(exist==1){
                    //System.out.println("HIT");
                    int pageAddBuff=0;
                    for(Page p1: buffer){
                        if(p1.getPageID()==p.getPageID()){
                            break;
                        }
                        pageAddBuff++;
                    }
                    Page existingPage=buffer.get(pageAddBuff);
                    buffer.remove(pageAddBuff);
                    Page fp=addContentsToPage(existingPage,tableName);
                    buffer.add(0,fp);
                    //List<String> recContent = existingPage.getPageContent();
                    //return recContent.get(recordld-p.getStartRecord());
                }else{

                    if(buffer.size()==NUM_PAGES){

                        Page p1=buffer.get(buffer.size()-1);
                        int fNo=p1.getFrameNo();
                        p.setFrameNo(fNo);
                        p1.setFrameNo(-1);
                        HashMap<Page,Integer> hm1=pageInfo.get(p1.getTableName());
                        hm1.put(p1,0);
                        hm.put(p,1);
                        buffer.remove(buffer.size() - 1);

                        Page fp=addContentsToPage(p,tableName);

                        buffer.add(0,fp);
                        //System.out.println("MISS "+fNo);
                        //List<String> recContent = fp.getPageContent();
                        //return recContent.get(recordld-p.getStartRecord());

                    }else{

                        int fNo=buffer.size();
                        p.setFrameNo(fNo);
                        Page fp=addContentsToPage(p,tableName);
                        buffer.add(0,fp);

                        //System.out.println("MISS "+fNo);
                        hm.put(p,1);
                        //List<String> recContent = fp.getPageContent();
                        //return recContent.get(recordld-p.getStartRecord());
                    }
                }

            }
        }
    }

    private static int getMaxPageID() {
        int pageId=0;
        for(String n : tables){
            HashMap<Page,Integer> hm=pageInfo.get(n);
            Iterator it = hm.entrySet().iterator();
            Page p;
            while (it.hasNext()) {
                Map.Entry pairs = (Map.Entry)it.next();
                p= (Page) pairs.getKey();
                if(p.getPageID()>=pageId){
                    pageId=p.getPageID();
                }
            }
        }
        return pageId;
    }
}
