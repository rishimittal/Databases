import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by rishimittal on 26/1/14.
 */
public class LRUCache {

    private List<Page> buffer = null;
    private List<Integer> frameNumbers = null;
    private List<String> tables = null;
    private HashMap<String, HashMap<Page, Integer>> pageInfo = null;
    private int bufferSize = 0;

    public LRUCache(int bufferSize, List<String> tables,HashMap<String, HashMap<Page, Integer>> pageInfo) {
        buffer = new LinkedList<Page>();
        this.bufferSize = bufferSize;
        this.tables = tables;
        this.pageInfo = pageInfo;
    }

    public List<Page> getBuffer() {
        return buffer;
    }

    public void setBuffer(List<Page> buffer) {
        this.buffer = buffer;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int pageReferred(Page rPage, int rValue, String tableName){

        if(rValue == 1) {
            //Page found in buffer
            System.out.println("HIT");
            //find the index of the page in the buffer from the map
            int pageAddressInBuffer = 0;

            for(Page p1 : buffer ){
                //System.out.println(p1.getPageId()+" "+rPage.getPageId());
                if(p1.getPageId() == rPage.getPageId()){
                    break;
                }
                pageAddressInBuffer++;
            }
            //System.out.println(pageAddressInBuffer);
            //System.out.println("will replace");
            buffer.remove(pageAddressInBuffer);
            //Again change to 0 in the pageInfo Map
            Page fPage = addContentsToPage(rPage, tableName, buffer.size() - 1);
            buffer.add(0, fPage);

            pageAddressInBuffer = 0;



            return 0;

        }else{

            //Page not found in buffer
            if(buffer.size() == bufferSize){

                //System.out.println(buffer.size());
                //pl.put(buffer.get(buffer.size() ), 0);
                //System.out.println(buffer.get(buffer.size() - 1).getPageId());

                Page p = buffer.get(buffer.size()-1);
                int fNo = p.getFrameNo();

                HashMap<Page, Integer> pl = pageInfo.get(getTableNameFromPageId(p.getPageId()));
                pl.put(p,0);

                //p.setFrameNo(-1);
                //System.out.println(p.getPageId());


                //
                //System.out.println(pl.get(p));
                //pageInfo.put(tableName, pl);

                //Use LRU to replace a page
                buffer.remove(buffer.size() - 1);
                System.out.println("MISS "+fNo);
                //Add the page later
                Page fPage = addContentsToPage(rPage, tableName, fNo);
                buffer.add(0,fPage);

                //System.out.println(pageAddressInBuffer);

            }else{
                System.out.println("MISS "+(buffer.size()));
                Page fPage = addContentsToPage(rPage, tableName, buffer.size());
                buffer.add(0,fPage);
            }
            return 0;
        }
}

    /* This method receives the page without the String contents
     * and returns the page with contents.
    */
    public Page addContentsToPage(Page p, String tableName, int i){

        int bytePage = Integer.parseInt(DBSystem.PAGE_SIZE);
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
            }

            recordLine.append(c);
            bytePage--;

            if(bytePage == 0){
                pageRecords.add(recordLine.toString());
                // System.out.println(recordLine);
                recordLine.setLength(0);
            }
        }
        p.setPageContents(pageRecords);
        p.setFrameNo(i);
    } catch (FileNotFoundException e) {
            e.printStackTrace();
    } catch (IOException e) {
            e.printStackTrace();
    }
        return p;
    }

    public String getTableNameFromPageId(int pageId){

        int i = 0;
        boolean flag = false;
        for(i = 0 ; i < tables.size() ; i++){
            HashMap<Page,Integer> hh=pageInfo.get(tables.get(i));
            Iterator iterator=hh.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry pairs=(Map.Entry) iterator.next();
                Page pp=(Page)pairs.getKey();
                if(pp.getPageId() == pageId ){
                    flag = true;
                    break;
                }
            }
            if(flag) return tables.get(i);
        }
        return "na";
    }

}