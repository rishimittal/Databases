import java.util.List;

/**
 * Created by rishimittal on 24/1/14.
 */
public class Page {

    private int pageId;
    private List<String> pageContents;
    private int startRecordId;
    private int endRecordId;
    private int fileOffset;
    private int frameNo;

    public int getFrameNo() {
        return frameNo;
    }

    public void setFrameNo(int frameNo) {
        this.frameNo = frameNo;
    }

    public int getPageId() {
        return pageId;
    }

    public int getStartRecordId() {
        return startRecordId;
    }

    public void setStartRecordId(int startRecordId) {
        this.startRecordId = startRecordId;
    }

    public int getEndRecordId() {
        return endRecordId;
    }

    public void setEndRecordId(int endRecordId) {
        this.endRecordId = endRecordId;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    public int getFileOffset() {
        return fileOffset;
    }

    public void setFileOffset(int fileOffset) {
        this.fileOffset = fileOffset;
    }

    public List<String> getPageContents() {
        return pageContents;
    }

    public void setPageContents(List<String> pageContents) {
        this.pageContents = pageContents;
    }
}
