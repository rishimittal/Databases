
import java.io.*;
/**
 * Created by rishimittal on 3/3/14.
 */
class FileBuffer {

    public static int CH_BUFFER_SIZE = 2048;
    public BufferedReader fbr = null;
    public File temp_file;
    private String cache = null;
    private boolean empty = false;


    public FileBuffer(File f ) throws IOException {

        temp_file = f;

        fbr = new BufferedReader(new FileReader(f)); //, CH_BUFFER_SIZE);

        reload();
    }

    public boolean empty(){

        return empty;
    }

    private void reload() throws IOException {

        boolean cflag = false;

        try{

            if((this.cache = fbr.readLine()) == null ){
                empty = true;
                cache = null;
            }else{

                empty = false;
            }
        }catch (EOFException ex){
            empty = true;
            cache = null;
        }
    }

    public void close() throws IOException {
        //Closes the File descriptor
        fbr.close();
    }

    public String peek(){


        if(empty) return null;
        //Returns the particular cache , i.e
        // first line of the requested file descriptor
        return cache.toString();
    }

    public String pop() throws IOException {

        String answer = peek();

        //System.out.println(answer);
        int p = 0;

        reload();

        return answer;
    }
}