/**
 * Created by rishimittal on 24/1/14.
 */
public class DBSTest {

    //private static String inputFilePath = "/home/rishimittal/Documents/SEM2/DB/Sample/config.txt";
    private static String inputFilePath = "/tmp/config.txt";
    public static void main(String arr[]){

        DBSystem dbs = new DBSystem();
        dbs.readConfig(inputFilePath);
        dbs.populateDBInfo();

        DBSystem.getRecord("countries", 0);
        DBSystem.getRecord("countries", 1);
        DBSystem.getRecord("countries", 2);
        DBSystem.getRecord("countries", 1);
        DBSystem.getRecord("countries", 2);
        DBSystem.getRecord("countries", 2);
        DBSystem.getRecord("countries", 3);
        DBSystem.getRecord("countries", 41);
        DBSystem.getRecord("countries", 9);
        DBSystem.getRecord("countries", 39);
        DBSystem.getRecord("countries", 28);
        DBSystem.getRecord("countries", 1);
        DBSystem.getRecord("countries", 30);
        DBSystem.getRecord("countries", 38);
        DBSystem.getRecord("countries", 39);
        DBSystem.getRecord("countries", 31);
        //DBSystem.getRecord("countries", -1);
        DBSystem.getRecord("countries", 42);
        DBSystem.getRecord("countries", 28);

        /*
        dbs.getRecord("student", 2);
        dbs.getRecord("student", 4);
        dbs.getRecord("student", 6);
        dbs.getRecord("student", 8);
        dbs.getRecord("student", 2);
        dbs.getRecord("employee", 11);
        dbs.getRecord("student", 3);

        dbs.getRecord("employee", 11);
        dbs.getRecord("student", 2);
        dbs.getRecord("student", 4);
        dbs.getRecord("student", 4);
        dbs.getRecord("employee", 4);
        dbs.getRecord("employee", 40);
        dbs.getRecord("employee", 11);
        dbs.getRecord("employee", 11);
        dbs.getRecord("student", 3);
        dbs.getRecord("student", 4);
        dbs.getRecord("student", 5);
        dbs.getRecord("student", 7);
        dbs.getRecord("student", 6);
        dbs.getRecord("student", 1);


        dbs.getRecord("employee", 8);
        dbs.getRecord("employee", 4);
        dbs.getRecord("employee", 8);
        dbs.getRecord("employee", 4);
        */

    }

}
