/**
 * Created by neel on 26/1/14.
 */
public class Testc {
    public static void main(String[] args) {

        DBSystem dbSystem=new DBSystem();
        dbSystem.readConfig("/tmp/db/config.txt");
        dbSystem.populateDBInfo();
        //dbSystem.test();
        //dbSystem.test();
        //ParseQuery parseQuery=new ParseQuery();
        System.out.println(dbSystem.getRecord("countries",1));
        /*DBSystem.readConfig("/tmp/config.txt");

        DBSystem.populateDBInfo();
        //DBSystem.test();
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
        DBSystem.insertRecord("countries","hwllofuckers111");
        DBSystem.getRecord("countries", 42);
        DBSystem.getRecord("countries", 28);
        System.out.println(DBSystem.getRecord("countries", 44));
        //DBSystem.test();
        //DBSystem.test();
        //DBSystem.test();
        //DBSystem.insertRecord("countries","somme string");*/
    }
}
