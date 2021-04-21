/**
 * @author: Yumin Zhang
 * @date: 2021/4/3
 * @version: 1
 */
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
public class Hw1Grp4 {
    private String file=null;
    // column
    private int  col= 0;
    // operation
    private String op = null;
    // value
    private float val = 0;
    private ArrayList<Integer> distinct = new ArrayList<Integer>();

    public String getFile() {
        return file;
    }

    public int getCol() {
        return col;
    }

    public String getOp() {
        return op;
    }

    public float getVal() {
        return val;
    }

    public ArrayList<Integer> getDistinct() {
        return distinct;
    }

    @Override
    public String toString() {
        return "Hw1Grp4{" +
                "file='" + file + '\'' +
                ", col=" + col +
                ", op='" + op + '\'' +
                ", val=" + val +
                ", distinct=" + distinct +
                '}';
    }

    public Hw1Grp4(){

    }
    /**
     * construction function
     * @param filePath
     * @param selectArgs
     * @param distinctArgs
     */
    public Hw1Grp4(String filePath,String [] selectArgs , String [] distinctArgs) {
        this.file  = filePath;
        this.col = Integer.valueOf(selectArgs[0].trim().substring(1));
        this.op = selectArgs[1].trim();
        this.val = Float.valueOf(selectArgs[2].trim());
        for (String  arg:
             distinctArgs) {
            this.distinct.add(Integer.valueOf(arg.trim().substring(1)));
        }
    }

    public void setFile(String file) {
        this.file = file;
    }

    public void setRow(int col) {
        this.col = col;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public void setVal(float val) {
        this.val = val;
    }

    public void setDistinct(ArrayList<Integer> distinct) {
        this.distinct = distinct;
    }

    /**
     * parse command to constrcut a Hw1Grp4 instance
     * @param args  command line
     * @return Hw1Grp4  a Hw1Grp4 instance
     */
    public static Hw1Grp4 parseCommand(String[] args){
        if(args.length<3){
            System.out.println("Usage for example:java Hw1Grp4 R=<file> select:R1,gt,5.1 distinct:R2,R3,R5");
            System.exit(1);
        }
        String filePath = "hdfs://localhost:9000"+args[0].substring(2);
        String select = args[1].replace("select:", "");
        String[] selectArgs = select.split(",");
        String distinct = args[2].replace("distinct:", "");
        String[] distinctArgs = distinct.split(",");
        return new Hw1Grp4(filePath,selectArgs,distinctArgs);
    }
    /**
     * Read data from the table according to the command
     * @param commandInfo
     * @return hashtable
     * @throws IOException
     */
    public static HashMap<ArrayList<String>,Integer> readTable(Hw1Grp4 commandInfo) throws IOException {
        String file = commandInfo.getFile();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;
        // use select filter row
        HashMap<ArrayList<String>,Integer> hashTable = new HashMap<>();
        // read rows
        while ((s=in.readLine())!=null) {
            String[] row = s.split("\\|");
            int selectCol = commandInfo.getCol();
            String op = commandInfo.getOp();
            float val = commandInfo.getVal();
            float rowVal = Float.valueOf(row[selectCol]);
            // Judge whether to be selected or not
            if(filterDependOnSelect(op,rowVal,val)){
                ArrayList<String> key = new ArrayList<>();
                ArrayList<Integer> distinct = commandInfo.getDistinct();
                // Read the corresponding column
                for (Integer col:
                     distinct) {
                    key.add(row[col]);
                }
                // add to hashtable
                distinctByHash(hashTable,key);
            }

        }
        in.close();
        fs.close();
        return hashTable;
    }

    /**
     * Judge whether to be selected according to the operation
     * @param op
     * @param rowVal
     * @param val
     * @return True/False
     */
    public static  boolean filterDependOnSelect(String op,float rowVal,float val){
        Boolean res = false;
        switch (op){
            case "gt":
                if(rowVal > val){
                    res = true;
                }
                break;
            case "ge":
                if(rowVal >= val){
                    res = true;
                }
                break;
            case "eq":
                if(rowVal == val){
                    res = true;
                }
                break;
            case "ne":
                if(rowVal != val){
                    res = true;
                }
                break;
            case "le":
                if(rowVal <= val){
                    res = true;
                }
                break;
            case "lt":
                if(rowVal < val){
                    res = true;
                }
                break;
        }
        return  res;
    }

    /**
     * Write the hashtable data to the database
     * @param hashTable
     * @param distinct
     * @throws IOException
     */
    public static void writeTable(HashMap<ArrayList<String>,Integer> hashTable,ArrayList<Integer> distinct) throws IOException {
        Logger.getRootLogger().setLevel(Level.WARN);

        // create table descriptor
        String tableName= "Result";
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        // create column descriptor
        HColumnDescriptor cf = new HColumnDescriptor("res");
        htd.addFamily(cf);
        // configure HBase
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);
        if (hAdmin.tableExists(tableName)) {
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
        }
        hAdmin.createTable(htd);
        hAdmin.close();
        HTable table = new HTable(configuration,tableName);
        Set<ArrayList<String>> writeData = hashTable.keySet();
        Integer num = 0;
        ArrayList<Put> putList =new ArrayList<>();
        // write operation
        for (ArrayList<String> strs:
             writeData) {
            Put put = new Put(num.toString().getBytes());
            for (int i = 0; i < strs.size(); i++) {
                String col = "R"+distinct.get(i);
                put.add("res".getBytes(),col.getBytes(),strs.get(i).getBytes());
            }
            putList.add(put);
            num++;

        }
        //close
        table.put(putList);
        table.flushCommits();
        table.close();

    }

    /**
     * judge put or not put inot hashtable according to key
     * @param hashTable 
     * @param key 
     */
    public static void distinctByHash(HashMap<ArrayList<String>,Integer> hashTable,ArrayList<String> key) {
        if (!hashTable.containsKey(key)){
            hashTable.put(key,1);
        }

    }

    /**
     * test function
     * @param args
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void main(String[] args) throws IOException, URISyntaxException{
        Hw1Grp4 commandInfo = Hw1Grp4.parseCommand(args);
        System.out.println("command infomation: "+commandInfo.toString());
        HashMap<ArrayList<String>, Integer> hashTable = Hw1Grp4.readTable(commandInfo);
        System.out.println(hashTable.size());
        Hw1Grp4.writeTable(hashTable,commandInfo.getDistinct());
        System.out.println("put successfully");

    }

}
