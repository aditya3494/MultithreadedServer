import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author adityabahuguna
 */

public class PoolServer {
    
    private static Hashtable<String, ArrayList<Cell> > INDEX = new Hashtable<>();
    
    public final static int PORT = 4401;
    
    public static void main(String[] args) throws IOException {
        
        System.getProperties().put("http.proxyHost", "172.31.1.3");
        System.getProperties().put("http.proxyPort", "8080");
        System.getProperties().put("http.proxyUser", "iit2012035");
        System.getProperties().put("http.proxyPassword", "i350");
        
        
        System.out.println("HOST IS: " + InetAddress.getLocalHost());
        PoolServer.fillTable();
        
        ExecutorService pool = Executors.newFixedThreadPool(20);
        ServerSocket server = null;
        
        try {
            server = new ServerSocket(PORT);
            try {
                    while(true) {
                        Socket connection = server.accept();
                        System.out.println("Connected here");

                        Callable<Void> task = new SearchTask(connection);
                        pool.submit(task);
                    }
                
            } catch(IOException ex) {}
        
        } catch(IOException ex) {}
        
    }
    
    private static class SearchTask implements Callable<Void>, Serializable {
        
        private Socket connection = null;
        private ObjectInputStream inStream = null;
        private ObjectOutputStream outStream = null;
        private static final long serialVersionUID = 42L;
       // private BufferedInputStream bis = null;
            
        SearchTask(Socket connection) throws SocketException, IOException {
            this.connection = connection;
            
            
            //connection.setSoTimeout(3000);
        }
        
        public Void call() throws ClassNotFoundException {
            try {
              System.out.println("Done!");
              
              if(connection == null) {
                  System.out.println("Refused");
              }
              
             
              inStream = new ObjectInputStream(connection.getInputStream());
              
              
              if(inStream == null) {
                  System.out.println("HURRaaa!! :)");
              }
              
              System.out.println("reached here!! 1");
              
              QueryInput INPUT = (QueryInput)inStream.readObject();
              
              System.out.println("reached here!! 2");
              
              if(INPUT == null) {
                  System.out.println("Empty QueryInput!!");
              }
              
              System.out.println("reached here!! 3");
              
              ArrayList<String> query = INPUT.getQuery();
              System.out.println("reached here!! 4");
              System.out.println("SIZE is:" + query.size());
              
              ResultSet resultObj = new ResultSet();
              
              System.out.println("" + query);
              
              ArrayList<ArrayList<Cell> > results = new ArrayList<>();
              
              for(int i = 0;i < query.size();i++) {
                  
                  String term = query.get(i);
                  System.out.println("term:" + term);
                  ArrayList<Cell> posting = INDEX.get(term);
                  results.add(posting);
              }
              
              resultObj.setter(results);
              
              System.out.println("" + results);
              outStream = new ObjectOutputStream(connection.getOutputStream());
             
              outStream.writeObject(resultObj);
              outStream.flush();
              System.out.println("hereeee");
              outStream.close();
              inStream.close();
              
            } catch(IOException ex){
            
            } finally {
                try {
                    connection.close();
                } catch(IOException e) {
                    
                }
            }
            
            return null;
        }
    }
    
    public static void fillTable() throws IOException {
       
       // BufferedReader br = new BufferedReader(new FileReader("dd"));
        BufferedReader br = new BufferedReader(new FileReader("Readinfo.txt"));
        String term;
        ArrayList<Cell> postingList;
        
        String line;
        
        while((line = br.readLine()) != null) {
            
            postingList = new ArrayList<>();
            String[] tokens = line.trim().split("\\s+");
            term = tokens[0];
           
            for(int j = 1;j < tokens.length;j++) {
                int docid, freq;
                String[] celltoken = tokens[j].split(",");
                docid = Integer.parseInt(celltoken[0]);
                freq = Integer.parseInt(celltoken[1]);
                Cell toAdd = new Cell(docid, freq);
                postingList.add(toAdd);
            }
             
            INDEX.put(term, postingList);
            
        }
        
        if(br != null)
            br.close();
    }
}
