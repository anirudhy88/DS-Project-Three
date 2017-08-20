package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;

import android.app.Activity;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.w3c.dom.NodeList;

public class SimpleDhtProvider extends ContentProvider {
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static String nodeId = "";
    static ArrayList<String> nodeList = new ArrayList<String>();
    static ArrayList<String> nodeListClient = new ArrayList<String>();
    static String myPort = "";
    static String predecessorNodeId = "";
    static String succesor = "";
    static String predecessor = "";
    static String globalKey = "";
    static ArrayList<String> starContainer = new ArrayList<String>() ;

    //static final String Insert = "Insert() :";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.i(TAG, "Delete : Delete hit");
        String path = getContext().getFilesDir().getPath();
        File f = new File(path);
        f.mkdirs();
        Boolean  found = false;
        File[] file = f.listFiles();
        for(File fi : file) {
            if(fi.getName().equals(selection)) {
                Log.i(TAG, "Delete : Deleting :" + selection);
                found = true;
                fi.delete();
            }
        }
        if(!found) {
            Log.i(TAG,"Delete : Not Found, so calling client");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", "delete", selection);
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.i(TAG, "Insert : Insert hit");
        // TODO Auto-generated method stub
        String key = (String) values.get("key");
        String value = (String) values.get("value");
        Log.i(TAG, "Insert : key is " + key);
        Log.i(TAG, "Insert : value is " + value);
        String[] splitKey = key.split(" ");
        Log.i(TAG, "Insert : splitKey size is " + splitKey.length);
        if (splitKey.length == 2) {
            key = splitKey[1];
        }
        // Creating a file to store the value
        // We have to do genHash on the key and get the correct chord id
        String hashKey = "";
        try {
            hashKey = genHash(key);
            // If the hashed key is in between me and my predecesor then I have to store
            Log.i(TAG, "Insert : NodeList size is " + nodeList.size());
            Log.i(TAG, "hashed key is :" + hashKey);
            Log.i(TAG, "nodeId is:" + nodeId);
            Log.i(TAG, "predecessorNodeId is :" + predecessorNodeId);
            Log.i(TAG, "hashKey.compareTo(genHash(nodeId)) : " + hashKey.compareTo(genHash(nodeId)));
            Log.i(TAG, "hashKey.compareTo(genHash(predecessor)) : " + hashKey.compareTo(genHash(predecessorNodeId)));
            Log.i(TAG, "nodeList.get(0).equals(myPort) : " + nodeList.get(0).equals(myPort));
            Log.i(TAG, "hashKey.compareTo(genHash(nodeId)) :" + hashKey.compareTo(genHash(nodeId)));
            if (nodeList.size() == 1 || splitKey.length == 2 ||
                    (hashKey.compareTo(genHash(nodeId)) <= 0 &&
                            hashKey.compareTo(genHash(predecessorNodeId)) > 0) ||
                    (nodeList.get(0).equals(myPort) &&
                            hashKey.compareTo(genHash(nodeId)) < 0)) {

                String fileName = "";
                Log.i(TAG, "Insert : In the first if check");
                if (splitKey.length == 2) {
                    Log.i(TAG, "Insert : splitKey length is 2 : " + splitKey[0]);
                    fileName = splitKey[1];
                } else {
                    Log.i(TAG, "Insert : splitKey length is 1 : " + splitKey[0]);
                    fileName = splitKey[0];
                }
                // To write data into the file, need to create a file output stream.
                FileOutputStream outputStream;
                try {
                    // Getting file output stream using openFileOutput
                    outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
                    // Write data into the file using the stream
                    outputStream.write(value.getBytes());
                    // Once you are done writing, just close it
                    outputStream.close();
                } catch (IOException e) {
                    Log.e(TAG, "Failure of writing data into a file");
                }
            }  // What is I am the last node of the chord, I need to check hash of key is
            // greater than my has and then put it in first node of the chord
            else if (nodeList.size() > 1 && nodeList.get(nodeList.size() - 1).equals(myPort)) {
                if (hashKey.compareTo(genHash(nodeId)) > 0) {
                    Log.i(TAG, "Insert : In the first else if check");
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", "special", key, value);
                } else {
                    Log.i(TAG, "Insert : else check of if else checkk");
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", key, value);
                }
                Log.i(TAG, "Insert : Just before leaving else if check");
            } else {
                Log.i(TAG, "Insert : In else check");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", key, value);
            }

        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "No such algorithm exception");
        }
        // BigInteger hashK = new BigInteger(hashKey, 32);
        //Log.i(TAG, "The integer value of hashedKey is :" + hashK);
        return uri;
    }

    @Override
    public boolean onCreate() {
        Log.i(TAG, "Oncreate hit");

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.i(TAG, "Provider myPort is :" + myPort);

        if (myPort.equals("11108")) {
            nodeList.add("11108");
            //nodeListClient.add("11108");
        } else {
            nodeList.add(myPort);
            //nodeListClient.add(myPort);
        }
        // Based on myPort, the node id is determined
        switch (Integer.parseInt(myPort)) {
            case 11108:
                nodeId = String.valueOf(5554);
                break;
            case 11112:
                nodeId = String.valueOf(5556);
                break;
            case 11116:
                nodeId = String.valueOf(5558);
                break;
            case 11120:
                nodeId = String.valueOf(5560);
                break;
            case 11124:
                nodeId = String.valueOf(5562);
                break;
        }
        try {
            Log.i(TAG, "Just before server task invoking");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Error in creating a socket");
            return false;
        }
        Log.i(TAG, "Just before client task invoking");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Log.i(TAG, "Query method  with selection as :" + selection);
        // TODO Auto-generated method stub
        // If the selection is * return all the keys and values.
        FileInputStream inputStream;
        String value = "";

        byte[] buffer = new byte[1024];
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        String path = getContext().getFilesDir().getPath();
        File f = new File(path);
        f.mkdirs();
        File[] file = f.listFiles();
        if (selection.equals("*") && nodeList.size() > 1) {
            Log.i(TAG,"selection element is *");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", "star", myPort, "2");
            try {
                Thread.sleep(2000);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Log.i(TAG, "starContainer size is :" + starContainer.size());
            for(int i = 0; i <starContainer.size(); i = i+2) {
                String key = starContainer.get(i);
                String values = starContainer.get(i+1);
                String[] columnValues = {key, values};
                cursor.addRow(columnValues);
            }
            StringBuilder sb3 = new StringBuilder();
            FileInputStream inputStream3;
            byte[] buffer2 = new byte[1024];
            int n2 = 0;
            for(File fi : file) {
                String key = fi.getName();
                try {
                    inputStream3 = getContext().openFileInput(fi.getName());
                    while ((n2 = inputStream3.read(buffer2)) != -1) {
                        sb3.append(new String(buffer2, 0, n2));
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                String values = sb3.toString();
                sb3 = new StringBuilder();
                String[] columnValues = {key, values};
                cursor.addRow(columnValues);
            }
            Log.i(TAG, " Done with for loop, now returning cursor object,, count : "+cursor.getCount());
            return cursor;
        } else if (selection.equals("@") || (selection.equals("*") && nodeList.size() == 1)) {
            Log.i(TAG, "File array size is :" + file.length);
            int n = 0;
            for (int i = 0; i < file.length; i++) {
                StringBuffer sb = new StringBuffer("");
                try {
                    inputStream = getContext().openFileInput(file[i].getName());
                    while ((n = inputStream.read(buffer)) != -1) {
                        sb.append(new String(buffer, 0, n));
                    }
                    Log.i(TAG, "sb is :" + sb.toString());
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "File not found");
                } catch (IOException e) {
                    Log.e(TAG, "IO Eception");
                }
                value = sb.toString();
                String[] columnValues = {file[i].getName(), value};
                cursor.addRow(columnValues);
                Log.v("query", selection);
            }
            return cursor;
        } else {
            // If the selection is @ return all my local keys and values.
            //FileInputStream inputStream;
            Boolean contains = false;
            StringBuffer sb = new StringBuffer("");
            int n = 0;
            for (File fi : file) {
                if (fi.getName().equals(selection)) {
                    contains = true;
                }
            }
            // Similar to writing into a file, this is read operation from a file
            // File name is sent as an argument 'selection'
            if (contains) {
                try {
                    inputStream = getContext().openFileInput(selection);
                    while ((n = inputStream.read(buffer)) != -1) {
                        sb.append(new String(buffer, 0, n));
                    }
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "file not found");
                } catch (IOException e) {
                    Log.e(TAG, "io exception");
                }
                value = sb.toString();
                String[] columnValues = {selection, value};
                cursor.addRow(columnValues);
                Log.v("query", selection);
            } else {
                try {
                    globalKey = "";
                    if (genHash(selection).compareTo(genHash(nodeId)) > 0) {
                        Log.i(TAG,"Query : Querying successor");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", "successor", selection);
                    }
                    else if(genHash(selection).compareTo(genHash(predecessorNodeId)) < 0){
                        Log.i(TAG,"Query : Querying predecessor");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query" ,"predecessor", selection);
                    }
                    Thread.sleep(1000);
                    Log.i(TAG,"Query : Updating the cursor when asked for non-existing key :"+globalKey);
                    String[] columnValues = {selection, globalKey};
                    cursor.addRow(columnValues);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return cursor;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.i(TAG, "Server Task : In server task");
            ServerSocket serverSocket = sockets[0];
            Socket server = null;
            String keyToSend = ""; // In case of query msg recevived
            String msgToSend = "";

            while (true) {
                try {
                    //ArrayList<String> msgReceived = new ArrayList<String>();
                    Boolean queryRec = false;
                    server = serverSocket.accept();
                    InputStream is = server.getInputStream();
                    BufferedReader br = new BufferedReader(new InputStreamReader(is));
                    Log.i(TAG, "Server Task : Accepted some connection");
                    String msgReceived = br.readLine();
                    Log.i(TAG, "Server Task : Msg received is :" + msgReceived);
                    String[] msgRecArray = msgReceived.trim().split(" ");
                    if (msgRecArray[0].equals("double")) {
                        Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
                        // Building ContentValues Object
                        ContentValues contVal = new ContentValues();
                        contVal.put(KEY_FIELD, "special " + msgRecArray[2]);
                        contVal.put(VALUE_FIELD, msgRecArray[3]);
                        // Inserting
                        getContext().getContentResolver().insert(uri, contVal);
                    } else if (msgRecArray[0].equals("special")) {
                        Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
                        // Building ContentValues Object
                        ContentValues contVal = new ContentValues();
                        contVal.put(KEY_FIELD, msgRecArray[1]);
                        contVal.put(VALUE_FIELD, msgRecArray[2]);
                        // Inserting
                        getContext().getContentResolver().insert(uri, contVal);
                    }else if(msgRecArray[0].equals("query")) {
                        //queryRec = true;
                        if(msgRecArray[1].equals("found")) {
                            Log.i(TAG, "Found so updating global key");
                            globalKey = msgRecArray[3];
                        } else if(msgRecArray[1].equals("star")) {
                            if(msgRecArray[2].equals("reply")) {
                                // Populate the arraylist
                                for(int i = 3; i < msgRecArray.length; i++) {
                                    Log.i(TAG,"Adding to star container");
                                    starContainer.add(msgRecArray[i]);
                                }
                            } else {
                                if ((Integer.parseInt(msgRecArray[3]) != nodeList.size())) {
                                    Log.i(TAG, "Calling publish progress for sending a star msg to my successor");
                                    publishProgress("query", "star", "two" , msgRecArray[2], msgRecArray[3]);
                                } else {
                                    Log.i(TAG, "Noo need to further send to successor because cap reached");
                                    publishProgress("query", "star", "one ", msgRecArray[2], msgRecArray[3]);
                                }
                            }
                        } else if(msgRecArray[1].equals("delete")) {
                            String path = getContext().getFilesDir().getPath();
                            File f = new File(path);
                            f.mkdirs();
                            Boolean  found = false;
                            File[] file = f.listFiles();
                            for(File fi : file) {
                                if(fi.getName().equals(msgRecArray[2])) {
                                    Log.i(TAG, "Delete : Deleting :" + msgRecArray[2]);
                                    found = true;
                                    fi.delete();
                                }
                            }
                            if(!found) {
                                Log.i(TAG,"Server Task : Not Found, so calling client");
                                publishProgress("query", "delete", msgRecArray[2]);;
                            }

                        }
                        else {
                            String path = getContext().getFilesDir().getPath();
                            File f = new File(path);
                            f.mkdirs();
                            File[] file = f.listFiles();
                            StringBuilder sb2 = new StringBuilder();
                            FileInputStream inputStream2;
                            byte[] buffer2 = new byte[1024];
                            int n2 = 0;
                            Boolean contains = false;
                            for (File fi : file) {
                                if (fi.getName().equals(msgRecArray[2])) {
                                    Log.i(TAG, "Contains is made true");
                                    contains = true;
                                    inputStream2 = getContext().openFileInput(fi.getName());
                                    while ((n2 = inputStream2.read(buffer2)) != -1) {
                                        sb2.append(new String(buffer2, 0, n2));
                                    }
                                }
                            }
                            if(!contains) {
                                publishProgress("query", msgRecArray[1], msgRecArray[2]);
                            } else {
                                keyToSend = sb2.toString();
                                publishProgress("query","found", msgRecArray[1], msgRecArray[2], keyToSend);
                                //msgToSend = keyToSend + "\n";
                            }
                        }

                    } else {
                        Log.i(TAG, "Server Task : Size of the msgRecArray is : " + msgRecArray.length);
                        for (String m : msgRecArray) {
                            if (!nodeList.contains(m) && !m.equals(" ")) {
                                Log.i(TAG, "Server Task : Adding to nodeList");
                                nodeList.add(m);
                                Log.i(TAG, "Server Task : nodeList size after adding is :" + nodeList.size());
                            }
                        }

                        Collections.sort(nodeList, new Dummy());
                        Log.i(TAG, "Server Task : Values in Node List are, its size is :" + nodeList.size());
                        for (String s : nodeList) {
                            Log.i(TAG, "Server Task : " + s + "\n");
                        }
                        int index = nodeList.indexOf(myPort);
                        Log.i(TAG, "Server Task : My index in nodeList is :" + index);
                        // If I am not first or last element in the list
                        if (index != 0 && index != nodeList.size() - 1) {
                            succesor = nodeList.get(index + 1);
                            predecessor = nodeList.get(index - 1);
                            Log.i(TAG, "Server Task : successor = " + succesor + ", predecessor = " + predecessor);
                        }
                        if (index == 0) {
                            succesor = nodeList.get(index + 1);
                            predecessor = nodeList.get(nodeList.size() - 1);
                            Log.i(TAG, "Server Task : successor = " + succesor + ", predecessor = " + predecessor);
                        }
                        if (index == nodeList.size() - 1) {
                            succesor = nodeList.get(0);
                            predecessor = nodeList.get(index - 1);
                            Log.i(TAG, "Server Task : successor = " + succesor + ", predecessor = " + predecessor);
                        }
                        switch (Integer.parseInt(predecessor)) {
                            case 11108:
                                predecessorNodeId = "5554";
                                break;
                            case 11112:
                                predecessorNodeId = "5556";
                                break;
                            case 11116:
                                predecessorNodeId = "5558";
                                break;
                            case 11120:
                                predecessorNodeId = "5560";
                                break;
                            case 11124:
                                predecessorNodeId = "5562";
                                break;
                        }
                        Log.i(TAG, "Server Task : Size of nodeList is :" + nodeList.size());
                    }
                    //}
                    msgToSend = "ACK MSG" + "\n";
                    Log.i(TAG,"Server Task : msgToSend :"+msgToSend);
                    DataOutputStream outToServer = new DataOutputStream(server.getOutputStream());
                    outToServer.writeBytes(msgToSend);
                    if (myPort.equals("11108") && !msgRecArray[0].equals("double") && !msgRecArray[0].equals("special")
                            && !msgRecArray[0].equals("query"))
                        publishProgress("server");
                    Log.i(TAG,"Server Task : Closing server ");
                    server.close();
                } catch (IOException e) {
                    Log.e(TAG, "publish progress failed");
                }
                Log.i(TAG, "Leaving server task");
            }
        }

        protected void onProgressUpdate(String... strings) {
            Log.i(TAG, "Server Task : In progress update, string.length is "+ strings.length);;
            if(strings.length == 3) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0],"server", strings[1], strings[2]);
            } else if(strings.length == 1){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0]);
            } else if(strings[1].equals("star")) {
                // query star one/two portNum count
                Log.i(TAG,"In progress update : +"+strings.length);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0], strings[1],
                        strings[2],strings[3], strings[4]);
            }
            else {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0], strings[1],strings[2],
                        strings[3],strings[4]);
            }
            return;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                //if(nodeList.size() == 1) return null;
                Log.i(TAG, "Client Task : In client task : msgs[0] is " + msgs[0] + " and msgs size is :" + msgs.length);
                for (String s : msgs)
                    Log.i(TAG, " " + s + " ");
                String msgToSend = "";
                String msgToSend2 = "";
                String msgToSendQuery = "";
                String msgToSendStar = "";
                if (msgs[0].equals("insert")) {
                    Log.i(TAG, "Client Task : insert method called me");
                    if (msgs[1].equals("special")) {
                        Log.i(TAG, "Client Task : msgs[0] is insert and msgs[1] is special");
                        msgToSend = "double special " + msgs[2] + " " + msgs[3];
                    } else {
                        msgToSend = "special " + msgs[1] + " " + msgs[2];
                    }

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(succesor));
                    // Log.i(TAG, "Client Task : msgs[0] is :" + msgs[0]);

                    DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
                    msgToSend += "\n";
                    outToServer.writeBytes(msgToSend);
                    Log.i(TAG, "Client Task : msgToSend is - " + msgToSend);
                    InputStream is = socket.getInputStream();
                    BufferedReader br = new BufferedReader(new InputStreamReader(is));
                    String msgReceived = br.readLine();
                    if (msgReceived.equals("ACK MSG")) {
                        Log.i(TAG, "Client Task : Closing socket");
                        socket.close();
                    } else {
                        Log.i(TAG, "Client Task : Did not receive ACK");
                    }
                } else if (msgs[0].equals("query")){
                    Log.i(TAG,"Client Task : Order from query method or server, have to send query");
                    // Note : myPort in the below msg might change if the second node also does not
                    // have the message, we should not be over riding the origin port number
                    if(msgs[1].equals("server")) {
                        msgToSendQuery = "query " + msgs[2] + " " + msgs[3] + "\n";
                        if(genHash(msgs[3]).compareTo(genHash(nodeId)) > 0) {
                            msgs[1] = "successor";
                        } else {
                            msgs[1] = "predecessor";
                        }
                    } else if(msgs[1].equals("found")) {
                        msgToSendQuery = "query"+" " + "found " + msgs[3] + " "+ msgs[4] +"\n";
                    } else if(msgs[1].equals("star")) {
                        //query star one/two originPort count
                        if (msgs.length == 5){
                            if (msgs[2].equals("two")) {
                                msgToSendQuery = "query " + "star " + msgs[3] + " " +
                                        (String.valueOf(Integer.parseInt(msgs[4])+1)) + "\n";
                            }
                            msgToSendStar = "query star reply ";
                            String path = getContext().getFilesDir().getPath();
                            File f = new File(path);
                            f.mkdirs();
                            File[] file = f.listFiles();
                            FileInputStream inputStream;
                            int n = 0;
                            byte[] buffer = new byte[1024];
                            StringBuilder sb = new StringBuilder();
                            Log.i(TAG,"Client Task : File size is to fill msgToSendStar:"+file.length);
                            for (File fi : file) {

                                msgToSendStar += fi.getName() + " ";
                                inputStream = getContext().openFileInput(fi.getName());
                                while ((n = inputStream.read(buffer)) != -1) {
                                    sb.append(new String(buffer, 0, n));
                                }
                                msgToSendStar += sb.toString() + " ";
                                sb = new StringBuilder();
                            }
                            msgToSendStar += "\n";
                        } else {
                            Log.i(TAG,"In client task : received from query method");
                            msgToSendQuery = "query " + "star " + msgs[2] + " " +
                                    (String.valueOf(Integer.parseInt(msgs[3]))) + "\n";
                        }
                    } else if(msgs[1].equals("delete")) {
                        Log.i(TAG,"Client Task : Got delete keywork");
                        msgToSendQuery = "query "+"delete "+ msgs[2];
                    }
                    else {
                        msgToSendQuery = "query " + myPort + " " + msgs[2] + "\n";
                    }
                    Log.i(TAG,"Client Task : msgToSendQuery : "+msgToSendQuery);
                    Socket socket;
                    if(msgs[1].equals("successor") || msgs[1].equals("delete")) {
                        Log.i(TAG,"Client Task : Sending to my successor :"+succesor);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(succesor));
                        DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
                        outToServer.writeBytes(msgToSendQuery);
                        InputStream is = socket.getInputStream();
                        BufferedReader br = new BufferedReader(new InputStreamReader(is));
                        String msgReceived = br.readLine();
                        Log.i(TAG, "Client Task : msg received is :" + msgReceived);
                        if (msgReceived.equals("ACK MSG")) {
                            Log.i(TAG, "Client Task : Closing socket");
                            socket.close();
                        } else {
                            Log.i(TAG, "Client Task : Did not receive ACK");
                        }
                    } else if(msgs[1].equals("found")) {
                        Log.i(TAG,"Client Task : Sending to destination :"+msgs[2]);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgs[2]));
                        DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
                        outToServer.writeBytes(msgToSendQuery);
                        InputStream is = socket.getInputStream();
                        BufferedReader br = new BufferedReader(new InputStreamReader(is));
                        String msgReceived = br.readLine();
                        Log.i(TAG, "Client Task : msg received is :" + msgReceived);
                        if (msgReceived.equals("ACK MSG")) {
                            Log.i(TAG, "Client Task : Closing socket");
                            socket.close();
                        } else {
                            Log.i(TAG, "Client Task : Did not receive ACK");
                        }
                    }
                    else if(msgs[1].equals("predecessor")) {
                        Log.i(TAG,"Client Task : Sending to my predecessor :"+predecessor);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(predecessor));
                        DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
                        outToServer.writeBytes(msgToSendQuery);
                        InputStream is = socket.getInputStream();
                        BufferedReader br = new BufferedReader(new InputStreamReader(is));
                        String msgReceived = br.readLine();
                        Log.i(TAG, "Client Task : msg received is :" + msgReceived);
                        if (msgReceived.equals("ACK MSG")) {
                            Log.i(TAG, "Client Task : Closing socket");
                            socket.close();
                        } else {
                            Log.i(TAG, "Client Task : Did not receive ACK");
                        }
                    } else if(msgs[1].equals("star")) {
                        if (msgs.length != 5) {
                            // Means query mehtod called me not sever invoked
                            Log.i(TAG, "Means query mehtod called me not sever invoked");
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(succesor));
                            DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
                            outToServer.writeBytes(msgToSendQuery);
                            InputStream is = socket.getInputStream();
                            BufferedReader br = new BufferedReader(new InputStreamReader(is));
                            String msgReceived = br.readLine();
                            Log.i(TAG, "Client Task : msg received is :" + msgReceived);
                            if (msgReceived.equals("ACK MSG")) {
                                Log.i(TAG, "Client Task : Closing socket");
                                socket.close();
                            } else {
                                Log.i(TAG, "Client Task : Did not receive ACK");
                            }
                        } else {
                            if (msgs[2].equals("two")) {
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(succesor));
                                DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
                                outToServer.writeBytes(msgToSendQuery);
                                InputStream is = socket.getInputStream();
                                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                                String msgReceived = br.readLine();
                                Log.i(TAG, "Client Task : msg received is :" + msgReceived);
                                if (msgReceived.equals("ACK MSG")) {
                                    Log.i(TAG, "Client Task : Closing socket");
                                    socket.close();
                                } else {
                                    Log.i(TAG, "Client Task : Did not receive ACK");
                                }
                            }
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(msgs[3]));
                            DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
                            outToServer.writeBytes(msgToSendStar);
                            InputStream is = socket.getInputStream();
                            BufferedReader br = new BufferedReader(new InputStreamReader(is));
                            String msgReceived = br.readLine();
                            Log.i(TAG, "Client Task : msg received is :" + msgReceived);
                            if (msgReceived.equals("ACK MSG")) {
                                Log.i(TAG, "Client Task : Closing socket");
                                socket.close();
                            } else {
                                Log.i(TAG, "Client Task : Did not receive ACK");
                            }
                        }
                    }
                } else {
                    String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
                    if (!myPort.equals(remotePorts[0]) || msgs[0].equals("server")) {

                        Log.i(TAG, "Client Task : Entering the OR check");

                        for (int i = 0; i < 5; i++) {
                            msgToSend2 = "";
                            if (myPort.equals(remotePorts[0])) {
                                if (!nodeList.contains(remotePorts[i])) continue;
                            }
                            if (myPort.equals(remotePorts[i])) {
                                Log.i(TAG, "Client Task : Continuing because it is me , i = " + i);
                                continue;
                            }
                            Log.i(TAG, "Client Task : Sending msg for  i = " + i);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remotePorts[i]));
                            Log.i(TAG, "Client Task : msgs[0] is :" + msgs[0]);
                            DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());

                            if (msgs[0].equals("server")) {
                                nodeListClient = nodeList;
                                for (String s : nodeListClient) {
                                    msgToSend2 += " " + s;
                                }
                                msgToSend2 += "\n";
                                Log.i(TAG, "Client Task : msgToSend2 is - " + msgToSend2);
                                outToServer.writeBytes(msgToSend2);
                                // toServer.writeObject(msgToSend2);
                            } else {
                                msgToSend2 = msgs[0] + "\n";
                                Log.i(TAG, "Client Task : msgToSend2 is - " + msgToSend2);
                                outToServer.writeBytes(msgToSend2);
                                // toServer.writeObject(msgToSend2);
                            }
                            //ObjectInputStream is = new ObjectInputStream(socket.getInputStream());

                            InputStream is = socket.getInputStream();
                            BufferedReader br = new BufferedReader(new InputStreamReader(is));
                            Log.i(TAG, "Client Task : Next to Read object");
                            String msgReceived = br.readLine();
                            Log.i(TAG, "Client Task : msg received is :" + msgReceived);
                            if (msgReceived.equals("ACK MSG")) {
                                Log.i(TAG, "Client Task : Closing socket");
                                socket.close();
                            } else {
                                Log.i(TAG, "Client Task : Did not receive ACK");
                            }
                            if (!msgs[0].equals("server")) {
                                Log.i(TAG, "Client Task : Breaking because this is not server invoked");
                                break;
                            }
                        }
                    }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (NullPointerException e) {
                Log.e(TAG, "ClientTask socket Null pointer exception");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            Log.i(TAG, "Client Task : Leaving client task");
            return null;
        }
    }

    private class Dummy implements Comparator<String> {
        public int compare(String s1, String s2) {
            try {
                switch (Integer.parseInt(s1)) {
                    case 11108:
                        s1 = "5554";
                        break;
                    case 11112:
                        s1 = "5556";
                        break;
                    case 11116:
                        s1 = "5558";
                        break;
                    case 11120:
                        s1 = "5560";
                        break;
                    case 11124:
                        s1 = "5562";
                        break;
                }
                switch (Integer.parseInt(s2)) {
                    case 11108:
                        s2 = "5554";
                        break;
                    case 11112:
                        s2 = "5556";
                        break;
                    case 11116:
                        s2 = "5558";
                        break;
                    case 11120:
                        s2 = "5560";
                        break;
                    case 11124:
                        s2 = "5562";
                        break;
                }
                s1 = genHash(s1);
                s2 = genHash(s2);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "No such algorithm error");
            }
            return s1.compareTo(s2);
        }
    }
}
