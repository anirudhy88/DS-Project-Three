package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static int sequenceNum = 0;
    private static final Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
    public static String nodeId = "";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        // Button 1 is LDump, so get local DHT key,value pairs by querying the CP with @
        // as selection parameter
        findViewById(R.id.button1).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Cursor cursor = getContentResolver().query(uri, null, "@", null, null);
                Log.i(TAG, "After getting cursor object");
                if (cursor == null) {
                    Log.e(TAG, "Result null");
                    try {
                        throw new Exception();
                    } catch (Exception e) {
                        Log.e(TAG, "cursor object is null");
                    }
                }

                if(cursor.moveToFirst()) {
                    do {
                        String cursorData = cursor.getString(cursor.getColumnIndex("key"));
                        TextView localTextView = (TextView) findViewById(R.id.textView1);
                        localTextView.append("\t" + cursorData); // This is one way to display a string.
                        TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                        remoteTextView.append("\n");
                    } while(cursor.moveToNext());
                }
                // Play with cursor to display the results onto the textView1 and
                // then close the cursor
            }
        });

        // Button 2 is DDump, so get entire DHT key,value pairs by querying the CP with *
        // as selection parameter
        findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Cursor cursor = getContentResolver().query(uri, null, "*", null, null);
                if (cursor == null) {
                    Log.e(TAG, "Result null");
                    try {
                        throw new Exception();
                    } catch (Exception e) {
                        Log.e(TAG, "cursor object is null");
                    }
                }
                if(cursor.moveToFirst()) {
                    do {
                        String cursorData = cursor.getString(cursor.getColumnIndex("key"));
                        cursorData += " " + cursor.getString(cursor.getColumnIndex("value"));
                        TextView localTextView = (TextView) findViewById(R.id.textView1);
                        localTextView.append("\t" + cursorData); // This is one way to display a string.
                        TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                        remoteTextView.append("\n");
                    } while(cursor.moveToNext());
                }

            }
        });

        /*
        Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger1.provider");
        // Building ContentValues Object
        ContentValues contVal = new ContentValues();
        contVal.put(KEY_FIELD, Integer.toString(sequenceNum));
        contVal.put(VALUE_FIELD,strReceived);
        // Inserting
        getContentResolver().insert(uri, contVal);*/
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
