
/*
 * HomeworkReducer.java
 *
 * Created on Oct 22, 2012, 5:46:32 PM
 */

package org.smaple;


import java.io.IOException;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author mac
 */
public class HomeworkReducer extends Reducer<Text,Text,Text,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.smaple.HomeworkReducer");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
       List<String> incoming = new ArrayList<String>();
       List<String> outgoing = new ArrayList<String>();
       for(Text value: values)
       {
           switch(value.toString().charAt(value.toString().length()-1))
           {
               case 'O':
                   outgoing.add(value.toString().substring(0,value.toString().length()-1));
                   break;
               case 'I':
                    incoming.add(value.toString().substring(0,value.toString().length()-1));
                    break;
               default:
                   break;
           }
       }
        HashMap<String,String> map = new HashMap<String,String>();
       outgoing.stream().forEach(outstr -> {
           incoming.stream().forEach(instr -> {
               if(!instr.equals(outstr) && !instr.equals(key.toString()))
               {
                   if(!map.containsKey(outstr+","+key))
                       map.put(outstr+","+key,instr);
                   else
                       map.put(outstr+","+key,map.get(outstr+","+key)+","+instr);
               }
           });
       });
       map.keySet().stream().forEach(k -> {
           try {
               context.write(new Text(k), new Text(map.get(k)));
           }
           catch (Exception ex)
           {
               ex.printStackTrace();
           }
       });
    }

}
