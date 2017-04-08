
/*
 * HomeworkMapper.java
 *
 * Created on Oct 22, 2012, 5:41:50 PM
 */

package org.smaple;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.List;

import org.apache.hadoop.io.Text;

/**
 *
 * @author mac
 */
public class HomeworkMapper extends Mapper<Text,Text,Text,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.smaple.HomeworkMapper");

    @Override
    protected void map(Text key, Text value, Context context)
                    throws IOException, InterruptedException {
       context.write(key,new Text(value.toString()+"O"));
       context.write(value, new Text(key.toString()+"I"));

    }
}
