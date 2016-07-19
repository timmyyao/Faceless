package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFMask.
 */
@Description(name = "mask",
        value = "_FUNC_(data,start,end,tag) - replace the sub-text from start to end with 'tag' (which must be one single character\n"
                + "start - start position (count from 1)"
                + "end - end position (end >= start)",
        extended = "Example:\n")
public class UDFMask extends UDF{
    public Text evaluate(Text data, IntWritable start, IntWritable end, Text tag) {
        //returns null when input is null
        if(data == null || start == null || end == null || tag == null){
            return null;
        }
        //returns null when 'tag' is invalid (not one single character)
        if(tag.getLength() != 1) {
            return null;
        }
        byte ch = data.getBytes()[0];
        int startVal = start.get();
        int endVal = end.get();
        //returns null if 'startVal' or 'endVal' is invalid (startVal > endVal)
        if(startVal > endVal) {
            return null;
        }
        if(startVal > data.getLength() || endVal < 1) {
            return data;
        }
        return null;
    }
}
