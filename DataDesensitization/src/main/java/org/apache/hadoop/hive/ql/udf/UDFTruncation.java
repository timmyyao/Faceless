package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFTruncation
 */
@Description(name = "truncation",
        value = "_FUNC_(data,length,mode) - returns a sub-Text with 'length' digits\n"
                + "mode - truncation method: 0 for reserving the first half, 1 for reserving the second half\n",
        extended = "Example:\n")
public class UDFTruncation extends UDF{
    public Text evaluate(Text data, IntWritable length, IntWritable mode) {
        //returns null when input is null
        if(data == null || length == null || mode == null) {
            return null;
        }
        int lengthVal = length.get();
        int modeVal = mode.get();
        //returns null when the value of 'mode' or 'length' is invalid
        if(modeVal != 0 && modeVal != 1 || lengthVal <= 0) {
            return null;
        }
        //returns data itself when 'length' exceeds the length of data
        if(lengthVal >= data.getLength()) {
            return data;
        }
        //returns the expected result
        Text result = new Text();
        result.set(data.getBytes(),modeVal*(data.getLength()-lengthVal),lengthVal);
        return result;
    }
}
