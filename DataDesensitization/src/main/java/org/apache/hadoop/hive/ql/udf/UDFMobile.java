package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFMobile.
 *
 */
@Description(name = "mobile",
             value = "_FUNC_(mobile, mask) - returns the masked value of mobile\n"
                + "mobile - original mobile number combined with a district number and a mobile number\n"
                + "mask - hide the district number or the mobile number",
             extended = "Example:\n")
public class UDFMobile extends UDF {
  private String evaluate(String mobile, int mask) {
    String sMask = Integer.toBinaryString(mask);
    char [] cMask = sMask.toCharArray();
    String [] subs = new String[3];
    if (subs.length < cMask.length) return mobile;
    int distance = subs.length - cMask.length;

    subs[0] = mobile.substring(0, 3);
    subs[1] = mobile.substring(3, 7);
    subs[2] = mobile.substring(7, 11);

    for (int i = 0; i < cMask.length; i++) {
      if (cMask[i] == '1') {
        subs[distance + i] = subs[distance + i].replaceAll("\\d", "*");
      }
    }

    return StringUtils.join(subs, "");
  }

  public Text evaluate(Text mobile, IntWritable mask) {
    if (mobile == null || mask == null) return null;
    int mode = mask.get();
    String str_mobile = mobile.toString();
    Text result = new Text();
    result.set(evaluate(str_mobile, mode));
    return result;
  }
}
