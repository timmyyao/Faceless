package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDFShift
 */
@Description(name = "shift",
        value = "_FUNC_(x,y) - add a shift y to data x",
        extended = "Example:\n")
public class UDFShift extends UDF{

}
