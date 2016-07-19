package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * UDFAge.
 *
 */
@Description(name = "age",
             value = "_FUNC_(x) - returns the masked age of x",
             extended = "Example:\n")
public class UDFAge extends UDF {
    public Text evaluate(Integer age) {
        return null;
    }
}
