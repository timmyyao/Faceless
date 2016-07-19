package org.apache.hadoop.hive.ql.udf;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.Arrays;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testUDFAscii() {

    }

    public void testUDFGeneralization() {
       /* UDFGeneralization test = new UDFGeneralization();

        //data = integer
        IntWritable val1 = new IntWritable();
        // mode=floor, data=integer
        int[] data1={1,45,0,10,21,29,-31,-30,-29,-5,-1,23,-23,435};
        int[] unit1={10,10,10,10,10,10,10,10,10,10,10,8,6,20};
        int mode1 = 0;
        int[] result1={0,40,0,10,20,20,-40,-30,-30,-10,-10,16,-24,420}; //expected result
        for (int i = 0; i < data1.length; i++) {
            IntWritable data = new IntWritable(data1[i]);
            IntWritable unit = new IntWritable(unit1[i]);
            IntWritable mode = new IntWritable(mode1);
            val1 = test.evaluate(data,mode,unit);
            if (val1.get() != result1[i]) {
                System.out.printf("data=%d,mode=0,unit=%d,expect result=%d,result=%d\n",data1[i],unit1[i],result1[i],val1.get());
            }
        }

        // mode=ceil, data=integer
        int[] data2={1,45,0,10,21,29,-31,-30,-29,-5,-1,23,-23,435};
        int[] unit2={10,10,10,10,10,10,10,10,10,10,10,8,6,20};
        int mode2 = 1;
        int[] result2={10,50,0,10,30,30,-30,-30,-20,0,0,24,-18,440}; //expected result
        for (int i = 0; i < data2.length; i++) {
            IntWritable data = new IntWritable(data2[i]);
            IntWritable unit = new IntWritable(unit2[i]);
            IntWritable mode = new IntWritable(mode2);
            val1 = test.evaluate(data,mode,unit);
            if (val1.get() != result2[i]) {
                System.out.printf("data=%d,mode=1,unit=%d,expect result=%d,result=%d\n",data2[i],unit2[i],result2[i],val1.get());
            }
        }

        //data = double
        DoubleWritable val2 = new DoubleWritable();
        // mode=floor, data=double
        double[] data3={0.1,0,-0.1,9.9,10,10.01,-10.3,-9.98,-10,-0.002,23.5,200.45,32.4,-0.3,-34.5};
        int[] unit3={10,10,10,10,10,10,10,10,10,10,6,20,1,1,1};
        double[] result3={0,0,-10,0,10,10,-20,-10,-10,-10,18,200,32,-1,-35};
        for (int i = 0; i < data3.length; i++) {
            DoubleWritable data = new DoubleWritable(data3[i]);
            IntWritable unit = new IntWritable(unit3[i]);
            IntWritable mode = new IntWritable(mode1);
            val2 = test.evaluate(data,mode,unit);
            if (val2.get() != result3[i]) {
                System.out.printf("data=%f,mode=0,unit=%d,expect result=%f,result=%f\n",data3[i],unit3[i],result3[i],val2.get());
            }
        }

        // mode=ceil, data=double
        double[] data4={0.1,0,-0.1,9.9,10,10.01,-10.3,-9.98,-10,-0.002,23.5,200.45,32.4,-0.3,-34.5};
        int[] unit4={10,10,10,10,10,10,10,10,10,10,6,20,1,1,1};
        double[] result4={10,0,0,10,10,20,-10,0,-10,0,24,220,33,0,-34};
        for (int i = 0; i < data4.length; i++) {
            DoubleWritable data = new DoubleWritable(data4[i]);
            IntWritable unit = new IntWritable(unit4[i]);
            IntWritable mode = new IntWritable(mode2);
            val2 = test.evaluate(data,mode,unit);
            if (val2.get() != result4[i]) {
                System.out.printf("data=%f,mode=1,unit=%d,expect result=%f,result=%f\n",data4[i],unit4[i],result4[i],val2.get());
            }
        }*/

        // incorrect mode or unit
    }

    public void testUDFTruncation() {
        UDFTruncation test = new UDFTruncation();
        Text str = new Text("1234567890");
        Text result = new Text();
        IntWritable length = new IntWritable(4);
        IntWritable mode = new IntWritable(0);
        result = test.evaluate(str,length,mode);
        mode.set(1);
        result = test.evaluate(str,length,mode);
    }

    public void testUDFMask() {
        Text data = new Text("hello world");
        UDFMask test = new UDFMask();
        IntWritable start = new IntWritable(-3);
        IntWritable end = new IntWritable(0);
        Text tag = new Text("*");
        Text result = test.evaluate(data,start,end,tag);
        result.set("hello");
    }
}
