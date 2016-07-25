package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 7/19/16.
 */
public class GenericUDAFKAnonymity extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
          throws SemanticException {
    if(parameters.length == 0) {
      throw new UDFArgumentException("Argument expected");
    }
    for(int i = 0; i < parameters.length; i++) {
      if(parameters[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentException("Only primitive type arguments are accepted");
      }
    }
    return new GenericUDAFKAnonymityEvaluator();
  }


  public static class GenericUDAFKAnonymityEvaluator extends GenericUDAFEvaluator {

    private IntWritable result;

    /** class for storing a length-variable input*/
    static class KeyObj {
      private Object[] values;
      private int num;

      public KeyObj() {
        values = null;
        num = 0;
      }

      public KeyObj(Object[] parameters) {
        num = parameters.length;
        values = new Object[num];
        for (int i = 0; i < num; i++) {
          values[i] = parameters[i];
        }
      }

      @Override
      public boolean equals(Object obj) {
        if(this == obj) {
          return true;
        }
        if(obj == null || getClass() != obj.getClass()) {
          return false;
        }
        KeyObj keyObj = (KeyObj) obj;
        if(num != keyObj.num) return false;
        for(int i = 0; i < num; i++) {
          if(values[i] != null ? !values[i].equals(keyObj.values[i]) : keyObj.values[i] != null) {
            return false;
          }
        }
        return true;
      }

      @Override
      public int hashCode() {
        int hash = 0;
        for(int i = 0; i < num; i++) {
          hash += values[i].hashCode();
        }
        return hash;
      }
    }

    /** init not completed */
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      System.out.println("init"+m);

      if(m == Mode.COMPLETE || m == Mode.FINAL) {
        System.out.println("init end");
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
      }
      else {
        System.out.println("init end");
        //return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        return ObjectInspectorFactory.getReflectionObjectInspector(Map.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
      }
    }

    /** class for storing frequency of different inputs. */
    @AggregationType
    static class FreqTable extends AbstractAggregationBuffer {
      Map<KeyObj,Integer> freqMap;
      /**
       * renew the <key,value> in map, if key exists, value++, else value = 1
       */
      void put(Object[] key) {
        KeyObj keyObj = new KeyObj(key);
        Integer v = freqMap.get(keyObj);
        if(v == null) {
          freqMap.put(keyObj,1);
        }
        else {
          freqMap.put(keyObj,v + 1);
        }
      }
      /**
       * return the minimal value in map
       */
      Integer min() {
        int result = Integer.MAX_VALUE;
        if(freqMap.size() == 0) {
          return null;
        }
        for(Integer value : freqMap.values()) {
          if(value.intValue() < result) {
            result = value.intValue();
          }
        }
        return result;
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      FreqTable buffer = new FreqTable();
      reset(buffer);
      return buffer;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((FreqTable)agg).freqMap = new HashMap<KeyObj,Integer>();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      ((FreqTable)agg).put(parameters);
      for(int i = 0; i < parameters.length; i++) {
        System.out.println("iterate:"+parameters[i]);
      }
      System.out.println(((FreqTable)agg).freqMap.size());
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      System.out.println("terminatePartial:"+((FreqTable)agg).freqMap.size());

      return agg;
    }

    /** merge not completed*/
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      System.out.println("merge1");
      //Map mypartial = (Map)partial;
      System.out.println("merge2");
      System.out.println("merge3:"+((FreqTable)agg).freqMap.size());
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      System.out.println("terminate");
      Integer minValue = ((FreqTable)agg).min();
      if(minValue == null) {
        System.out.println("terminate: null");
        return null;
      }
      else {
        result = new IntWritable(minValue);
        System.out.println("terminate:" + ((FreqTable) agg).freqMap.size());
        return result;
      }
    }
  }
}
