package com.zxf.hive.common.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * author: zhangxf
 * date: 2018-02-11 18:00
 * description:
 */

@Description(name="udfGetCollectionMaxOrMinItems", value="_FUNC_( 'map/array','max/min','key/value') - return pre_event_id",
        extended="")
public class UDFGetCollectionMaxOrMinItems extends GenericUDF {
    private ObjectInspectorConverters.Converter[] converters;
    private ObjectInspector.Category category;
    private StandardListObjectInspector stdListInspector;
    private StandardMapObjectInspector stdMapInspector;
    private PrimitiveObjectInspector valueInspector;
    private PrimitiveObjectInspector keyInspector;
    private ListObjectInspector listInspector;
    private MapObjectInspector mapInspector;
    private StringObjectInspector stringInspector1;
    private StringObjectInspector stringInspector2;
    private static String GET_MAX = "max";
    private static String GET_MIN = "min";
    private static String COM_KEY = "key";
    private static String COM_VALUE = "value";

    public static final Logger LOG =
            LoggerFactory.getLogger(UDFGetCollectionMaxOrMinItems.class.getName());


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String max_min = this.stringInspector1.getPrimitiveJavaObject( arguments[1].get());
        String key_value = UDFGetCollectionMaxOrMinItems.COM_VALUE;
        if(arguments.length ==3 ) {
            key_value = this.stringInspector2.getPrimitiveJavaObject(arguments[2].get());
        }
        if(this.category == ObjectInspector.Category.LIST ){
            List mList = listInspector.getList(arguments[0].get());
            if(mList.size() <= 0 ){
                return stdListInspector.create(0);
            }
            switch ( valueInspector.getPrimitiveCategory()){
                case STRING:
                    return getStringListMaxOrMin(mList,max_min);
                case DOUBLE:
                    return getDoubleListMaxOrMin(mList,max_min);
                case LONG:
                    return getLongListMaxOrMin((List<LongWritable>) mList, max_min);
                case INT:
                    return getIntListMaxOrMin((List<IntWritable>)mList,max_min);
                default:LOG.error("test 13 is niubility" + this.category);

            }
        }else if(this.category == ObjectInspector.Category.MAP){
            Map mMap = mapInspector.getMap(arguments[0].get());
            if( mMap.size() <= 0 ){
                return (Map)stdMapInspector.create();
            }
            return getMapMaxOrMin(mMap, max_min, key_value);
        }else{
            throw new UDFArgumentException("function only takes maps or lists.");
        }
        return null;
    }

    private Object getLongListMaxOrMin(List<LongWritable> mList, String max_min) {
        List<Long> buffefList = new ArrayList<Long>(mList.size());
        for(int i = 0; i < mList.size(); i++){
            buffefList.add(mList.get(i).get());
        }
        List result = (List)this.stdListInspector.create(0);
        long max = max_min.equals(this.GET_MAX)? Collections.max(buffefList).longValue()
                :Collections.min(buffefList).longValue();
        for(int i=0; i < buffefList.size(); i++){
            if(buffefList.get(i).longValue() == max ){
                result.add(buffefList.get(i));
            }
        }
        return result;
    }

    private Object getIntListMaxOrMin(List<IntWritable> mList, String max_min) {
        List<Integer> buffefList = new ArrayList<Integer>(mList.size());
        for(int i = 0; i < mList.size(); i++){
            buffefList.add(mList.get(i).get());
        }
        List result = (List)this.stdListInspector.create(0);
        int max = max_min.equals(this.GET_MAX)?Collections.max(buffefList).intValue()
                :Collections.min(buffefList).intValue();
        for(int i=0; i < buffefList.size(); i++){
            if(buffefList.get(i).intValue() == max ){
                result.add(buffefList.get(i));
            }
        }
        return result;
    }

    private Object getDoubleListMaxOrMin(List<DoubleWritable> mList, String max_min) {
        List<Double> buffefList = new ArrayList<Double>(mList.size());
        for(int i = 0; i < mList.size(); i++){
            buffefList.add(mList.get(i).get());
        }
        List result = (List)this.stdListInspector.create(0);
        double max = max_min.equals(this.GET_MAX)?Collections.max(buffefList).doubleValue()
                :Collections.min(buffefList).doubleValue();
        for(int i=0; i < buffefList.size(); i++){
            if(buffefList.get(i).doubleValue() == max ){
                result.add(buffefList.get(i));
            }
        }
        return result;
    }

    private Object getMapMaxOrMin( Map map,String max_min, String key_value){

        Map result = (Map)stdMapInspector.create();
        Collection comList = key_value.equals(COM_KEY)?map.keySet():map.values();
        Object max = max_min.equals(GET_MAX)?Collections.max(comList):Collections.min(comList);
        Iterator iterator = map.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry entry = (Map.Entry)iterator.next();
            boolean bool = key_value.equals(COM_KEY)?entry.getKey().equals(max):entry.getValue().equals(max);
            if(bool){
                result.put(keyInspector.getPrimitiveJavaObject(entry.getKey())
                        , valueInspector.getPrimitiveJavaObject(entry.getValue()));
            }
        }
        return result;

    }


    private Object getStringListMaxOrMin(List mList, String max_min) {
        List<String> buffefList = new ArrayList<String>(mList.size());
        for(int i = 0; i < mList.size(); i++){
            buffefList.add(mList.get(i).toString());
        }

        List result = (List)this.stdListInspector.create(0);
        String max = max_min.equals(this.GET_MAX)?Collections.max(buffefList)
                :Collections.min(buffefList);
        for(int i=0; i < buffefList.size(); i++){
            if(buffefList.get(i).equals(max) ){
                result.add(buffefList.get(i));
            }
        }
        return result;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 2 || arguments.length > 3) {
            throw new UDFArgumentLengthException("The function need 2 or 3 arguments,the first is collect(map/arry)" +
                    ",the second is max/min, if the first param is a map ,then the third is compare key/value, else none" +
                    ",it will return map/array that only contains max/min items");
        }
        ObjectInspector first = arguments[0];
        this.category = first.getCategory();

        if (category == ObjectInspector.Category.LIST) {
            this.listInspector = (ListObjectInspector) first;
            this.valueInspector = (PrimitiveObjectInspector)(this.listInspector.getListElementObjectInspector());
            this.stdListInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first
                    , ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
        } else if (category == ObjectInspector.Category.MAP) {
            this.mapInspector = (MapObjectInspector) first;
            this.keyInspector = (PrimitiveObjectInspector)(this.mapInspector.getMapKeyObjectInspector());
            this.valueInspector = (PrimitiveObjectInspector)(this.mapInspector.getMapValueObjectInspector());
            this.stdMapInspector = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first
                    , ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
        } else {
            throw new UDFArgumentException(" function only takes maps or lists.");
        }


        if (!(arguments[1] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(1, "\"" + ObjectInspector.Category.PRIMITIVE.toString().toLowerCase()
                    + "\" is expected at function UDFFill_Pre_Event_ID, " + "but \"" + arguments[1].getTypeName()
                    + "\" is found");
        }else{
            this.stringInspector1 = (StringObjectInspector)arguments[1];
        }

        if (arguments.length ==3 && !(arguments[2] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(2, "\"" + ObjectInspector.Category.PRIMITIVE.toString().toLowerCase()
                    + "\" is expected at function UDFFill_Pre_Event_ID, " + "but \""
                    + arguments[2].getTypeName() + "\" is found");
        }else{
            if(arguments.length == 3) {
                this.stringInspector2 = (StringObjectInspector) arguments[2];
            }
        }

        if(this.category == ObjectInspector.Category.LIST ){
            return this.stdListInspector;
        }else if(this.category == ObjectInspector.Category.MAP){
            return this.stdMapInspector;
        }else {
            throw new UDFArgumentException(" function only takes maps or lists.");
        }
    }

    public String getDisplayString(String[] args) {
        StringBuilder sb = new StringBuilder("max/min( ");
        for (int i = 0; i < args.length - 1; ++i) {
            sb.append(args[i]);
            sb.append(",");
        }
        sb.append(args[args.length - 1]);
        sb.append(")");
        return sb.toString();
    }

    public static void main(String args[]){
        List a = new ArrayList();
        a.add(new LongWritable(1l));
        a.add(new LongWritable(2l));
        a.add(new LongWritable(3l));
        a.add(new LongWritable(4l));
        a.add(new LongWritable(4l));

        UDFGetCollectionMaxOrMinItems gmo = new UDFGetCollectionMaxOrMinItems();
        gmo.stdListInspector = (StandardListObjectInspector) ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        System.out.println( gmo.getLongListMaxOrMin(a, "min"));
    }
}
