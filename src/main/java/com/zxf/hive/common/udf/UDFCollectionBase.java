package com.zxf.hive.common.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * author: zhangxf
 * date: 2018-02-11 17:59
 * description:
 */
public class UDFCollectionBase extends GenericUDF {
    protected ObjectInspector.Category category;
    protected StandardListObjectInspector stdListInspector;
    protected StandardMapObjectInspector stdMapInspector;
    protected PrimitiveObjectInspector valueInspector;
    protected PrimitiveObjectInspector keyInspector;
    protected ListObjectInspector listInspector;
    protected MapObjectInspector mapInspector;

    public static final Logger LOG =
            LoggerFactory.getLogger(UDFCollectionBase.class.getName());


    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        ObjectInspector first = arguments[0];
        this.category = first.getCategory();
        if (category == ObjectInspector.Category.LIST) {
            this.listInspector = (ListObjectInspector) first;
            this.valueInspector = (PrimitiveObjectInspector) (this.listInspector.getListElementObjectInspector());
            this.stdListInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first
                    , ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
        } else if (category == ObjectInspector.Category.MAP) {
            this.mapInspector = (MapObjectInspector) first;
            this.keyInspector = (PrimitiveObjectInspector) (this.mapInspector.getMapKeyObjectInspector());
            this.valueInspector = (PrimitiveObjectInspector) (this.mapInspector.getMapValueObjectInspector());
            this.stdMapInspector = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first
                    , ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
        } else {
            throw new UDFArgumentException(" function only takes maps or lists.");
        }

        if (this.category == ObjectInspector.Category.LIST) {
            return this.stdListInspector;
        } else if (this.category == ObjectInspector.Category.MAP) {
            return this.stdMapInspector;
        } else {
            throw new UDFArgumentException(" function only takes maps or lists.");
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return null;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "UDFCollectionBase(" + arg0 + " )";
    }

}
