package com.zxf.hive.common.udf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * author: zhangxf
 * date: 2018-02-11 17:53
 * description:
 */
public class UDFArryMerge extends  GenericUDF{
    private static final Logger LOG = Logger.getLogger(UDFArryMerge.class);
    private StandardListObjectInspector retValInspector;
    private ListObjectInspector[] listInspectorArr;

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] arg0) throws HiveException {
        Set<Object> objects = new HashSet<Object>();
        List retVal = (List) retValInspector.create(0);

        for (int i = 0; i < arg0.length; ++i) {
            Object undeferred = arg0[i].get();
            for (int j = 0; j < listInspectorArr[i].getListLength(undeferred); ++j) {
                Object nonStd = listInspectorArr[i].getListElement(undeferred, j);
                if (!objects.contains(nonStd)){
                    retVal.add(nonStd);
                    objects.add(nonStd);
                }
            }
        }
        return retVal;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "array_union(" + arg0[0] + ", " + arg0[1] + " )";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length < 2) {
            throw new UDFArgumentException(" Expecting at least two arrays as arguments ");
        }
        ObjectInspector first = arg0[0];
        listInspectorArr = new ListObjectInspector[arg0.length];
        if (first.getCategory() == ObjectInspector.Category.LIST) {
            listInspectorArr[0] = (ListObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting an array as first argument ");
        }
        for (int i = 1; i < arg0.length; ++i) {
            if (arg0[i].getCategory() != ObjectInspector.Category.LIST) {
                throw new UDFArgumentException(" Expecting arrays arguments ");
            }
            ListObjectInspector checkInspector = (ListObjectInspector) arg0[i];
            if (!ObjectInspectorUtils.compareTypes(listInspectorArr[0].getListElementObjectInspector(), checkInspector.getListElementObjectInspector())) {
                throw new UDFArgumentException(" Array types must match " + listInspectorArr[0].getTypeName() + " != " + checkInspector.getTypeName());
            }
            listInspectorArr[i] = checkInspector;
        }

        retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);
        return retValInspector;
    }
}
