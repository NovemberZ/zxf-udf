package com.zxf.hive.common.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * author: zhangxf
 * date: 2018-02-11 18:02
 * description:
 */
public class UDFGetFirstDate extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(UDFGetFirstDate.class);
    private PrimitiveObjectInspector outputInspector;
    private SimpleDateFormat sdf;
    private Calendar calendar = Calendar.getInstance();
    private transient ObjectInspectorConverters.Converter[] converters;
    private String date_format = "yyyyMMdd";


    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        String date = ((Text) this.converters[0].convert(arg0[0].get())).toString();
        String type = ((Text) this.converters[1].convert(arg0[1].get())).toString();
        if (!type.equals("day") && !type.equals("week") && !type.equals("month") && !type.equals("quarter")) {
            throw new UDFArgumentTypeException(0, "type's value type must be day or week or month or quarter");
        }
        if (arg0.length == 3) {
            date_format = ((Text) this.converters[2].convert(arg0[2].get())).toString();
        }

        sdf = new SimpleDateFormat(date_format);
        String rst = getDateByType(date, type);
        LOG.info(date);
        LOG.info(type);
        Text retVal = new Text(rst);
        return retVal;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "UDFGetFirstDate(" + arg0[0] + ", " + arg0[1] + " )";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length < 2) {
            throw new UDFArgumentException(" Expecting at least two arrays as arguments ");
        }
        converters = new ObjectInspectorConverters.Converter[arg0.length];
        this.converters[0] = ObjectInspectorConverters.getConverter(arg0[0], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        this.converters[1] = ObjectInspectorConverters.getConverter(arg0[1], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        if (arg0.length == 3) {
            this.converters[2] = ObjectInspectorConverters.getConverter(arg0[2], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }
        outputInspector = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        return outputInspector;
    }

    public String getDateByType(String date, String type) {
        try {
            calendar.setTime(sdf.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        switch (type) {
            case "day":
                break;
            case "week":
                int a = calendar.get(Calendar.DAY_OF_WEEK) - 2;
                if (a < 0) {
                    calendar.add(Calendar.DATE, 1);
                } else {
                    calendar.add(Calendar.DATE, 7 - a);
                }
                calendar.add(Calendar.WEEK_OF_YEAR, -1);
                break;
            case "month":
                int month_minus = calendar.get(Calendar.DAY_OF_MONTH) - 1;
                calendar.add(Calendar.DATE, -month_minus);
                break;
            case "quarter":
                getCurrentQuarterStartTime();
                int quarter_minus = calendar.get(Calendar.DAY_OF_MONTH) - 1;
                calendar.add(Calendar.DATE, -quarter_minus);
                break;
            default:
                break;
        }
        return sdf.format(calendar.getTime());
    }

    public Calendar getCurrentQuarterStartTime() {
        int currentMonth = calendar.get(Calendar.MONTH) + 1;
        try {
            if (currentMonth >= 1 && currentMonth <= 3)
                calendar.set(Calendar.MONTH, 0);
            else if (currentMonth >= 4 && currentMonth <= 6)
                calendar.set(Calendar.MONTH, 3);
            else if (currentMonth >= 7 && currentMonth <= 9)
                calendar.set(Calendar.MONTH, 6);
            else if (currentMonth >= 10 && currentMonth <= 12)
                calendar.set(Calendar.MONTH, 9);
            calendar.set(Calendar.DATE, 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return calendar;
    }
}
