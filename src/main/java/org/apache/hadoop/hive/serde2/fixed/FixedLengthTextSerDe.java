package org.apache.hadoop.hive.serde2.fixed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FixedLengthTextSerDe extends AbstractSerDe {

    private List<Integer> fieldLengths;
    private StructObjectInspector rowOI;
    private ArrayList<Object> row;

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        // Read the configuration property
        String lengthsStr = tbl.getProperty("field.lengths");
        if (lengthsStr == null || lengthsStr.isEmpty()) {
            throw new SerDeException("This SerDe requires the 'field.lengths' property");
        }

        String[] lengthsArray = lengthsStr.split(",");
        fieldLengths = new ArrayList<>();
        for (String length : lengthsArray) {
            fieldLengths.add(Integer.parseInt(length.trim()));
        }

        // Set up the row ObjectInspector
        ArrayList<String> columnNames = new ArrayList<>(Arrays.asList(tbl.getProperty("columns").split(",")));
        ArrayList<ObjectInspector> columnOIs = new ArrayList<>();
        for (int i = 0; i < fieldLengths.size(); i++) {
            columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

        // Initialize row object
        row = new ArrayList<>(fieldLengths.size());
    }

    @Override
    public SerDeStats getSerDeStats() {
        // 返回一个空的 SerDeStats 对象。如果需要，可以在这里添加统计信息。
        return new SerDeStats();
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        Text rowText = (Text) blob;
        row.clear();
        String rowStr = rowText.toString();

        int startIndex = 0;
        for (int len : fieldLengths) {
            if (startIndex + len > rowStr.length()) {
                throw new SerDeException("Data length shorter than expected.");
            }
            row.add(rowStr.substring(startIndex, startIndex + len));
            startIndex += len;
        }

        return row;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        // 确保 ObjectInspector 是 StructObjectInspector 类型
        if (!(objInspector instanceof StructObjectInspector)) {
            throw new SerDeException("Expected a StructObjectInspector");
        }

        // 将行数据转换为标准结构
        StructObjectInspector structInspector = (StructObjectInspector) objInspector;
        List<Object> structFields = structInspector.getStructFieldsDataAsList(obj);

        // 检查字段数量是否与预期一致
        if (structFields.size() != fieldLengths.size()) {
            throw new SerDeException("Field count does not match field lengths");
        }

        // 拼接每个字段，根据预定义的长度修剪或填充空格
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < structFields.size(); i++) {
            String fieldData = structFields.get(i).toString();
            int length = fieldLengths.get(i);

            // 截断或填充字段数据以匹配长度
            if (fieldData.length() > length) {
                sb.append(fieldData.substring(0, length));
            } else {
                sb.append(fieldData);
                // 对每个字段单独进行填充
                int paddingLength = length - fieldData.length();
                for (int j = 0; j < paddingLength; j++) {
                    sb.append(' '); // 使用空格填充
                }
            }
        }

        return new Text(sb.toString());
    }


    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }
}
