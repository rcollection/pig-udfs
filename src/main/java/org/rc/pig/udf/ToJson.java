package org.rc.pig.udf;

import com.sun.corba.se.impl.ior.OldJIDLObjectKeyTemplate;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.apache.tools.ant.taskdefs.Exec;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Properties;

/**
 * Created by zhujian on 2015/12/12.
 */
public class ToJson extends EvalFunc<String> {
    public static final String INPUT_TUPLE_SCHEMA = "rc.udf.schema";

    private Schema schema = null;

    @Override
    public String exec(Tuple tuple) throws IOException {
        if(tuple == null || tuple.size() == 0){
            throw new IOException("UDF Bad Usage, expect at least one parameter");
        }
        if(schema == null){
            parseSchema();
        }
        //Object jsonObject = fieldToJson(field, schema.getField(0));
        Object jsonObject = tupleToJson(tuple, new FieldSchema(null, schema));
        String json = jsonObject.toString();
        return json;
    }

    private void parseSchema() throws ExecException, ParserException {
        Properties udfProp = UDFContext.getUDFContext().getUDFProperties(this.getClass());
        String schemaString = (String) udfProp.get(INPUT_TUPLE_SCHEMA);
        if(schemaString == null){
            throw new ExecException("No Schema info in the UDF context");
        }
        schema = Utils.getSchemaFromString(schemaString.substring(1, schemaString.length()-1));
    }

    private Object fieldToJson(Object o, FieldSchema fieldSchema) throws ExecException{
        byte type = DataType.findType(o);
        switch (type){
            case DataType.NULL:
            case DataType.BOOLEAN:
            case DataType.INTEGER:
            case DataType.LONG:
            case DataType.FLOAT:
            case DataType.DOUBLE:
            case DataType.CHARARRAY:
                return o;
            case DataType.TUPLE:
                return tupleToJson((Tuple)o, fieldSchema);
            case DataType.BAG:
                return bagToJson((DataBag)o, fieldSchema);
            default:
                throw new ExecException("Type is not supported to parse to json:" +
                DataType.findTypeName(type));
        }
    }

    private Object tupleToJson(Tuple tuple, FieldSchema tupleSchema) throws ExecException {
        JSONObject object = new JSONObject();
        List<FieldSchema> schemas = tupleSchema.schema.getFields();
        for(int i = 0; i < tuple.size(); i++){
            FieldSchema fieldSchema = schemas.get(i);
            object.put(fieldSchema.alias, fieldToJson(tuple.get(i), fieldSchema));
        }
        return object;
    }

    private Object bagToJson(DataBag bag, FieldSchema bagSchema)throws ExecException{
        JSONArray array = new JSONArray();
        FieldSchema tupleSchema = bagSchema.schema.getFields().get(0);
        for(Tuple tuple: bag){
            array.put(tupleToJson(tuple, tupleSchema));
        }
        return array;
    }

    @Override
    public Schema outputSchema(Schema input) {
        String schemaString = input.toString();

        UDFContext context = UDFContext.getUDFContext();
        Properties udfProp = context.getUDFProperties(this.getClass());

        udfProp.setProperty(INPUT_TUPLE_SCHEMA, schemaString);
        log.info("Input Schema of " + this.getClass().getName() + ":" + schemaString);

        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
}
