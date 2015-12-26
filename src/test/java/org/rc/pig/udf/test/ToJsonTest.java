package org.rc.pig.udf.test;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.parser.ParserException;
import org.junit.BeforeClass;
import org.rc.pig.udf.ToJson;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by zhujian on 2015/12/12.
 */
public class ToJsonTest {
    static public TupleFactory tupleFactory = TupleFactory.getInstance();
    static public BagFactory bagFactory = BagFactory.getInstance();

    @Test(expectedExceptions = IOException.class)
    public void testExec_null() throws IOException{
        ToJson test = new ToJson();
        Schema inputSchema = new Schema(new Schema.FieldSchema("id", DataType.CHARARRAY));
        test.outputSchema(inputSchema);

        test.exec(null);
    }

    @Test(expectedExceptions = IOException.class)
    public void testExec_emptyTuple() throws IOException{
        ToJson test = new ToJson();
        Schema inputSchema = new Schema(new Schema.FieldSchema("id", DataType.CHARARRAY));
        test.outputSchema(inputSchema);

        Tuple tuple = tupleFactory.newTuple();
        test.exec(tuple);
    }

    @Test(expectedExceptions = ExecException.class)
    public void testExec_nullSchema() throws IOException{
        ToJson test = new ToJson();
        Properties udfProp = UDFContext.getUDFContext().getUDFProperties(test.getClass());
        udfProp.remove(ToJson.INPUT_TUPLE_SCHEMA);

        Tuple tuple = tupleFactory.newTuple();
        tuple.append("100");
        test.exec(tuple);
    }

    @Test(expectedExceptions = ParserException.class)
    public void testExec_badSchema() throws IOException{
        ToJson test = new ToJson();

        Properties udfProp = UDFContext.getUDFContext().getUDFProperties(test.getClass());
        udfProp.setProperty(ToJson.INPUT_TUPLE_SCHEMA, "bad+schema+str");

        Tuple tuple = tupleFactory.newTuple();
        tuple.append("100");
        test.exec(tuple);
    }

    @Test
    public void testExec_1Field() throws IOException{
        ToJson test = new ToJson();
        Schema inputSchema = new Schema(new Schema.FieldSchema("id", DataType.CHARARRAY));
        test.outputSchema(inputSchema);

        Tuple tuple = tupleFactory.newTuple();
        tuple.append("1001");
        String json = test.exec(tuple);
        Assert.assertEquals(json, "{\"id\":\"1001\"}");
    }

    @Test
    public void testExec_2Fields() throws IOException{
        ToJson test = new ToJson();
        List<Schema.FieldSchema> list = new ArrayList<Schema.FieldSchema>();
        list.add(new Schema.FieldSchema("id", DataType.CHARARRAY));
        list.add(new Schema.FieldSchema("cnt", DataType.INTEGER));
        Schema inputSchema = new Schema(list);
        test.outputSchema(inputSchema);

        Tuple tuple = tupleFactory.newTuple();
        tuple.append("1001");
        tuple.append(new Integer(30));
        String json = test.exec(tuple);
        Assert.assertEquals(json, "{\"id\":\"1001\",\"cnt\":30}");
    }

    @Test
    public void testExec_2FieldsWithNull() throws IOException{
        ToJson test = new ToJson();
        List<Schema.FieldSchema> list = new ArrayList<Schema.FieldSchema>();
        list.add(new Schema.FieldSchema("id", DataType.CHARARRAY));
        list.add(new Schema.FieldSchema("cnt", DataType.INTEGER));
        Schema inputSchema = new Schema(list);
        test.outputSchema(inputSchema);

        Tuple tuple = tupleFactory.newTuple();
        tuple.append("1001");
        tuple.append(null);
        String json = test.exec(tuple);
        Assert.assertEquals(json, "{\"id\":\"1001\"}");
    }
}
