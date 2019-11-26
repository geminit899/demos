package com.htdata.plugin.spark;

import net.razorvine.pickle.IObjectPickler;
import net.razorvine.pickle.Opcodes;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;
import net.razorvine.pickle.Unpickler;
import net.razorvine.pickle.objects.ArrayConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by tyx on 11/17/19.
 */
public class EvaluatePython {
    public static Object toJava(Object obj, DataType dataType) {
        if (obj == null) {
            return null;
        } else if (obj instanceof InternalRow && dataType instanceof StructType) {
            InternalRow row = (InternalRow) obj;
            StructType struct = (StructType) dataType;
            Object[] values = new Object[row.numFields()];
            int i = 0;
            while (i < row.numFields()) {
                values[i] = toJava(row.get(i, struct.fields()[i].dataType()), struct.fields()[i].dataType());
                i++;
            }
            return new GenericRowWithSchema(values, struct);
        } else if (obj instanceof ArrayData && dataType instanceof ArrayType) {
            ArrayData a = (ArrayData) obj;
            ArrayType array = (ArrayType) dataType;
            List<Object> values = new ArrayList<>();

            int size = a.numElements();
            int i = 0;
            while (i < size) {
                if (a.isNullAt(i)) {
                    values.add(toJava(null, array.elementType()));
                } else {
                    values.add(toJava(a.get(i, array.elementType()), array.elementType()));
                }
            }
            return values;
        } else if (obj instanceof MapData && dataType instanceof MapType) {
            MapData map = (MapData) obj;
            MapType mt = (MapType) dataType;
            Map jmap = new HashMap();

            int length = map.numElements();
            ArrayData keys = map.keyArray();
            ArrayData values = map.valueArray();
            int i = 0;
            while (i < length) {
                jmap.put(toJava(keys.get(i, mt.keyType()), mt.keyType()), toJava(values.get(i, mt.valueType()), mt.valueType()));
            }
            return jmap;
        } else if (dataType instanceof UserDefinedType) {
            UserDefinedType udt = (UserDefinedType) dataType;
            return toJava(obj, udt.sqlType());
        } else if (obj instanceof Decimal) {
            Decimal d = (Decimal) obj;
            return d.toJavaBigDecimal();
        } else if (obj instanceof UTF8String) {
            UTF8String s = (UTF8String) obj;
            return s.toString();
        } else {
            return obj;
        }
    }

    private static String module = "pyspark.sql.types";

    /**
     * Pickler for StructType
     */
    private static class StructTypePickler implements IObjectPickler {
        private Class cls = StructType.class;

        void register() {
            Pickler.registerCustomPickler(cls, this);
        }

        @Override
        public void pickle(Object obj, OutputStream out, Pickler pickler) throws PickleException, IOException {
            out.write(Opcodes.GLOBAL);
            out.write((module + "\n_parse_datatype_json_string\n").getBytes(StandardCharsets.UTF_8));
            StructType schema = (StructType) obj;
            pickler.save(schema.json());
            out.write(Opcodes.TUPLE1);
            out.write(Opcodes.REDUCE);
        }
    }

    /**
     * Pickler for external row
     */
    private static class RowPickler implements IObjectPickler{
        private Class cls = GenericRowWithSchema.class;

        // register this to Pickler and UnPickler
        void register() {
            Pickler.registerCustomPickler(this.getClass(), this);
            Pickler.registerCustomPickler(cls, this);
        }

        @Override
        public void pickle(Object obj, OutputStream out, Pickler pickler) throws PickleException, IOException {
            if (obj == this) {
                out.write(Opcodes.GLOBAL);
                out.write((module + "\n_create_row_inbound_converter\n").getBytes(StandardCharsets.UTF_8));
            } else {
                // it will be memorized by Pickler to save some bytes
                pickler.save(this);
                GenericRowWithSchema row = (GenericRowWithSchema) obj;
                // schema should always be same object for memorization
                pickler.save(row.schema());
                out.write(Opcodes.TUPLE1);
                out.write(Opcodes.REDUCE);

                out.write(Opcodes.MARK);
                int i = 0;
                while (i < row.values().length) {
                    pickler.save(row.values()[i]);
                    i++;
                }
                out.write(Opcodes.TUPLE);
                out.write(Opcodes.REDUCE);
            }
        }
    }

    private static void registerPicklers() {
        Unpickler.registerConstructor("array", "array", new ArrayConstructor());
        new StructTypePickler().register();
        new RowPickler().register();
    }

    public static Iterator<byte[]> javaToPython(Iterator<Row> partitionIterator) {
        registerPicklers();

        Pickler pickler = new Pickler();
        List<byte[]> list = new ArrayList<>();
        while (partitionIterator.hasNext()) {
            List<Object> buffer = new ArrayList<>();
            buffer.add(partitionIterator.next());
            try {
                list.add(pickler.dumps(buffer.toArray(new Object[buffer.size()])));
            } catch (IOException e) {
                continue;
            }
        }
        return list.iterator();
//        return new AutoBatchedPickler(partitionIterator);
    }

    /**
     *  Choose batch size based on size of objects
     */
    public static class AutoBatchedPickler implements Iterator {
        private Pickler pickler = new Pickler();
        private int batch = 1;
        private List<Object> buffer = new ArrayList<>();
        private Iterator iter;

        public AutoBatchedPickler(Iterator iter) {
            this.iter = iter;
        }

        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public byte[] next() {
            while (iter.hasNext() && buffer.size() < batch) {
                buffer.add(iter.next());
            }
            byte[] bytes = new byte[0];
            try {
                bytes = pickler.dumps(buffer.toArray(new Object[buffer.size()]));
            } catch (IOException e) {
                e.printStackTrace();
            }
            int size = bytes.length;
            // let 1M < size < 10M
            if (size < 1024 * 1024) {
                batch *= 2;
            } else if (size > 1024 * 1024 * 10 && batch > 1) {
                batch /= 2;
            }
            buffer = new ArrayList<>();
            return bytes;
        }
    }
}
