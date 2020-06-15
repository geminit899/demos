package com.htdata.plugin.spark.socket;

import com.htdata.plugin.spark.Util;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.Text;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Server {
    private static int port = 56764;

    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("127.0.0.1", port);
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        System.out.println(1);
        ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), allocator);
        System.out.println(2);
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        System.out.println(3);
        List<FieldVector> vectors = root.getFieldVectors();
        System.out.println(4);

        List<Tuple2<StructType, Row>> rows = new ArrayList<>();
        List<StructField> structFields = new ArrayList<>();
        StructType outputStructType = null;
        // 第一次获取
        if (reader.loadNextBatch()) {
            Object[] valueObjects = new Object[vectors.size()];
            for (int i = 0; i < vectors.size(); i++) {
                String name = vectors.get(i).getField().getName();
                if (name.equals("__index_level_0__")) {
                    continue;
                }
                DataType type = Util.fromArrowType(vectors.get(i).getField().getType());
                structFields.add(DataTypes.createStructField(name, type, true));
                valueObjects[i] = vectors.get(i).getObject(0);
                if (valueObjects[i] instanceof Text) {
                    valueObjects[i] = valueObjects[i].toString();
                }
            }
            outputStructType = DataTypes.createStructType(structFields);
            rows.add(new Tuple2<>(outputStructType, RowFactory.create(valueObjects)));
        }
    }
}
