package com.htdata.plugin.spark;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Created by tyx on 11/15/19.
 */
public class PythonCompute implements Serializable {
    /**
     * @param fileUri the python files's path, such as python files are in a zip file
     *                should unzip the file and commandFilePath is the unzip's path
     *                And the main python file must named pythonCompute.py
     *                and it's main function must named compute(list : List<Row>)
     * @param input A Dataset<Row>
     * @return
     */
    public JavaPairRDD<StructType, Row> compute(String fileUri, Dataset<Row> input) throws IOException {
        File localizeDir = new File(new File(System.getProperty("user.dir"), "python-unzip"),
                UUID.randomUUID().toString() + " - " + System.currentTimeMillis());
        return input.toJavaRDD().mapPartitionsToPair(
            new PairFlatMapFunction<Iterator<Row>, StructType, Row>() {
                @Override
                public Iterator<Tuple2<StructType, Row>> call(Iterator<Row> inputRDDIterator) throws Exception {
                    PythonFactory pythonFactory = new PythonFactory();
                    Socket worker = pythonFactory.create();
                    Iterator<byte[]> iterator = EvaluatePython.javaToPython(inputRDDIterator);

                    WriteThread writeThread = new WriteThread(fileUri, localizeDir.getPath(), worker, iterator);
                    writeThread.start();

                    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                    ArrowStreamReader reader = new ArrowStreamReader(worker.getInputStream(), allocator);
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    List<FieldVector> vectors = root.getFieldVectors();

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
                    try {
                        while (reader.loadNextBatch()) {
                            int size = vectors.size();
                            Object[] valueObjects = new Object[size];
                            for (int i = 0; i < vectors.size(); i++) {
                                if (vectors.get(i).getField().getName().equals("__index_level_0__")) {
                                    continue;
                                }
                                valueObjects[i] = vectors.get(i).getObject(0);
                                if (valueObjects[i] instanceof Text) {
                                    valueObjects[i] = valueObjects[i].toString();
                                }
                            }
                            rows.add(new Tuple2<>(outputStructType, RowFactory.create(valueObjects)));
                        }
                    } catch (Exception e) {}

                    //关闭socket的输出流
                    worker.shutdownOutput();
                    pythonFactory.stop();
                    Util.deleteDir(localizeDir.getPath());

                    return rows.iterator();
                }
            }
        );
    }
}
