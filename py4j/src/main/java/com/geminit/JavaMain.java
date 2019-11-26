package com.geminit;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import py4j.ClientServer;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JavaMain {
    private static Process process;
    private static String scripts = "def transform(self, args, emitter):\n" +
            "\tjson = {}\n" +
            "\tif (args['name'] == 'tyx'):\n" +
            "\t\tjson['nickname'] = 'erMao'\n" +
            "\tif (args['age'] >= 18):\n" +
            "\t\tjson['adult'] = True\n" +
            "\tif (args['isMan']):\n" +
            "\t\tjson['sex'] = 1\n" +
            "\telse:\n" +
            "\t\tjson['sex'] = 0\n" +
            "\temitter.emit(str(json))";

    public static void initPython() throws Exception {
//        String python = "class Main(object):\n\t";
//
//        for (String script : scripts.split("\\n")) {
//            python += "\n\t" + script;
//        }
//        python += "\n\n\tclass Java:\n" +
//                "\t\timplements = [\"py4j.examples.MainInterface\"]\n\n" +
//                "from py4j.clientserver import ClientServer, JavaParameters, PythonParameters\n" +
//                "\n" +
//                "main = Main()\n" +
//                "gateway = ClientServer(\n" +
//                "\tjava_parameters = JavaParameters(),\n" +
//                "\tpython_parameters = PythonParameters(),\n" +
//                "\tpython_server_entry_point = main\n" +
//                ")";
        String path = "/home/ideaProjects/Demos/py4j/src/main/python/PythonMain.py";
//        File file = new File(path);
//        file.delete();
//        try {
//            file.createNewFile();
//            FileOutputStream in = new FileOutputStream(file);
//            in.write(python.getBytes());
//            in.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        if (!file.exists()) {
//            throw new Exception("file don't exists.");
//        }

        try {
            if (process != null && process.isAlive()) {
                process.destroy();
            }
            process = Runtime.getRuntime().exec("python " + path);
            Thread.sleep(500);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            initPython();

            ClientServer server = new ClientServer(null);
            MainInterface main = (MainInterface) server.getPythonServerEntryPoint(new Class[] {MainInterface.class});

            main.initSparkContext();
            try {
                SparkConf conf = new SparkConf();
                conf.setAppName("zpsb");
                conf.setMaster("local");
                SparkContext context = new SparkContext(conf);

                List<String> strList = new ArrayList<>();

                for (int i = 1; i < 5; i++) {
                    List<Integer> list = new ArrayList<>();
                    list.add(i);
                    Seq<Integer> seq = JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
                    RDD<Integer> rdd = context.parallelize(seq, 1, ClassTag$.MODULE$.apply(Integer.class));

                    String rddPath = "/home/rdds/" + i;
                    rdd.saveAsTextFile(rddPath);

                    Emitter emitter = new Emitter();
                    main.toPython(rddPath, emitter);

                    String outputPath = emitter.get();
                    RDD<String> result = context.textFile(outputPath, 1);
                    Object object = result.collect();
                    String[] arr = (String[]) object;

                    for (String str : arr) {
                        strList.add(str);
                    }
                }

                for (String str : strList) {
                    System.out.println(str);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            server.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            process.destroy();
        }
    }
}


