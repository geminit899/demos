package com.geminit;

import com.google.gson.Gson;
import py4j.ClientServer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        String path = "/Users/geminit/IdeaProjects/Demos/py4j/src/main/python/PythonMain.py";
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
            process = Runtime.getRuntime().exec("python3 " + path);
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
            try {
                List<Map> list = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    Map<String, Object> dic = new HashMap<>();
                    dic.put("name", "tyx" + i);
                    dic.put("age", 20 + i);
                    dic.put("isMan", true);
                    list.add(dic);
                }
                Emitter emitter = new Emitter();
                main.comput(list, emitter);
                System.out.println(emitter.get());
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


