package com.geminit.offline;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateConfig {

    private static final String CONF_DIR = "/etc/htsc/conf.dist";

    public static void main(String[] args) throws Exception {
        File htscSite = new File(CONF_DIR, "htsc-site.xml");
        File htscSiteExample = new File("/home/geminit/work/svn/HTSC/branches/HTSC-6.0.0/cdap-standalone/src/main/resources/htsc-site.xml");
        File htscEnv = new File(CONF_DIR, "htsc-env.sh");
        File htscDefault = new File("/home/geminit/work/svn/HTSC/branches/HTSC-6.0.0/cdap-common/src/main/resources/htsc-default.xml");


        SAXReader reader = new SAXReader();

        Map<String, String> siteMap = xml2Map(reader.read(htscSite));
        Map<String, String> defaultMap = xml2Map(reader.read(htscDefault));
        Map<String, String> exampleMap = xml2Map(reader.read(htscSiteExample));
        Map<String, String> defaultAll = new HashMap<>();
        defaultAll.putAll(exampleMap);
        defaultAll.putAll(defaultMap);

        compare2Maps(siteMap, defaultAll);
    }

    private static Map<String, String> xml2Map(Document xml) {
        Map<String, String> map = new HashMap<>();

        List<Element> elements = xml.getRootElement().elements();
        for (Element element : elements) {
            map.put(element.elementText("name"), element.elementText("value"));
        }

        return map;
    }

    private static void compare2Maps(Map<String, String> siteMap, Map<String, String> defaultMap) {
        for (Map.Entry<String, String> entry : siteMap.entrySet()) {
            String key = entry.getKey();
            if (!defaultMap.containsKey(key)) {
                String content = "default doesn't has key : " + key + "\n";
                content += "site has it, and value is : " + siteMap.get(key);
                printOne(content);
            }
        }

        for (Map.Entry<String, String> entry : defaultMap.entrySet()) {
            String key = entry.getKey();
            if (siteMap.containsKey(key)) {
                if (!defaultMap.get(key).equals(siteMap.get(key))) {
                    String content = "key : " + key + "\n";
                    content += "default : " + defaultMap.get(key) + "\n";
                    content += "site : " + siteMap.get(key);
                    printOne(content);
                }
            }
        }
    }

    private static void printOne(String content) {
        System.out.println("\n-------------------------------------------------------------");
        System.out.println(content);
        System.out.println("-------------------------------------------------------------\n");
    }

}
