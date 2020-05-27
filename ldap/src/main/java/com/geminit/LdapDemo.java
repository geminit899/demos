package com.geminit;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class LdapDemo {
    private static final String HOST = "192.168.0.114";
    private static final String PORT = "389";
    private static final String BASE_DN = "dc=mycompany,dc=com";
    private static final String ROOT = "cn=admin,dc=mycompany,dc=com";
    private static final String PWD = "admin";

    private static LdapContext getDirContext() throws NamingException {
        Hashtable<String, String> tbl = new Hashtable<>();
        tbl.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        tbl.put(Context.PROVIDER_URL, "ldap://" + HOST + ":" + PORT + "/");
        tbl.put(Context.SECURITY_AUTHENTICATION, "simple");
        tbl.put(Context.SECURITY_PRINCIPAL, ROOT);
        tbl.put(Context.SECURITY_CREDENTIALS, PWD);
        return new InitialLdapContext(tbl, null);
    }

    public static boolean addGoups(Map<String, String> lu, LdapContext ctx) {
        BasicAttributes attributes = new BasicAttributes();
        BasicAttribute objclassSet = new BasicAttribute("objectClass");
        objclassSet.add("person");
        objclassSet.add("top");
        attributes.put(objclassSet);
        attributes.put("userPassword", lu.get("userPassword"));//显示
        attributes.put("cn", lu.get("cn"));//显示
        attributes.put("sn", lu.get("cn"));//显示
//        attributes.put("ou", lu.get("htsc"));//显示
        try {
            String cn = "cn=" + lu.get("cn") + "," + BASE_DN;
            System.out.println(cn);
            ctx.createSubcontext(cn, attributes);
            System.out.println("添加用户group成功");
            return true;
        } catch (Exception e) {
            System.out.println("添加用户group失败");
            e.printStackTrace();
            return false;
        }
    }

    public static boolean addUser(Map<String, String> lu, LdapContext ctx) {
        BasicAttributes attrsbu = new BasicAttributes();
        BasicAttribute objclassSet = new BasicAttribute("objectClass");
        // objclassSet.add("account");
        objclassSet.add("posixAccount");
        objclassSet.add("inetOrgPerson");
        objclassSet.add("top");
        objclassSet.add("shadowAccount");
        attrsbu.put(objclassSet);
        attrsbu.put("uid",  lu.get("uid"));//显示账号
        attrsbu.put("sn", lu.get("sn"));//显示姓名
        attrsbu.put("cn", lu.get("cn"));//显示账号
        attrsbu.put("gecos", lu.get("gecos"));//显示账号
        attrsbu.put("userPassword", lu.get("userPassword"));//显示密码
        attrsbu.put("displayName", lu.get("displayName"));//显示描述
        attrsbu.put("mail", lu.get("mail"));//显示邮箱
        attrsbu.put("homeDirectory", "/home/" + lu.get("homeDirectory"));//显示home地址
        attrsbu.put("loginShell", "/bin/bash");//显示shell方式
        attrsbu.put("uidNumber", lu.get("uidNumber"));/*显示id */
        attrsbu.put("gidNumber", lu.get("gidNumber"));/*显示组id */

        try {
            String dn = "cn=" + lu.get("cn") + ",ou=People,dc=tcjf,dc=com";
            System.out.println(dn);
            ctx.createSubcontext(dn, attrsbu);
            System.out.println("添加用户成功");
            return true;
        } catch (Exception e) {
            System.out.println("添加用户失败");
            e.printStackTrace();
            return false;
        }
    }

    public static boolean modifyInformation(Map<String, String> lu, LdapContext ctx) {
        try {
            ModificationItem[] mods = new ModificationItem[1];
            String dn = "cn=" + lu.get("cn") + ",ou=People,dc=tcjf,dc=com";
            /*添加属性*/
            //  Attribute attr0 = new BasicAttribute("description", "测试");
            //  mods[0] = new ModificationItem(DirContext.ADD_ATTRIBUTE,attr0);

            /*修改属性*/
            Attribute attr0 = new BasicAttribute("userPassword", lu.get("userPassword"));
            mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attr0);

            /*删除属性*/
            //  Attribute attr0 = new BasicAttribute("description", "测试");
            //  mods[0] = new ModificationItem(DirContext.REMOVE_ATTRIBUTE, attr0);
            ctx.modifyAttributes(dn, mods);
            System.out.println("修改成功");
            return true;
        } catch (NamingException ne) {
            System.out.println("修改失败");
            ne.printStackTrace();
            return false;
        }
    }

    public  static boolean delete(String dn, LdapContext ctx) {
        try {
            ctx.destroySubcontext(dn);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static List<Map<String, String>> readLdap(LdapContext ctx, String basedn){

        List<Map<String, String>> lm = new ArrayList<>();
        try {
            if(ctx!=null){
                //过滤条件
                String filter = "(&(objectClass=*)(uid=*))";
                String[] attrPersonArray = { "uid", "userPassword", "displayName", "cn", "sn", "mail", "description" };
                SearchControls searchControls = new SearchControls();//搜索控件
                searchControls.setSearchScope(2);//搜索范围
                searchControls.setReturningAttributes(attrPersonArray);
                //1.要搜索的上下文或对象的名称；2.过滤条件，可为null，默认搜索所有信息；3.搜索控件，可为null，使用默认的搜索控件
                NamingEnumeration<SearchResult> answer = ctx.search(basedn, filter.toString(),searchControls);
                while (answer.hasMore()) {
                    SearchResult result = (SearchResult) answer.next();
                    NamingEnumeration<? extends Attribute> attrs = result.getAttributes().getAll();
                    Map<String, String> lu = new HashMap<>();
                    while (attrs.hasMore()) {
                        Attribute attr = (Attribute) attrs.next();
                        if("userPassword".equals(attr.getID())){
                            Object value = attr.get();
                            lu.put("userPassword", new String((byte [])value));
                        }else if("uid".equals(attr.getID())){
                            lu.put("uid", attr.get().toString());
                        }else if("displayName".equals(attr.getID())){
                            lu.put("displayName", attr.get().toString());
                        }else if("cn".equals(attr.getID())){
                            lu.put("cn", attr.get().toString());
                        }else if("sn".equals(attr.getID())){
                            lu.put("sn", attr.get().toString());
                        }else if("mail".equals(attr.getID())){
                            lu.put("mail", attr.get().toString());
                        }else if("description".equals(attr.getID())){
                            lu.put("description", attr.get().toString());
                        }
                    }
                    if(lu.get("uid")!=null) {
                        lm.add(lu);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("获取用户信息异常:");
            e.printStackTrace();
        }

        return lm;
    }

    public static void main(String[] args) throws Exception {
        LdapContext context = getDirContext();

//        Map<String, String> group = new HashMap<>();
//        group.put("userPassword", "21232f297a57a5a743894a0e4a801fc3");
//        group.put("cn", "root");
//        addGoups(group, context);
//
//
//        List<Map<String, String>> res = readLdap(context, BASE_DN);

        System.out.println("1");

//        BasicAttributes attrsbu = new BasicAttributes();
//        BasicAttribute objclassSet = new BasicAttribute("objectClass");
//        objclassSet.add("*");
//        attrsbu.put(objclassSet);
//        attrsbu.put("cn", "root");//显示账号
//        attrsbu.put("userPassword", "21232f297a57a5a743894a0e4a801fc3");
//
//        String filter = "(&(objectClass={0})({1}={2}))";
//        Object[] filterArguments = new Object[]{"*", "cn", "root"};
//        NamingEnumeration<SearchResult> results = context.search("dc=example,dc=org", filter, filterArguments, ctls);
    }
}
