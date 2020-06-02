package com.geminit;

import com.sun.jndi.toolkit.dir.SearchFilter;

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
    private static final String HTSC_BASE_DN = "ou=htsc,dc=mycompany,dc=com";
    private static final String HTSC_USER_BASE_DN = "ou=sysuser,ou=htsc,dc=mycompany,dc=com";
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
        objclassSet.add("organizationalRole");
        attributes.put(objclassSet);
        attributes.put("ou", lu.get("ou"));//显示
        attributes.put("cn", lu.get("cn"));//显示
        try {
            String ou = "ou=" + lu.get("ou") + "," + HTSC_BASE_DN;
            ctx.createSubcontext(ou, attributes);
            System.out.println("添加用户group成功");
            return true;
        } catch (Exception e) {
            System.out.println("添加用户group失败");
            e.printStackTrace();
            return false;
        }
    }

    public static boolean addUser(String username, String password, LdapContext ctx) {
        BasicAttributes attributes = new BasicAttributes();
        BasicAttribute objclassSet = new BasicAttribute("objectClass");
        // objclassSet.add("account");
        objclassSet.add("posixAccount");
        objclassSet.add("inetOrgPerson");
        attributes.put(objclassSet);
        attributes.put("uid",  username);//显示账号
        attributes.put("givenName", username);//显示描述
        attributes.put("sn", username);//显示姓名
        attributes.put("cn", username);//显示账号
        attributes.put("uidNumber", "0");/*显示id */
        attributes.put("gidNumber", "0");/*显示组id */
        attributes.put("homeDirectory", "/home/" + username);//显示home地址
        attributes.put("userPassword", password);//显示密码

        try {
            String dn = "uid=" + username + "," + HTSC_USER_BASE_DN;
            ctx.createSubcontext(dn, attributes);
            System.out.println("添加用户成功");
            return true;
        } catch (Exception e) {
            System.out.println("添加用户失败");
            e.printStackTrace();
            return false;
        }
    }

    public static boolean authorization(String username, String password, LdapContext ctx) {
        try {
            SearchControls ctls = new SearchControls();
            ctls.setCountLimit(1);
            ctls.setDerefLinkFlag(true);
            ctls.setSearchScope(2);
            String filter = "(&(objectClass={0})({1}={2}))";
            Object[] filterArguments = new Object[]{"top", "uid", username};
            NamingEnumeration<SearchResult> results = ctx.search(HTSC_USER_BASE_DN, filter, filterArguments, ctls);
            if (!results.hasMoreElements()) {
                System.out.println("认证失败!\n找不到该用户！");
                return false;
            }
            SearchResult result = results.nextElement();
            Attribute attribute = result.getAttributes().get("userPassword");
            if (attribute == null) {
                System.out.println("认证失败!\n该用户没有userPassword属性！");
                return false;
            }
            byte[] value = (byte[])((byte[])attribute.get());
            String credential = new String(value);
            if (credential != null && credential.equals(password)) {
                System.out.println("认证成功!");
                return true;
            } else {
                System.out.println("认证失败!");
                return false;
            }
        } catch (NamingException ne) {
            System.out.println("认证失败!");
            ne.printStackTrace();
            return false;
        }
    }

    public static boolean modifyInformation(String username, String password, LdapContext ctx) {
        try {
            ModificationItem[] mods = new ModificationItem[1];
            String dn = "uid=" + username + "," + HTSC_USER_BASE_DN;
            /*添加属性*/
            //  Attribute attr0 = new BasicAttribute("description", "测试");
            //  mods[0] = new ModificationItem(DirContext.ADD_ATTRIBUTE,attr0);

            /*修改属性*/
            Attribute attr0 = new BasicAttribute("userPassword", password);
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

//        // add ou=htsc
//        Map<String, String> group = new HashMap<>();
//        group.put("ou", "htsc");
//        group.put("cn", "admin");
//        addGoups(group, context);

//        // add ou=sysuser
//        Map<String, String> group = new HashMap<>();
//        group.put("ou", "sysuser");
//        group.put("cn", "admin");
//        addGoups(group, context);

//        // add user: root
//        addUser("xixi", "haha", context);

        // modify
        modifyInformation("xixi", "haha", context);

        // authorization
//        authorization("xixi", "4e4d6c332b6fe62a63afe56171fd3725", context);

        System.out.println("1");


//
//        String filter = "(&(objectClass={0})({1}={2}))";
//        Object[] filterArguments = new Object[]{"*", "cn", "root"};
//        NamingEnumeration<SearchResult> results = context.search("dc=example,dc=org", filter, filterArguments, ctls);
    }
}
