package com.geminit;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;

public class Neo4jDemo {
    private static Session session;

    public static void deleteAll() {
        session.writeTransaction(new TransactionWork<Object>() {
            @Override
            public Object execute(Transaction transaction) {
                transaction.run("MATCH (n1)-[rel]-(n2) delete n1,rel,n2");
                transaction.run("match (n) delete n");
                transaction.commit();
                transaction.close();
                return null;
            }
        });
    }

    public static void createTwoTables() {
        session.writeTransaction(new TransactionWork<Object>() {
            @Override
            public Object execute(Transaction transaction) {
                // 创建日表
                transaction.run("CREATE (day:Entity {dname:\"日表\"})");
                // 创建月表
                transaction.run("CREATE (month:Entity {dname:\"月表\"})");
                // 创建日表属性
                transaction.run("MATCH (day:Entity) \n" +
                        "WHERE day.dname = '日表' \n" +
                        "CREATE (day)-[:属性]->(:Attribute {dname:\"用户ID\",table:\"day\"})\n" +
                        "CREATE (day)-[:属性]->(:Attribute {dname:\"台区编号\",table:\"day\"})\n" +
                        "CREATE (day)-[:属性]->(:Attribute {dname:\"用电量\",table:\"day\"})");
                // 创建月表属性
                transaction.run("MATCH (month:Entity) \n" +
                        "WHERE month.dname = '月表' \n" +
                        "CREATE (month)-[:属性]->(:Attribute {dname:\"用户ID\",table:\"month\"})\n" +
                        "CREATE (month)-[:属性]->(:Attribute {dname:\"用电量\",table:\"month\"})\n" +
                        "CREATE (month)-[:属性]->(:Attribute {dname:\"用电梯度\",table:\"month\"})\n" +
                        "CREATE (month)-[:属性]->(:Attribute {dname:\"用电单价\",table:\"month\"})\n" +
                        "CREATE (month)-[:属性]->(:Attribute {dname:\"总电价\",table:\"month\"})");
                // 创建日表月表的主键关系
                transaction.run("MATCH (userId1:Attribute),(userId2:Attribute)\n" +
                        "WHERE userId1.dname = '用户ID' AND userId1.table = 'day' " +
                        "AND userId2.dname = '用户ID' AND userId2.table = 'month'\n" +
                        "CREATE (userId1)-[:主键]->(userId2)");
                transaction.commit();
                transaction.close();
                return null;
            }
        });
    }

    public static void main(String[] args) {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "ht1234"));
        session = driver.session();

        deleteAll();
        createTwoTables();

        session.close();
        driver.close();
    }
}
