package com.geminit;

import grakn.client.GraknClient;
import grakn.client.answer.ConceptMap;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlUndefine;

import java.util.List;
import java.util.stream.Stream;

import static graql.lang.Graql.var;

public class GraknDemo {
    public static void main(String[] args) {
        GraknClient client = new GraknClient("localhost:48555");
        GraknClient.Session session = client.session("grakn");

//        // 创属性 phone、name, 将phone属性赋给person
//        GraknClient.Transaction transaction1 = session.transaction().write();
//        GraqlDefine query1 = Graql.define(
//                Graql.type("keyField").sub("relation").relates("same"),
//                Graql.type("name").sub("attribute").value("string").plays("same"),
//                Graql.type("x").sub("attribute").value("string").plays("same"),
//                Graql.type("phone").sub("attribute").value("string"),
//                Graql.type("village").sub("attribute").value("string"),
//                Graql.type("a").sub("attribute").value("string"),
//                Graql.type("b").sub("attribute").value("string"),
//                Graql.type("day").sub("entity").has("name").has("phone").has("a"),
//                Graql.type("season").sub("entity").has("x").has("village").has("b")
//        );
//        transaction1.execute(query1).get();
//        transaction1.commit();


        // 删除属性phone
        GraknClient.Transaction transaction4 = session.transaction().write();
        GraqlUndefine query4 = Graql.undefine(Graql.type("x").sub("attribute").value("string"));
        transaction4.execute(query4);
        transaction4.commit();

        // 将name属性赋给person
        GraknClient.Transaction transaction5 = session.transaction().write();
        GraqlDefine query5 = Graql.define(
                Graql.type("season").sub("entity").has("name")
        );
        transaction5.execute(query5).get();
        transaction5.commit();


//
//        // Insert a person using a WRITE transaction
//        GraknClient.Transaction writeTransaction = session.transaction().write();
//        GraqlInsert insertQuery = Graql.insert(var("x").isa("person").has("email", "x@email.com"));
//        List<ConceptMap> insertedId = writeTransaction.execute(insertQuery).get();
//        System.out.println("Inserted a person with ID: " + insertedId.get(0).get("x").id());
//        // to persist changes, a write transaction must always be committed (closed)
//        writeTransaction.commit();

        // Read the person using a READ only transaction
//        GraknClient.Transaction readTransaction = session.transaction().read();
//        GraqlGet getQuery = Graql.match(var("p").isa("person")).get().limit(10);
//        Stream<ConceptMap> answers = readTransaction.stream(getQuery).get();
//        answers.forEach(answer -> System.out.println(answer.get("p").asEntity().toString()));

        // transactions, sessions and clients must always be closed
//        readTransaction.close();
        session.close();
        client.close();
    }
}
