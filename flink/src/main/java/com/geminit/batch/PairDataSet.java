package com.geminit.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PairDataSet {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        List<Tuple2<String, String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>("001", "geminit"));
        DataSet<Tuple2<String, String>> pairDataSet1 = env.fromCollection(list1);

        List<Tuple2<String, Optional<Integer>>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>("001", Optional.empty()));
        DataSet<Tuple2<String, Optional<Integer>>> pairDataSet2 = env.fromCollection(list2);



    }

    public static void join(DataSet<Tuple2<String, String>> join1, DataSet<Tuple2<String, Optional<Integer>>> join2) {
        DataSet<Tuple2<String, Optional<Integer>>> afterJoin = join1.leftOuterJoin(join2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<String, String>, Tuple2<String, Optional<Integer>>, Tuple2<String, Optional<Integer>>>() {
            @Override
            public Tuple2<String, Optional<Integer>> join(Tuple2<String, String> t1, Tuple2<String, Optional<Integer>> t2)
                    throws Exception {
                return new Tuple2<>(t1.f1, t2.f1.isPresent() ? t2.f1 : Optional.<Integer>empty());
            }
        });
    }
}
