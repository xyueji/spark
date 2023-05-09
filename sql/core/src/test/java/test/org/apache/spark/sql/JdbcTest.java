/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test.org.apache.spark.sql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Properties;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JdbcTest implements Serializable {
    private transient SparkSession spark;
    private transient JavaSparkContext jsc;

    @Before
    public void setUp() {
        spark = SparkSession.builder()
                .master("local[*]")
                .appName("testing")
                .getOrCreate();
        jsc = new JavaSparkContext(spark.sparkContext());
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    @Test
    public void testHive() {
        Properties properties = new Properties();
        properties.put("user", "impala");
        properties.put("password", "impala_+-");
        properties.put("driver", "org.apache.hive.jdbc.HiveDriver");
        Dataset<Row> hiveDs = spark.read().jdbc("jdbc:hive2://cdp-dt-dev05.data:10000?hive.resultset.use.unique.column.names=false",
                "(select * from xzg_test.t2 where id < 3) as tmp",
                properties);

        hiveDs.createOrReplaceTempView("x_hive_t1");
        hiveDs.show();
    }

    @Test
    public void testImpala() {
        Dataset<Row> impalaDs = spark.read().format("jdbc")
                .option("driver", "com.cloudera.impala.jdbc41.Driver")
                .option("url", "jdbc:impala://cdp-dt-test02.data:21050;AuthMech=3;")
                .option("user", "impala")
                .option("password", "impala_+-")
                .option("dbtable", "(select * from xzg_test.t2 where id < 4) as tmp")
                .load();

        impalaDs.createOrReplaceTempView("x_impala_t2");
        impalaDs.show();
    }
}
