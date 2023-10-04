/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.io.Resources;
import org.testng.ITest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_EXECUTION_TIME;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_RUN_TIME;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedLineitemAndOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersHll;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPart;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPartSupp;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createSupplier;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractTestNativeTpchQueries
        extends AbstractTestQueryFramework
        implements ITest
{
    private String testName = "";

    @BeforeMethod(alwaysRun = true)
    public void setCustomTestcaseName(Method method, Object[] testData)
    {
        this.testName = method.getName();
    }

    @Override
    public String getTestName()
    {
        return testName;
    }

    @DataProvider(name = "sessionProvider")
    public Object[][] sessionConfigProvider()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        return new Object[][] {
                new Object[] {
                        Session.builder(queryRunner.getDefaultSession())
                                .setSchema("tpcds")
                                .setSystemProperty(QUERY_MAX_RUN_TIME, "2m")
                                .setSystemProperty(QUERY_MAX_EXECUTION_TIME, "2m").build()}};
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createBucketedLineitemAndOrders(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createOrdersHll(queryRunner);
        createNation(queryRunner);
        createCustomer(queryRunner);
        createBucketedCustomer(queryRunner);
        createPart(queryRunner);
        createPartSupp(queryRunner);
        createRegion(queryRunner);
        createSupplier(queryRunner);
    }

    private static String getTpchQuery(int q)
            throws IOException
    {
        String sql = Resources.toString(Resources.getResource("tpch/queries/q" + q + ".sql"), UTF_8);
        sql = sql.replaceFirst("(?m);$", "");
        return sql;
    }

    // This test runs the 22 TPC-H queries.

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ1(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(1));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ2(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(2));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ3(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(3));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ4(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(4));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ5(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(5));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ6(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(6));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ7(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(7));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ8(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(8));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ9(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(9));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ10(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(10));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ11(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(11));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ12(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(12));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ13(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(13));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ14(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(14));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ15(String propChange, Session session)
            throws Exception
    {
        // Q15 doesn't reliably return correct results.
        // The same issue is observed with Presto java also.
        // The errors are on account of 2 causes:
        //  i) WITH expansion in Presto expands the query in place each time.
        //     As per SQL spec, the expansion should happen only once.
        // ii) On account of the double expansion, the aggregate value with double
        //     type has minor differences in each expansion causing the
        //     subquery to not always find an equal match in values.
        // Creating a table with the revenue SQL always returns correct results,
        assertQuerySucceeds(getTpchQuery(15));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ16(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(16));
    }

    // TODO This test is failing in CI often. The failures cannot be reproduced locally. Re-enable when failures are fixed.
//    @Ignore
    @Test(dataProvider = "sessionProvider")
    public void testTpchQ17(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(17));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ18(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(18));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ19(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(19));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ20(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(20));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ21(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(21));
    }

    @Test(dataProvider = "sessionProvider")
    public void testTpchQ22(String propChange, Session session)
            throws Exception
    {
        assertQuery(session, getTpchQuery(22));
    }
}
