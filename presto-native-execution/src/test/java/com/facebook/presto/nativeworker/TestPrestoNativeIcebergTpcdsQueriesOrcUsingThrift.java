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

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

@Test(groups = {"orc"})
public class TestPrestoNativeIcebergTpcdsQueriesOrcUsingThrift
        extends AbstractTestNativeTpcdsQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.storageFormat = "ORC";
        return PrestoNativeQueryRunnerUtils.createNativeIcebergQueryRunner(true, "ORC");
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        this.storageFormat = "ORC";
        return PrestoNativeQueryRunnerUtils.createJavaIcebergQueryRunner("ORC");
    }

    @Test
    public void doDeletesAndQuery() throws Exception
    {
        doDeletes();
        verifyDeletes();
        runAllQueries();
    }
}
