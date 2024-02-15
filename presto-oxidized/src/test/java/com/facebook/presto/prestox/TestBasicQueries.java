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

package com.facebook.presto.prestox;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.util.OptionalInt;

public class TestBasicQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoXQueryRunner.createQueryRunner(OptionalInt.of(1), OptionalInt.empty());
    }

    @Test
    public void testSelectAllValues()
    {
        assertQuery("SELECT * FROM (VALUES 1, 2, 3)", "VALUES 1, 2, 3");
    }

    @Test
    public void testSelectAllValuesMultipleColumns()
    {
        assertQuery("SELECT * FROM (VALUES (1, 2), (3, 4), (5, 6))", "VALUES (1, 2), (3, 4), (5, 6)");
    }
}
