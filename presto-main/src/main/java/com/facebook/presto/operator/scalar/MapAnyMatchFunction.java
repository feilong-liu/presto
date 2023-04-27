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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.sql.gen.lambda.BinaryFunctionInterface;

import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static java.lang.Boolean.TRUE;

@Description("Returns true if the map contains one or more elements that match the given predicate")
@ScalarFunction(value = "map_any_match")
public final class MapAnyMatchFunction
{
    private MapAnyMatchFunction() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean anyMatchBlock(
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valType,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType("function(K, V, boolean)") BinaryFunctionInterface function)
    {
        boolean hasNullResult = false;
        for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
            Object key = readNativeValue(keyType, mapBlock, i);
            Object value = readNativeValue(valType, mapBlock, i + 1);
            Boolean match = (Boolean) function.apply(key, value);
            if (TRUE.equals(match)) {
                return true;
            }
            if (match == null) {
                hasNullResult = true;
            }
        }
        if (hasNullResult) {
            return null;
        }
        return false;
    }
}
