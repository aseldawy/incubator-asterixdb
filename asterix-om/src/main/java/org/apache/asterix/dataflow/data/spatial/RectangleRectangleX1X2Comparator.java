/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.dataflow.data.spatial;

import java.nio.ByteBuffer;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Compares two two rectangles according to the left edge (x1) of the first
 * rectangle and right edge (x2) of the second rectangle.
 * 
 * @author Ahmed Eldawy
 */
public class RectangleRectangleX1X2Comparator implements ITuplePairComparator {

    /** The column index of the rectangle attribute in the first dataset */
    private int col0;

    /** The column index of the rectangle attribute in the second dataset */
    private int col1;

    /**
     * Initializes the comparator from the index of the point (or rectangle)
     * column in the two tuples.
     * Currently, we assume that the first x coordinate in the rectangle is the
     * smallest value (x1) and the second x coordinate is the largest (x2). If
     * the two corners of the rectangle are not necessarily sorted, then this
     * comparator will need to be modified to read both x-coordinates of the
     * two rectangles, sort them, and choose the smallest (largest) value accordingly.
     * 
     * @param col0
     *            the index of the point column in the first dataset
     * @param col2
     *            the index of the point column in the second dataset
     */
    public RectangleRectangleX1X2Comparator(int col0, int col1) {
        this.col0 = col0;
        this.col1 = col1;
    }

    @Override
    public int compare(IFrameTupleAccessor fta0, int tupidx0, IFrameTupleAccessor fta1, int tupidx1)
            throws HyracksDataException {
        ByteBuffer buffer = fta0.getBuffer();
        // x1 of the first rectangle
        double x1_0 = ADoubleSerializerDeserializer.getDouble(buffer.array(),
                fta0.getAbsoluteFieldStartOffset(tupidx0, col0));
        buffer = fta1.getBuffer();
        // x2 of the second rectangle. We add 8*2 to skip two doubles (x1 & y1)
        double x2_1 = ADoubleSerializerDeserializer.getDouble(buffer.array(),
                fta1.getAbsoluteFieldStartOffset(tupidx1, col1) + 8 * 2);
        if (x1_0 < x2_1)
            return -1;
        if (x1_0 > x2_1)
            return 1;
        return 0;
    }

}
