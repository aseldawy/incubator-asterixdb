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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;

/**
 * Tests if two rectangles overlap.
 * 
 * @author Ahmed Eldawy
 */
public class RectangleRectangleOverlapPredicate implements IPredicateEvaluator {

    /** Index of the rectangle attribute in the first dataset */
    private int colidx0;

    /** Index of the rectangle attribute in the second dataset */
    private int colidx1;

    /**
     * Initializes a new predicate evaluator that compares two tuples according
     * to rectangles attributes.
     * 
     * @param colidx0
     * @param colidx1
     */
    public RectangleRectangleOverlapPredicate(int colidx0, int colidx1) {
        this.colidx0 = colidx0;
        this.colidx1 = colidx1;
    }

    @Override
    public boolean evaluate(IFrameTupleAccessor fta0, int tupId0, IFrameTupleAccessor fta1, int tupId1) {
        // Read the coordinates of the first rectangle
        ByteBuffer buf = fta0.getBuffer();
        int offset = fta0.getAbsoluteFieldStartOffset(tupId0, colidx0);
        double r0_x1 = buf.getDouble(offset);
        double r0_y1 = buf.getDouble(offset + 8);
        double r0_x2 = buf.getDouble(offset + 16);
        double r0_y2 = buf.getDouble(offset + 24);
        // Read the coordinates of the second rectangle
        buf = fta1.getBuffer();
        offset = fta1.getAbsoluteFieldStartOffset(tupId1, colidx1);
        double r1_x1 = buf.getDouble(offset);
        double r1_y1 = buf.getDouble(offset + 8);
        double r1_x2 = buf.getDouble(offset + 16);
        double r1_y2 = buf.getDouble(offset + 24);

        // Evaluate the overlap of the two rectangles
        return r0_x2 > r1_x1 && r1_x2 > r0_x1 && r0_y2 > r1_y1 && r1_y2 > r0_y1;
    }

}
