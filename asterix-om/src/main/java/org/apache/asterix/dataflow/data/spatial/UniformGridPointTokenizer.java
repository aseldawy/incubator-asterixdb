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

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;

/**
 * Tokenizes points according to a uniform grid. Each point is assigned to the
 * cell in which it is contained. Points that are outside the input space, if
 * any, are assigned a special cell with ID -1
 * 
 * @author Ahmed Eldawy
 */
public class UniformGridPointTokenizer extends AbstractUniformGridTokenizer {

    /** A flag that tells whether the current value in CellIDToken is valid or not */
    private boolean valid;

    /**
     * Constructs a tokenizer given the grid information
     * 
     * @param x1
     * @param y1
     * @param x2
     * @param y2
     * @param rows
     * @param columns
     */
    public UniformGridPointTokenizer(double x1, double y1, double x2, double y2, int rows, int columns) {
        super(x1, y1, x2, y2, rows, columns);
        this.valid = false;
    }

    @Override
    public boolean hasNext() {
        return valid;
    }

    @Override
    public void next() {
        // Invalidate the current value as there is no next value
        valid = false;
    }

    @Override
    public void reset(byte[] data, int start, int length) {
        double x = ADoubleSerializerDeserializer.getDouble(data, start);
        double y = ADoubleSerializerDeserializer.getDouble(data, start + 8);
        if (x < gx1 || x >= gx2 || y < gy1 || y >= gy2) {
            this.currentCellID.setCellID(-1); // An indicator for overflow cell
        } else {
            int col = (int) Math.floor((x - gx1) * columns / (gx2 - gx1));
            int row = (int) Math.floor((y - gy1) * rows / (gy2 - gy1));
            this.currentCellID.setCellID(row * columns + col);
        }
    }

    @Override
    public short getTokensCount() {
        // A point always matches with exactly one grid cell
        return 1;
    }

}
