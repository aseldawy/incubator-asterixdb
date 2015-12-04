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

import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;

/**
 * A tokenizer factory that creates tokenizers of type {@link UniformGridRectangleTokenizer}
 * 
 * @author Ahmed Eldawy
 */
public class UniformGridRectangleTokenizerFactory implements IBinaryTokenizerFactory {

    private static final long serialVersionUID = 5604051607546274874L;
    /** Minimum x coordinate on the grid */
    private double gx1;
    /** Minimum y coordinate on the grid */
    private double gy1;
    /** Maximum x coordinate on the grid */
    private double gx2;
    /** Maximum y coordinate on the grid */
    private double gy2;
    /** Total number of rows in the grid */
    private int rows;
    /** Total number of columns in the grid */
    private int columns;

    public UniformGridRectangleTokenizerFactory(double gx1, double gy1, double gx2, double gy2, int columns, int rows) {
        this.gx1 = gx1;
        this.gy1 = gy1;
        this.gx2 = gx2;
        this.gy2 = gy2;
        this.columns = columns;
        this.rows = rows;

    }

    @Override
    public IBinaryTokenizer createTokenizer() {
        return new UniformGridRectangleTokenizer(gx1, gy1, gx2, gy2, columns, rows);
    }

}
