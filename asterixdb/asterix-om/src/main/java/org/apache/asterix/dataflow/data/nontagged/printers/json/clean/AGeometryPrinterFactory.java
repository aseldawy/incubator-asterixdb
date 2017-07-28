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
package org.apache.asterix.dataflow.data.nontagged.printers.json.clean;

import com.esri.core.geometry.OperatorImportFromWkb;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class AGeometryPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final AGeometryPrinterFactory INSTANCE = new AGeometryPrinterFactory();

    public static final IPrinter PRINTER = (byte[] b, int s, int l, PrintStream ps) -> {
        byte[] data = Arrays.copyOfRange(b, s + 5, s + l);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        OGCGeometry geometry = OGCGeometry
                .createFromOGCStructure(OperatorImportFromWkb.local().executeOGC(0, buffer, null),
                        SpatialReference.create(4326));
        ps.print(geometry.asGeoJson());
    };

    @Override
    public IPrinter createPrinter() {
        return PRINTER;
    }
}
