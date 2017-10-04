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
package org.apache.asterix.runtime.evaluators.functions.geo;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.functions.AbstractSTSingleGeometryDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class STXMinDescriptor extends AbstractSTSingleGeometryDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STXMinDescriptor();
        }
    };

    @Override
    protected Object evaluateOGCGeometry(OGCGeometry geometry) throws HyracksDataException {
        Geometry esriGeom = geometry.getEsriGeometry();
        if (esriGeom != null) {
            Envelope env = new Envelope();
            esriGeom.queryEnvelope(env);
            return env.getXMin();
        } else {
            throw new HyracksDataException(
                    "This method is not spported for the geometry type: " + geometry.geometryType());
        }

    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_X_MIN;
    }

}