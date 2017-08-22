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
package org.apache.asterix.runtime.evaluators.functions;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import org.apache.asterix.dataflow.data.nontagged.serde.AGeometrySerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.om.base.AGeomety;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import java.io.*;

public abstract class AbstractSTGeometryNDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    abstract protected OGCGeometry evaluateOGCGeometry(OGCGeometry geometry, int n) throws HyracksDataException;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws HyracksDataException {

                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable inputArg = new VoidPointable();
                    private IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);
                    private IPointable inputArg0 = new VoidPointable();
                    private IScalarEvaluator eval0 = args[1].createScalarEvaluator(ctx);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        eval.evaluate(tuple, inputArg);
                        byte[] data = inputArg.getByteArray();
                        int offset = inputArg.getStartOffset();
                        int len = inputArg.getLength();

                        eval0.evaluate(tuple, inputArg0);
                        byte[] data0 = inputArg0.getByteArray();
                        int offset0 = inputArg0.getStartOffset();

                        if (data[offset] != ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG) {
                            throw new TypeMismatchException(getIdentifier(), 0, data[offset],
                                    ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                        }
                        if (data0[offset0] != ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
                            throw new TypeMismatchException(getIdentifier(), 0, data0[offset0],
                                    ATypeTag.SERIALIZED_INT64_TYPE_TAG);
                        }

                        ByteArrayInputStream inStream = new ByteArrayInputStream(data, offset + 1, len - 1);
                        DataInput dataIn = new DataInputStream(inStream);
                        OGCGeometry geometry =
                                AGeometrySerializerDeserializer.INSTANCE.deserialize(dataIn).getGeometry();
                        int n = (int) AInt64SerializerDeserializer.getLong(data0, offset0 + 1);

                        OGCGeometry geometryN = evaluateOGCGeometry(geometry, n);
                        try {
                            out.writeByte(ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                            AGeometrySerializerDeserializer.INSTANCE.serialize(new AGeomety(geometryN), out);
                            result.set(resultStorage);
                        } catch (IOException e) {
                            throw new InvalidDataFormatException(getIdentifier(), e,
                                    ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                        }
                    }
                };
            }
        };
    }
}
