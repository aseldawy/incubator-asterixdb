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
import org.apache.asterix.dataflow.data.nontagged.serde.AGeometrySerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.*;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;

public abstract class AbstractSTSingleGeometryDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    abstract protected Object evaluateOGCGeometry(OGCGeometry geometry) throws HyracksDataException;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            @SuppressWarnings("unchecked")
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IPointable argPtr0 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);

                    private final AMutableInt32 intRes = new AMutableInt32(0);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr0);

                        try {
                            byte[] bytes0 = argPtr0.getByteArray();
                            int offset0 = argPtr0.getStartOffset();
                            int len0 = argPtr0.getLength();

                            ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]);
                            if (tag != ATypeTag.GEOMETRY) {
                                throw new TypeMismatchException(getIdentifier(), 0, bytes0[offset0],
                                        ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                            }

                            DataInput dataIn0 =
                                    new DataInputStream(new ByteArrayInputStream(bytes0, offset0 + 1, len0 - 1));
                            OGCGeometry geometry0 =
                                    AGeometrySerializerDeserializer.INSTANCE.deserialize(dataIn0).getGeometry();

                            Object finalResult = evaluateOGCGeometry(geometry0);
                            if (finalResult == null) {
                                out.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                            } else if (finalResult instanceof Double) {
                                out.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                                out.writeDouble((double) finalResult);
                            } else if (finalResult instanceof Boolean) {
                                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN)
                                        .serialize((boolean) finalResult ? ABoolean.TRUE : ABoolean.FALSE, out);
                            } else if (finalResult instanceof Integer) {
                                intRes.setValue((int) finalResult);
                                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32)
                                        .serialize(intRes, out);
                            } else if (finalResult instanceof String) {
                                out.write(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                                out.write(UTF8StringUtil.writeStringToBytes((String) finalResult));
                            } else if (finalResult instanceof byte[]) {
                                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABINARY)
                                        .serialize(new ABinary((byte[]) finalResult), out);
                            } else if (finalResult instanceof OGCGeometry) {
                                out.writeByte(ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                                AGeometrySerializerDeserializer.INSTANCE
                                        .serialize(new AGeomety((OGCGeometry) finalResult), out);
                            }
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }
}
