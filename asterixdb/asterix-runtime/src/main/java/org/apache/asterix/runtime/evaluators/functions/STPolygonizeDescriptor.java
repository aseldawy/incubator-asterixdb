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

import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import org.apache.asterix.dataflow.data.nontagged.serde.AGeometrySerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.*;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.*;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class STPolygonizeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STPolygonizeDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_POLYGONIZE;
    }

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

                    @Override
                    @SuppressWarnings("unchecked")
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        eval.evaluate(tuple, inputArg);
                        byte[] bytes = inputArg.getByteArray();
                        int offset = inputArg.getStartOffset();
                        int len = inputArg.getLength();

                        AOrderedListType type = new AOrderedListType(BuiltinType.AGEOMETRY, null);
                        byte typeTag = inputArg.getByteArray()[inputArg.getStartOffset()];
                        ISerializerDeserializer serde;
                        if (typeTag == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                            serde = new AOrderedListSerializerDeserializer(type);
                        } else if (typeTag == ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                            serde = new AOrderedListSerializerDeserializer(type);
                        } else {
                            throw new TypeMismatchException(BuiltinFunctions.ST_POLYGONIZE, 0, typeTag,
                                    ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG,
                                    ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG);
                        }

                        ByteArrayInputStream inStream = new ByteArrayInputStream(bytes, offset + 1, len - 1);
                        DataInput dataIn = new DataInputStream(inStream);
                        IACursor cursor = ((IACollection) serde.deserialize(dataIn)).getCursor();
                        List<OGCGeometry> list = new ArrayList<>();
                        while (cursor.next()) {
                            IAObject object = cursor.get();
                            list.add(((AGeomety) object).getGeometry());
                        }
                        OGCGeometryCollection geometryCollection =
                                new OGCConcreteGeometryCollection(list, SpatialReference.create(4326));
                        try {
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AGEOMETRY)
                                    .serialize(new AGeomety(geometryCollection), out);
                        } catch (IOException e) {
                            throw new InvalidDataFormatException(getIdentifier(), e,
                                    ATypeTag.SERIALIZED_GEOMETRY_TYPE_TAG);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }
}
