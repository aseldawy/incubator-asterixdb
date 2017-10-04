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

package org.apache.asterix.runtime.functions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.utils.CodeGenHelper;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.aggregates.collections.FirstElementAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.collections.ListifyAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.collections.LocalFirstElementAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSTUnionAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableGlobalAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableGlobalSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableIntermediateAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableIntermediateSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableLocalAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableLocalSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableLocalSqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableLocalSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableSqlCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableSqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.AvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.CountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.GlobalAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.GlobalSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.IntermediateAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.IntermediateSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSqlMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSqlMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.MaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.MinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.STUnionAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.stream.EmptyStreamAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.stream.NonEmptyStreamAggregateDescriptor;
import org.apache.asterix.runtime.evaluators.accessors.CircleCenterAccessor;
import org.apache.asterix.runtime.evaluators.accessors.CircleRadiusAccessor;
import org.apache.asterix.runtime.evaluators.accessors.LineRectanglePolygonAccessor;
import org.apache.asterix.runtime.evaluators.accessors.PointXCoordinateAccessor;
import org.apache.asterix.runtime.evaluators.accessors.PointYCoordinateAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalDayAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalHourAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalEndAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalEndDateAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalEndDatetimeAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalEndTimeAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalStartAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalStartDateAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalStartDatetimeAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalStartTimeAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalMillisecondAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalMinuteAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalMonthAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalSecondAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalYearAccessor;
import org.apache.asterix.runtime.evaluators.comparisons.EqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.GreaterThanDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.GreaterThanOrEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.LessThanDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.LessThanOrEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.NotEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ABinaryBase64StringConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ABinaryHexStringConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ABooleanConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ACircleConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADateConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADateTimeConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADayTimeDurationConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADoubleConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADurationConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AFloatConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AInt16ConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AInt32ConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AInt64ConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AInt8ConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AIntervalConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AIntervalStartFromDateConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AIntervalStartFromDateTimeConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AIntervalStartFromTimeConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ALineConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.APoint3DConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.APointConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.APolygonConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ARectangleConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AStringConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ATimeConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AUUIDFromStringConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AYearMonthDurationConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ClosedRecordConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.OpenRecordConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.OrderedListConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.UnorderedListConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.*;
import org.apache.asterix.runtime.evaluators.functions.binary.BinaryConcatDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.BinaryLengthDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.FindBinaryDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.FindBinaryFromDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.ParseBinaryDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.PrintBinaryDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.SubBinaryFromDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.SubBinaryFromToDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessByIndexDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessByNameDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessNestedDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.GetRecordFieldValueDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.GetRecordFieldsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.ParseGeoJSONDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordAddFieldsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordMergeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordPairsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordRemoveFieldsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.AdjustDateTimeForTimeZoneDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.AdjustTimeForTimeZoneDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CalendarDuartionFromDateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CalendarDurationFromDateTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CurrentDateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CurrentDateTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CurrentTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DateFromDatetimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DateFromUnixTimeInDaysDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DatetimeFromDateAndTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DatetimeFromUnixTimeInMsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DatetimeFromUnixTimeInSecsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DayOfWeekDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DayTimeDurationGreaterThanComparatorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DayTimeDurationLessThanComparatorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DurationEqualDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DurationFromIntervalDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DurationFromMillisecondsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DurationFromMonthsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.GetDayTimeDurationDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.GetOverlappingIntervalDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.GetYearMonthDurationDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalAfterDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalBeforeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalBinDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalCoveredByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalCoversDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalEndedByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalEndsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalMeetsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalMetByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalOverlappedByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalOverlapsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalStartedByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalStartsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.MillisecondsFromDayTimeDurationDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.MonthsFromYearMonthDurationDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.OverlapBinsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.OverlapDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.ParseDateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.ParseDateTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.ParseTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.PrintDateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.PrintDateTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.PrintTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.TimeFromDatetimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.TimeFromUnixTimeInMsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.UnixTimeFromDateInDaysDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.UnixTimeFromDatetimeInMsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.UnixTimeFromDatetimeInSecsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.UnixTimeFromTimeInMsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.YearMonthDurationGreaterThanComparatorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.YearMonthDurationLessThanComparatorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STAreaDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STMakePointDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STMakePoint3DDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STMakePoint3DWithMDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STIntersectsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STUnionDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STIsCollectionDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STContainsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STCrossesDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STDisjointDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STOverlapsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STTouchesDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STWithinDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STIsEmptyDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STIsSimpleDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STCoordDimDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STDimensionDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STGeomentryTypeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STMDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STNRingsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STNPointsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STNumGeometriesDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STNumInteriorRingsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STSRIDDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STXDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STYDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STXMaxDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STXMinDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STYMaxDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STYMinDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STZDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STZMaxDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STZMinDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STAsBinaryDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STAsTextDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STAsGeoJSONDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STDistanceDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STLengthDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STGeomFromTextDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STGeomFromTextSRIDDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STGeomFromWKBDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STLineFromMultiPointDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STMakeEnvelopeDescriptorSRID;
import org.apache.asterix.runtime.evaluators.functions.geo.STIsClosedDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STIsRingDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STRelateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STBoundaryDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STEndPointDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STEnvelopeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STExteriorRingDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STGeometryNDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STInteriorRingNDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STPointNDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STStartPointDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STDifferenceDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STIntersectionDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STSymDifferenceDescriptor;
import org.apache.asterix.runtime.evaluators.functions.geo.STPolygonizeDescriptor;
import org.apache.asterix.runtime.runningaggregates.std.TidRunningAggregateDescriptor;
import org.apache.asterix.runtime.unnestingfunctions.std.RangeDescriptor;
import org.apache.asterix.runtime.unnestingfunctions.std.ScanCollectionDescriptor;
import org.apache.asterix.runtime.unnestingfunctions.std.SubsetCollectionDescriptor;

/**
 * This class (statically) holds a list of function descriptor factories.
 */
public final class FunctionCollection {

    private static final String FACTORY = "FACTORY";
    private static final List<IFunctionDescriptorFactory> temp = new ArrayList<>();

    static {
        // unnesting function
        temp.add(TidRunningAggregateDescriptor.FACTORY);
        temp.add(ScanCollectionDescriptor.FACTORY);
        temp.add(RangeDescriptor.FACTORY);
        temp.add(SubsetCollectionDescriptor.FACTORY);

        // aggregate functions
        temp.add(ListifyAggregateDescriptor.FACTORY);
        temp.add(CountAggregateDescriptor.FACTORY);
        temp.add(AvgAggregateDescriptor.FACTORY);
        temp.add(LocalAvgAggregateDescriptor.FACTORY);
        temp.add(IntermediateAvgAggregateDescriptor.FACTORY);
        temp.add(GlobalAvgAggregateDescriptor.FACTORY);
        temp.add(SumAggregateDescriptor.FACTORY);
        temp.add(LocalSumAggregateDescriptor.FACTORY);
        temp.add(MaxAggregateDescriptor.FACTORY);
        temp.add(LocalMaxAggregateDescriptor.FACTORY);
        temp.add(MinAggregateDescriptor.FACTORY);
        temp.add(LocalMinAggregateDescriptor.FACTORY);
        temp.add(FirstElementAggregateDescriptor.FACTORY);
        temp.add(LocalFirstElementAggregateDescriptor.FACTORY);

        // serializable aggregates
        temp.add(SerializableCountAggregateDescriptor.FACTORY);
        temp.add(SerializableAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableIntermediateAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableGlobalAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableSumAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalSumAggregateDescriptor.FACTORY);

        // scalar aggregates
        temp.add(ScalarCountAggregateDescriptor.FACTORY);
        temp.add(ScalarAvgAggregateDescriptor.FACTORY);
        temp.add(ScalarSumAggregateDescriptor.FACTORY);
        temp.add(ScalarMaxAggregateDescriptor.FACTORY);
        temp.add(ScalarMinAggregateDescriptor.FACTORY);
        temp.add(EmptyStreamAggregateDescriptor.FACTORY);
        temp.add(NonEmptyStreamAggregateDescriptor.FACTORY);

        // SQL aggregates
        temp.add(SqlCountAggregateDescriptor.FACTORY);
        temp.add(SqlAvgAggregateDescriptor.FACTORY);
        temp.add(LocalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(IntermediateSqlAvgAggregateDescriptor.FACTORY);
        temp.add(GlobalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SqlSumAggregateDescriptor.FACTORY);
        temp.add(LocalSqlSumAggregateDescriptor.FACTORY);
        temp.add(SqlMaxAggregateDescriptor.FACTORY);
        temp.add(LocalSqlMaxAggregateDescriptor.FACTORY);
        temp.add(SqlMinAggregateDescriptor.FACTORY);
        temp.add(LocalSqlMinAggregateDescriptor.FACTORY);

        // SQL serializable aggregates
        temp.add(SerializableSqlCountAggregateDescriptor.FACTORY);
        temp.add(SerializableSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableIntermediateSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableGlobalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableSqlSumAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalSqlSumAggregateDescriptor.FACTORY);

        // SQL scalar aggregates
        temp.add(ScalarSqlCountAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlAvgAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlSumAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlMaxAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlMinAggregateDescriptor.FACTORY);

        // boolean functions
        temp.add(AndDescriptor.FACTORY);
        temp.add(OrDescriptor.FACTORY);

        // Record constructors
        temp.add(ClosedRecordConstructorDescriptor.FACTORY);
        temp.add(OpenRecordConstructorDescriptor.FACTORY);

        // List constructors
        temp.add(OrderedListConstructorDescriptor.FACTORY);
        temp.add(UnorderedListConstructorDescriptor.FACTORY);

        // Sleep function
        temp.add(SleepDescriptor.FACTORY);

        // Inject failure function
        temp.add(InjectFailureDescriptor.FACTORY);

        // Switch case
        temp.add(SwitchCaseDescriptor.FACTORY);

        // null functions
        temp.add(IsMissingDescriptor.FACTORY);
        temp.add(IsNullDescriptor.FACTORY);
        temp.add(IsUnknownDescriptor.FACTORY);
        temp.add(IsSystemNullDescriptor.FACTORY);
        temp.add(CheckUnknownDescriptor.FACTORY);
        temp.add(IfMissingDescriptor.FACTORY);
        temp.add(IfNullDescriptor.FACTORY);
        temp.add(IfMissingOrNullDescriptor.FACTORY);

        // uuid generators (zero independent functions)
        temp.add(CreateUUIDDescriptor.FACTORY);
        temp.add(UUIDDescriptor.FACTORY);
        temp.add(CreateQueryUIDDescriptor.FACTORY);
        temp.add(CurrentDateDescriptor.FACTORY);
        temp.add(CurrentTimeDescriptor.FACTORY);
        temp.add(CurrentDateTimeDescriptor.FACTORY);

        // TODO: decide how should we deal these two weird functions as
        // the number of arguments of the function depend on the first few arguments.
        temp.add(SimilarityJaccardPrefixDescriptor.FACTORY);
        temp.add(SimilarityJaccardPrefixCheckDescriptor.FACTORY);

        //Geo functions
        temp.add(ScalarSTUnionAggregateDescriptor.FACTORY);
        temp.add(STUnionAggregateDescriptor.FACTORY);

        // functions that need generated class for null-handling.
        List<IFunctionDescriptorFactory> functionsToInjectUnkownHandling = new ArrayList<>();

        // Element accessors.
        functionsToInjectUnkownHandling.add(FieldAccessByIndexDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(FieldAccessByNameDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(FieldAccessNestedDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AnyCollectionMemberDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(GetItemDescriptor.FACTORY);

        // Numeric functions
        functionsToInjectUnkownHandling.add(NumericUnaryMinusDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericAddDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericDivideDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericMultiplyDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericSubDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericModuloDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericCaretDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NotDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(LenDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericAbsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericCeilingDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericFloorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericRoundDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericRoundHalfToEvenDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericRoundHalfToEven2Descriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericACosDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericASinDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericATanDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericCosDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericSinDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericTanDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericExpDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericLnDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericLogDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericSqrtDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericSignDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericTruncDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NumericATan2Descriptor.FACTORY);

        // Comparisons.
        functionsToInjectUnkownHandling.add(EqualsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(GreaterThanDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(GreaterThanOrEqualsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(LessThanDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(LessThanOrEqualsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(NotEqualsDescriptor.FACTORY);

        // Binary functions
        functionsToInjectUnkownHandling.add(BinaryLengthDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ParseBinaryDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(PrintBinaryDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(BinaryConcatDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SubBinaryFromDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SubBinaryFromToDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(FindBinaryDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(FindBinaryFromDescriptor.FACTORY);

        // String functions
        functionsToInjectUnkownHandling.add(StringLikeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringContainsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringEndsWithDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringStartsWithDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SubstringDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringEqualDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringLowerCaseDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringUpperCaseDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringLengthDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(Substring2Descriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SubstringBeforeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SubstringAfterDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringToCodePointDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(CodePointToStringDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringConcatDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringJoinDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringRegExpContainsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringRegExpContainsWithFlagDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringRegExpLikeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringRegExpLikeWithFlagDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringRegExpPositionDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringRegExpPositionWithFlagDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringRegExpReplaceDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringRegExpReplaceWithFlagsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringInitCapDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringTrimDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringLTrimDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringRTrimDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringTrim2Descriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringLTrim2Descriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringRTrim2Descriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringPositionDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringRepeatDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(StringSplitDescriptor.FACTORY);

        // Constructors
        functionsToInjectUnkownHandling.add(ABooleanConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ABinaryHexStringConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ABinaryBase64StringConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AStringConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AInt8ConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AInt16ConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AInt32ConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AInt64ConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AFloatConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ADoubleConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(APointConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(APoint3DConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ALineConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(APolygonConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ACircleConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ARectangleConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ATimeConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ADateConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ADateTimeConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ADurationConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AYearMonthDurationConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ADayTimeDurationConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AUUIDFromStringConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AIntervalConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AIntervalStartFromDateConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AIntervalStartFromDateTimeConstructorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AIntervalStartFromTimeConstructorDescriptor.FACTORY);

        // Spatial
        functionsToInjectUnkownHandling.add(CreatePointDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(CreateLineDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(CreatePolygonDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(CreateCircleDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(CreateRectangleDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SpatialAreaDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SpatialDistanceDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SpatialIntersectDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(CreateMBRDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SpatialCellDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(PointXCoordinateAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(PointYCoordinateAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(CircleRadiusAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(CircleCenterAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(LineRectanglePolygonAccessor.FACTORY);

        // fuzzyjoin function
        functionsToInjectUnkownHandling.add(PrefixLenJaccardDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(WordTokensDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(HashedWordTokensDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(CountHashedWordTokensDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(GramTokensDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(HashedGramTokensDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(CountHashedGramTokensDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(EditDistanceDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(EditDistanceCheckDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(EditDistanceStringIsFilterableDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(EditDistanceListIsFilterableDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(EditDistanceContainsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SimilarityJaccardDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SimilarityJaccardCheckDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SimilarityJaccardSortedDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(SimilarityJaccardSortedCheckDescriptor.FACTORY);

        // full-text function
        functionsToInjectUnkownHandling.add(FullTextContainsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(FullTextContainsWithoutOptionDescriptor.FACTORY);

        // Record functions.
        functionsToInjectUnkownHandling.add(GetRecordFieldsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(GetRecordFieldValueDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DeepEqualityDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(RecordMergeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(RecordAddFieldsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(RecordRemoveFieldsDescriptor.FACTORY);

        // Spatial and temporal type accessors
        functionsToInjectUnkownHandling.add(TemporalYearAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalMonthAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalDayAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalHourAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalMinuteAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalSecondAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalMillisecondAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalIntervalStartAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalIntervalEndAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalIntervalStartDateAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalIntervalEndDateAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalIntervalStartTimeAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalIntervalEndTimeAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalIntervalStartDatetimeAccessor.FACTORY);
        functionsToInjectUnkownHandling.add(TemporalIntervalEndDatetimeAccessor.FACTORY);

        // Temporal functions
        functionsToInjectUnkownHandling.add(UnixTimeFromDateInDaysDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(UnixTimeFromTimeInMsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(UnixTimeFromDatetimeInMsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(UnixTimeFromDatetimeInSecsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DateFromUnixTimeInDaysDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DateFromDatetimeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(TimeFromUnixTimeInMsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(TimeFromDatetimeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DatetimeFromUnixTimeInMsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DatetimeFromUnixTimeInSecsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DatetimeFromDateAndTimeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(CalendarDurationFromDateTimeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(CalendarDuartionFromDateDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AdjustDateTimeForTimeZoneDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(AdjustTimeForTimeZoneDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalBeforeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalAfterDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalMeetsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalMetByDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalOverlapsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalOverlappedByDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(OverlapDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalStartsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalStartedByDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalCoversDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalCoveredByDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalEndsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalEndedByDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DurationFromMillisecondsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DurationFromMonthsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(YearMonthDurationGreaterThanComparatorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(YearMonthDurationLessThanComparatorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DayTimeDurationGreaterThanComparatorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DayTimeDurationLessThanComparatorDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(MonthsFromYearMonthDurationDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(MillisecondsFromDayTimeDurationDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DurationEqualDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(GetYearMonthDurationDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(GetDayTimeDurationDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IntervalBinDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(OverlapBinsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DayOfWeekDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ParseDateDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ParseTimeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ParseDateTimeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(PrintDateDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(PrintTimeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(PrintDateTimeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(GetOverlappingIntervalDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(DurationFromIntervalDescriptor.FACTORY);

        // Type functions.
        functionsToInjectUnkownHandling.add(IsBooleanDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IsNumberDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IsStringDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IsArrayDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(IsObjectDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ToBooleanDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ToStringDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ToDoubleDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(ToBigIntDescriptor.FACTORY);

        // Cast function
        functionsToInjectUnkownHandling.add(CastTypeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(CastTypeLaxDescriptor.FACTORY);

        // Record function
        functionsToInjectUnkownHandling.add(RecordPairsDescriptor.FACTORY);

        //GeoJSON
        functionsToInjectUnkownHandling.add(ParseGeoJSONDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STAreaDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STMakePointDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STMakePoint3DDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STMakePoint3DWithMDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STIntersectsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STUnionDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STIsCollectionDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STContainsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STCrossesDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STDisjointDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STEqualsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STOverlapsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STTouchesDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STWithinDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STIsEmptyDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STIsSimpleDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STCoordDimDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STDimensionDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STGeomentryTypeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STMDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STNRingsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STNPointsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STNumGeometriesDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STNumInteriorRingsDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STSRIDDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STXDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STYDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STXMaxDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STXMinDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STYMaxDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STYMinDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STZDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STZMaxDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STZMinDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STAsBinaryDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STAsTextDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STAsGeoJSONDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STDistanceDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STLengthDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STGeomFromTextDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STGeomFromTextSRIDDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STGeomFromWKBDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STLineFromMultiPointDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STMakeEnvelopeDescriptorSRID.FACTORY);
        functionsToInjectUnkownHandling.add(STIsClosedDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STIsRingDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STRelateDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STBoundaryDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STEndPointDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STEnvelopeDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STExteriorRingDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STGeometryNDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STInteriorRingNDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STPointNDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STStartPointDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STDifferenceDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STIntersectionDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STSymDifferenceDescriptor.FACTORY);
        functionsToInjectUnkownHandling.add(STPolygonizeDescriptor.FACTORY);

        List<IFunctionDescriptorFactory> generatedFactories = new ArrayList<>();
        for (IFunctionDescriptorFactory factory : functionsToInjectUnkownHandling) {
            generatedFactories
                    .add(getGeneratedFunctionDescriptorFactory(factory.createFunctionDescriptor().getClass()));
        }
        temp.addAll(generatedFactories);
    }

    public static List<IFunctionDescriptorFactory> getFunctionDescriptorFactories() {
        return temp;
    }

    /**
     * Gets the generated function descriptor factory from an <code>IFunctionDescriptor</code>
     * implementation class.
     *
     * @param cl,
     *            the class of an <code>IFunctionDescriptor</code> implementation.
     * @return the IFunctionDescriptorFactory instance defined in the class.
     */
    private static IFunctionDescriptorFactory getGeneratedFunctionDescriptorFactory(Class<?> cl) {
        try {
            String className = CodeGenHelper.getGeneratedClassName(cl.getName(),
                    CodeGenHelper.DEFAULT_SUFFIX_FOR_GENERATED_CLASS);
            Class<?> generatedCl = cl.getClassLoader().loadClass(className);
            Field factory = generatedCl.getDeclaredField(FACTORY);
            return (IFunctionDescriptorFactory) factory.get(null);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private FunctionCollection() {
    }
}
