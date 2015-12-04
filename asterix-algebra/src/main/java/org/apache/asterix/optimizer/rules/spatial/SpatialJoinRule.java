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
package org.apache.asterix.optimizer.rules.spatial;

import java.util.List;

import org.apache.asterix.dataflow.data.spatial.UniformGridRectangleTokenizerFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Applies a specialized sub-plan for spatial join based on the PBSM algorithm.
 * 
 * @author Ahmed Eldawy
 */
public class SpatialJoinRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (!(op instanceof InnerJoinOperator))
            return false;
        InnerJoinOperator joinOp = (InnerJoinOperator) op;
        // The join condition has to be a function call
        ILogicalExpression joinCond = joinOp.getCondition().getValue();
        if (joinCond.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL)
            return false;
        // And the called function must be the "spatial-intersect" function (for now)
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) joinCond;
        FunctionIdentifier fi = fexp.getFunctionIdentifier();
        if (!fi.getName().equals("spatial-intersect"))
            return false;
        // Finally, all inputs of the join condition should be rectangles (for now)
        IVariableTypeEnvironment typeEnv = context.getOutputTypeEnvironment(op);
        for (Mutable<ILogicalExpression> operand : fexp.getArguments()) {
            Object type = typeEnv.getType(operand.getValue());
            if (operand != BuiltinType.ARECTANGLE)
                //if (!operand.toString().equals("RECTANGLE"))
                return false;
        }
        // At this point, we can apply the spatial join plan
        // For each input, Create the subplan (Tokenize -> HashPartition -> HashExchange -> Sort)
        // TODO retrieve grid information.
        double gx1 = -180, gx2 = 180, gy1 = -90, gy2 = 90;
        int columns = 36, rows = 18;
        UniformGridRectangleTokenizerFactory tokenizerFactory = new UniformGridRectangleTokenizerFactory(gx1, gy1, gx2,
                gy2, columns, rows);
        for (Mutable<ILogicalExpression> operand : fexp.getArguments()) {
            TokenizeOperator tokenizeOp = new TokenizeOperator(operand.getValue(), tokenizerFactory);
        }

        // For the two inputs, apply the planes-sweep join
        // TODO Auto-generated method stub
        return false;
    }

}
