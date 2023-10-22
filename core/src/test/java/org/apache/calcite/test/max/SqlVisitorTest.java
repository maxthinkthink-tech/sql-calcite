/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.test.max;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;

public class SqlVisitorTest extends SqlBasicVisitor<Void> {
  @Override
  public Void visit(SqlLiteral literal) {
    String fmt = "visit:kind:%s, type:%s, value:%s";
    String str = String.format(fmt, literal.getKind(), literal.getTypeName(), literal.toValue());
    System.out.println(str);
    return super.visit(literal);
  }

  @Override
  public Void visit(SqlCall call) {
    String fmt = "visit:kind:%s, type:%s, value:%s";
    String str = String.format(fmt, call.getKind(), call.getOperator(), call.getOperandList());
    System.out.println(str);
    return super.visit(call);
  }

  @Override
  public Void visit(SqlNodeList nodeList) {
    String fmt = "visit:kind:%s, type:%s";
    String str = String.format(fmt, nodeList.getKind(), nodeList.getList());
    System.out.println(str);
    return super.visit(nodeList);
  }

  @Override
  public Void visit(SqlIdentifier id) {
    String fmt = "visit:kind:%s, type:%s, names=%s";
    String str = String.format(fmt, id.getKind(), id.getSimple(), id.names);
    System.out.println(str);
    return super.visit(id);
  }

  @Override
  public Void visit(SqlDataTypeSpec type) {
    String fmt = "visit:kind:%s, type:%s, names=%s";
    String str = String.format(fmt, type.getKind(), type.getTypeName(), type.toString());
    System.out.println(str);
    return super.visit(type);
  }

  @Override
  public Void visit(SqlDynamicParam param) {
    return super.visit(param);
  }

  @Override
  public Void visit(SqlIntervalQualifier intervalQualifier) {
    return super.visit(intervalQualifier);
  }
}
