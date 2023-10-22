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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
//import org.apache.calcite.jdbc.SimpleCalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class RelOptUtilTest {
  @Test
  public void readModels() throws Exception {
    SqlParser.Config parserConfig = SqlParser.Config.DEFAULT;
    SqlParser parser = SqlParser.create("select id from A", parserConfig);
    SqlNode node = parser.parseStmt();
    SqlKind kind = node.getKind();
    System.out.println(kind.sql);
    SqlBasicVisitor visitor = new SqlVisitorTest();
    node.accept(visitor);

    Class.forName("com.mysql.jdbc.Driver");
    Properties props = new Properties();
    props.put("model", "src/test/resources/maxModel.json");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", props);

    String sql = "select * from A";
    PreparedStatement ps = connection.prepareStatement(sql);
    ps.getResultSet();

    CalciteConnection con = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = con.getRootSchema();

    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(CalciteSchema.from(rootSchema),
            Arrays.asList("myschema"),
            new JavaTypeFactoryImpl(),
            CalciteConnectionConfig.DEFAULT);

    SqlOperatorTable operatorTable = SqlStdOperatorTable.instance();
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    SqlValidator.Config validatorconfig = SqlValidator.Config.DEFAULT;

    SqlValidator validator = SqlValidatorUtil.newValidator(operatorTable,
        catalogReader,
        typeFactory,
        validatorconfig);
    SqlNode validatedNode = validator.validate(node);


    FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
        .defaultSchema(rootSchema)
        .build();
    RelBuilder relBuilder = RelBuilder.create(frameworkConfig);
//    RelNode relNode = relBuilder.scan("test")a
//        .build();
//    String relStr = RelOptUtil.toString(relNode);
//    System.out.println(relStr);
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
    planner.addRule(CoreRules.FILTER_INTO_JOIN);

    RelOptCluster cluster = RelOptCluster.create(planner, relBuilder.getRexBuilder());
    SqlToRelConverter converter = new SqlToRelConverter(new RelOptTable.ViewExpander() {
      @Override
      public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath,
          @Nullable List<String> viewPath) {
        return null;
      }
    },
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());

    RelRoot relRoot = converter.convertQuery(validatedNode, false, true);
    relRoot = relRoot.withRel(converter.flattenTypes(relRoot.rel, true));
    relRoot = relRoot.withRel(RelDecorrelator.decorrelateQuery(relRoot.rel, relBuilder));
    RelNode relNode = relRoot.rel;
    System.out.println(RelOptUtil.toString(relNode));

//    RelNode relNode = RelDecorrelator.decorrelateQuery(relRoot.rel, relBuilder);
//    System.out.println(relNode.explain());
//    System.out.println(RelOptUtil.toString(relNode));


    //
    RelTraitSet desiredTraitSet =
        relNode.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
    relNode = planner.changeTraits(relNode, desiredTraitSet);
//    RelTraitSet traits = cluster.traitSet();
//    planner.changeTraits(relNode, traits);

    planner.setRoot(relNode);
    RelNode optimizedRelNode = planner.findBestExp();

    RelToSqlConverter relToSqlConverter =
        new JdbcImplementor(SqlDialect.DatabaseProduct.MYSQL.getDialect(),
                new JavaTypeFactoryImpl());
//    SqlImplementor.Result result = relToSqlConverter.visitRoot(optimizedRelNode);
    SqlImplementor.Result result = relToSqlConverter.visitInput(optimizedRelNode, 0);
    SqlNode sqlNodeConvertedBack = result.asStatement();
    System.out.println("----:");
    System.out.println(sqlNodeConvertedBack.toSqlString(SqlDialect.DatabaseProduct.MYSQL.getDialect()).getSql());
    System.out.println("----:");
  }

  @Test
  public void testParser() {
    SqlParser.Config config = SqlParser.Config.DEFAULT;
    SqlParser parser = SqlParser.create("select * from a", config);
    try {
      SqlNode node = parser.parseStmt();
      SqlKind kind = node.getKind();
      System.out.println(kind.sql);
      SqlBasicVisitor visitor = new SqlVisitorTest();
      node.accept(visitor);
    } catch (SqlParseException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test() throws Exception {

    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("EMP", new Table() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return null;
      }

      @Override
      public Statistic getStatistic() {
        return null;
      }

      @Override
      public Schema.TableType getJdbcTableType() {
        return null;
      }

      @Override
      public boolean isRolledUp(String column) {
        return false;
      }

      @Override
      public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
          @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
        return false;
      }
    });
    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(rootSchema)
        .build();


    RelBuilder builder = RelBuilder.create(config);
    RelNode root = builder.scan("EMP").build();
    RelOptUtil.toString(root);
  }
}
