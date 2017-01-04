package io.confluent.ksql.structured;

import io.confluent.ksql.function.udf.KUDF;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.physical.PhysicalPlanBuilder;
import io.confluent.ksql.serde.KQLTopicSerDe;
import io.confluent.ksql.util.ExpressionUtil;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.Triplet;

import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.ArrayList;

public class SchemaKStream {

  final Schema schema;
  final KStream kStream;
  final Field keyField;
  final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;

  public SchemaKStream(Schema schema, KStream kStream, Field keyField) {
    this.schema = schema;
    this.kStream = kStream;
    this.keyField = keyField;
    this.genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
  }

  public SchemaKStream into(String kafkaTopicName, Serde<GenericRow> topicValueSerDe) {

    kStream.to(Serdes.String(), topicValueSerDe, kafkaTopicName);
    return this;
  }

  public SchemaKStream print() {
    KStream
        printKStream =
        kStream.map(new KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>() {
          @Override
          public KeyValue<String, GenericRow> apply(String s, GenericRow genericRow) {
            System.out.println(genericRow.toString());
            return new KeyValue<String, GenericRow>(s, genericRow);
          }

        });
    return this;
  }

  public SchemaKStream filter(Expression filterExpression) throws Exception {
    SQLPredicate predicate = new SQLPredicate(filterExpression, schema);
    KStream filteredKStream = kStream.filter(predicate.getPredicate());
    return new SchemaKStream(schema, filteredKStream, keyField);
  }

  public SchemaKStream select(Schema selectSchema) {

    KStream
        projectedKStream =
        kStream.map(new KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>() {
          @Override
          public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
            List<Object> newColumns = new ArrayList();
            for (Field schemaField : selectSchema.fields()) {
              newColumns.add(
                  row.getColumns().get(SchemaUtil.getFieldIndexByName(schema, schemaField.name())));
            }
            GenericRow newRow = new GenericRow(newColumns);
            return new KeyValue<String, GenericRow>(key, newRow);
          }
        });

    return new SchemaKStream(selectSchema, projectedKStream, keyField);
  }

  public SchemaKStream select(List<Expression> expressions, Schema selectSchema) throws Exception {
    ExpressionUtil expressionUtil = new ExpressionUtil();
    // TODO: Optimize to remove the code gen for constants and single columns references and use them directly.
    // TODO: Only use code get when we have real expression.
    List<Triplet<IExpressionEvaluator, int[], KUDF[]>> expressionEvaluators = new ArrayList<>();
    for (Expression expression : expressions) {
      Triplet<IExpressionEvaluator, int[], KUDF[]>
          expressionEvaluatorPair =
          expressionUtil.getExpressionEvaluator(expression, schema);
      expressionEvaluators.add(expressionEvaluatorPair);
    }
    KStream
        projectedKStream =
        kStream.map(new KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>() {
          @Override
          public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
            List<Object> newColumns = new ArrayList();
            for (int i = 0; i < expressions.size(); i++) {
              Expression expression = expressions.get(i);
              int[] parameterIndexes = expressionEvaluators.get(i).getSecond();
              KUDF[] kudfs = expressionEvaluators.get(i).getThird();
              Object[] parameterObjects = new Object[parameterIndexes.length];
              for (int j = 0; j < parameterIndexes.length; j++) {
                if (parameterIndexes[j] < 0) {
                  parameterObjects[j] = kudfs[j];
                } else {
                  parameterObjects[j] = genericRowValueTypeEnforcer.enforceFieldType
                      (parameterIndexes[j],row.getColumns().get(parameterIndexes[j]));
//                  if (row.getColumns().get(parameterIndexes[j]) instanceof CharSequence) {
//                    parameterObjects[j] = row.getColumns().get(parameterIndexes[j]).toString();
//                  } else {
//                    parameterObjects[j] = row.getColumns().get(parameterIndexes[j]);
//                  }
                }
              }
              Object columnValue = null;
              try {
                columnValue = expressionEvaluators.get(i).getFirst().evaluate(parameterObjects);
              } catch (InvocationTargetException e) {
                e.printStackTrace();
              }
              newColumns.add(columnValue);

            }
            GenericRow newRow = new GenericRow(newColumns);
            return new KeyValue<String, GenericRow>(key, newRow);
          }
        });

    return new SchemaKStream(selectSchema, projectedKStream, keyField);
  }

  public SchemaKStream leftJoin(SchemaKTable schemaKTable, Schema joinSchema, Field joinKey,
                                Serde<GenericRow> resultValueSerDe) {

    KStream
        joinedKStream =
        kStream.leftJoin(schemaKTable.getkTable(),
                         new ValueJoiner<GenericRow, GenericRow, GenericRow>() {
                           @Override
                           public GenericRow apply(GenericRow leftGenericRow,
                                                   GenericRow rightGenericRow) {
                             List<Object> columns = new ArrayList<>();
                             columns.addAll(leftGenericRow.getColumns());
                             if (rightGenericRow == null) {
                               for (int i = leftGenericRow.getColumns().size();
                                    i < joinSchema.fields().size(); i++) {
                                 columns.add(null);
                               }
                             } else {
                               columns.addAll(rightGenericRow.getColumns());
                             }

                             GenericRow joinGenericRow = new GenericRow(columns);
                             return joinGenericRow;
                           }
                         }, Serdes.String(), resultValueSerDe);

    return new SchemaKStream(joinSchema, joinedKStream, joinKey);
  }

  public SchemaKStream selectKey(Field newKeyField) {
    if (keyField.name().equalsIgnoreCase(newKeyField.name())) {
      return this;
    }

    KStream keyedKStream = kStream.selectKey(new KeyValueMapper<String, GenericRow, String>() {
      @Override
      public String apply(String key, GenericRow value) {

        String
            newKey =
            value.getColumns().get(SchemaUtil.getFieldIndexByName(schema, newKeyField.name()))
                .toString();
        return newKey;
      }
    });

    return new SchemaKStream(schema, keyedKStream, newKeyField);
  }

  public Field getKeyField() {
    return keyField;
  }

  public Schema getSchema() {
    return schema;
  }

  public KStream getkStream() {
    return kStream;
  }
}
