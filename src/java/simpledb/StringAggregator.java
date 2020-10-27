package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;
  private int gbfield;
  private Type gbfieldType;
  private int aggregationField;
  private Op op;
  Map<Field, Integer> integerAggregatorMap = new HashMap<>();

  /**
   * Aggregate constructor
   *
   * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if
   *                    there is no grouping
   * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no
   *                    grouping
   * @param afield      the 0-based index of the aggregate field in the tuple
   * @param what        aggregation operator to use -- only supports COUNT
   * @throws IllegalArgumentException if what != COUNT
   */

  public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    // some code goes here
    if (!Op.COUNT.equals(what)) {
      throw new IllegalArgumentException("only supports COUNT");
    }
    this.gbfield = gbfield;
    this.gbfieldType = gbfieldtype;
    this.aggregationField = afield;
    this.op = what;
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the constructor
   *
   * @param tup the Tuple containing an aggregate field and a group-by field
   */
  public void mergeTupleIntoGroup(Tuple tup) {
    // some code goes here
    Field curKeyField;
    if (gbfieldType == null) {
      curKeyField = new IntField(0);
    } else {
      curKeyField = tup.getField(gbfield);
    }
    integerAggregatorMap.putIfAbsent(curKeyField, null);
    Field field = tup.getField(aggregationField);
    if (field == null) {
      return;
    }
    Integer aggregateValue = integerAggregatorMap.get(curKeyField);
    if(aggregateValue == null){
      aggregateValue = 0;
    }
    integerAggregatorMap.put(curKeyField, aggregateValue + 1);

  }

  /**
   * Create a OpIterator over group aggregate results.
   *
   * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if using group, or a
   * single (aggregateVal) if no grouping. The aggregateVal is determined by the type of aggregate
   * specified in the constructor.
   */
  public OpIterator iterator() {
    // some code goes here
    TupleDesc td;
    if (gbfieldType == null) {
      td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
    } else {
      td = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE},
          new String[]{"groupVal", "aggregateVal"});
    }
    List<Tuple> tupleList = new ArrayList<>();
    for (Entry<Field, Integer> objectIntegerEntry : integerAggregatorMap.entrySet()) {
      Tuple tuple = new Tuple(td);
      if (td.numFields() == 1) {
        tuple.setField(0, new IntField(objectIntegerEntry.getValue()));
      } else {
        tuple.setField(0, objectIntegerEntry.getKey());
        Integer value = objectIntegerEntry.getValue();
        tuple.setField(1, new IntField(value));
      }
      tupleList.add(tuple);
    }
    return new Operator() {
      private Iterator<Tuple> iterator = tupleList.iterator();
      @Override
      protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if(iterator.hasNext()){
          return iterator.next();
        }
        return  null;
      }

      @Override
      public OpIterator[] getChildren() {
        return new OpIterator[0];
      }

      @Override
      public void setChildren(OpIterator[] children) {

      }

      @Override
      public void rewind() throws DbException, TransactionAbortedException {
        super.close();
        super.open();
        iterator = tupleList.iterator();
      }

      @Override
      public TupleDesc getTupleDesc() {
        return td;
      }
    };
  }

}
