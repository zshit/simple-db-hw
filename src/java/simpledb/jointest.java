package simpledb;

import java.io.File;
import java.io.IOException;
import simpledb.Predicate.Op;

public class jointest {

  public static void main(String[] args) {
    Type [] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
    String [] names = new String[]{"field0", "field1", "field2"};
    TupleDesc td = new TupleDesc(types, names);
    HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
    Database.getCatalog().addTable(table1, "table1");
    HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td);
    Database.getCatalog().addTable(table2, "table2");

    TransactionId tid = new TransactionId();

    /**
     * table scan
     */
    SeqScan seqScan1 = new SeqScan(tid, table1.getId(), "table1");
    SeqScan seqScan2 = new SeqScan(tid, table2.getId(), "table2");

    /**
     * filter
     */
    Filter filter = new Filter(new Predicate(0, Op.GREATER_THAN, new IntField(0)), seqScan1);
    Join join = new Join(new JoinPredicate(1, Op.EQUALS, 1), filter, seqScan2);
    try {
      join.open();
      while (join.hasNext()){
        Tuple tuple = join.next();
        System.out.println(tuple);
        System.out.println("---------tuple line --------");
      }
      join.close();
      Database.getBufferPool().transactionComplete(tid);
    } catch (DbException e) {
      e.printStackTrace();
    } catch (TransactionAbortedException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
