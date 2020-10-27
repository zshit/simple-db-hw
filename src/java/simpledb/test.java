package simpledb;

import java.io.File;

public class test {

  public static void main(String[] args) throws DbException, TransactionAbortedException {

    Type [] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
    String [] names = new String[]{"field0", "field1", "field2"};
    TupleDesc tupleDesc = new TupleDesc(types, names);
    HeapFile heapFile = new HeapFile(new File("some_data_file.dat") ,tupleDesc);
    Database.getCatalog().addTable(heapFile);
    TransactionId tid = new TransactionId();
    SeqScan seqScan = new SeqScan(tid,heapFile.getId());
    seqScan.open();
    while(seqScan.hasNext()){
      Tuple tuple = seqScan.next();
      System.out.println(tuple);
    }
    seqScan.close();
  }

}
