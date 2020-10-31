package simpledb;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples in no particular
 * order. Tuples are stored on pages, each of which is a fixed size, and the file is simply a
 * collection of those pages. HeapFile works closely with HeapPage. The format of HeapPages is
 * described in the HeapPage constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

  private File file;

  private TupleDesc td;


  /**
   * Constructs a heap file backed by the specified file.
   *
   * @param f the file that stores the on-disk backing store for this heap file.
   */
  public HeapFile(File f, TupleDesc td) {
    // some code goes here
    this.file = f;
    this.td = td;
    Database.getCatalog().addTable(this);
  }

  /**
   * Returns the File backing this HeapFile on disk.
   *
   * @return the File backing this HeapFile on disk.
   */
  public File getFile() {
    // some code goes here
    return file;
  }

  /**
   * Returns an ID uniquely identifying this HeapFile. Implementation note: you will need to
   * generate this tableid somewhere to ensure that each HeapFile has a "unique id," and that you
   * always return the same value for a particular HeapFile. We suggest hashing the absolute file
   * name of the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
   *
   * @return an ID uniquely identifying this HeapFile.
   */
  public int getId() {
    return file.getAbsoluteFile().hashCode();
    // some code goes here
  }

  /**
   * Returns the TupleDesc of the table stored in this DbFile.
   *
   * @return TupleDesc of this DbFile.
   */
  public TupleDesc getTupleDesc() {
    // some code goes here
    return td;
  }

  // see DbFile.java for javadocs
  public Page readPage(PageId pid) {
    // some code goes here
    try {
      RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
      randomAccessFile.seek(pid.getPageNumber() * BufferPool.getPageSize());
      byte[] bytes = new byte[BufferPool.getPageSize()];
      randomAccessFile.readFully(bytes);
      return new HeapPage((HeapPageId) pid, bytes);
    } catch (IOException e) {
    }
    return null;
  }

  // see DbFile.java for javadocs
  public void writePage(Page page) throws IOException {
    // some code goes here
    // not necessary for lab1
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "wr");
    randomAccessFile.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
    randomAccessFile.write(page.getPageData());
    randomAccessFile.close();
  }

  /**
   * Returns the number of pages in this HeapFile.
   */
  public int numPages() {
    // some code goes here
    return Math.toIntExact(file.length() / BufferPool.getPageSize());
  }

  // see DbFile.java for javadocs
  public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    // some code goes here
    // not necessary for lab1
    if (t ==null){
      return null;
    }
    int pageSize = numPages();
    for (int i = 0; i < pageSize; i++) {
      HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY);
      if(page ==null){
        continue;
      }
      if(page.getNumEmptySlots() >0){
        page.insertTuple(t);
        return new ArrayList<>(Collections.singleton(page));
      }
    }

    Files.write(file.toPath(),new byte[BufferPool.getPageSize()], StandardOpenOption.APPEND);
    HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageSize), Permissions.READ_ONLY);
    heapPage.insertTuple(t);
    return new ArrayList<>(Collections.singleton(heapPage));
  }

  // see DbFile.java for javadocs
  public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
      TransactionAbortedException {
    // some code goes here
    // not necessary for lab1
    if (t ==null){
      return null;
    }

    HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_ONLY);
    if(heapPage == null){
      throw new DbException("");
    }
    heapPage.deleteTuple(t);
    try {
      writePage(heapPage);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new ArrayList<>(Collections.singleton(
        heapPage));
  }

  // see DbFile.java for javadocs
  public DbFileIterator iterator(TransactionId tid) {
    // some code goes here

    return new DbFileIterator() {

      int pages = numPages();
      int curPage = 0;
      HeapPageId curPageId;
      Iterator<Tuple> iterator ;

      @Override
      public void open() throws DbException, TransactionAbortedException {
        curPage = 0;
        curPageId = new HeapPageId(getId(), curPage);
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, curPageId, null);

        if(heapPage !=null){
          iterator = heapPage.iterator();
        }
      }

      @Override
      public boolean hasNext() throws DbException, TransactionAbortedException {
        if (pages <= 0 || curPage >= pages || curPageId == null) {
          return false;
        }
        if(iterator.hasNext()){
          return true;
        }
        if(curPage + 1 >=pages){
          return false;
        }
        HeapPageId pageId = new HeapPageId(getId(),curPage +1);
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, null);
        return heapPage.iterator().hasNext();
      }

      @Override
      public Tuple next()
          throws DbException, TransactionAbortedException, NoSuchElementException {
        if (curPageId == null) {
          throw new NoSuchElementException();
        }
        if(iterator.hasNext()){
          return iterator.next();
        }
        if(curPage + 1 >=pages){
          throw new NoSuchElementException();
        }
        curPage ++;
        HeapPageId pageId = new HeapPageId(getId(),curPage);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, null);
        iterator = page.iterator();
        if(iterator.hasNext()){
          return iterator.next();
        }
        throw new NoSuchElementException();
      }

      @Override
      public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
      }

      @Override
      public void close() {
        curPage = 0;
        curPageId = null;
      }
    };
  }

}

