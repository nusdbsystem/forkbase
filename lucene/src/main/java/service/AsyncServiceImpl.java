package service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import pojo.Const;

@Service
public class AsyncServiceImpl implements AsyncService {
  private static final Logger logger = LogManager.getLogger(AsyncServiceImpl.class);

  @Async
  @Override
  public void asyncIndexFile(Path path) {
    try {
      Directory directory = FSDirectory.open(Paths.get(Const.INDEX_DIR));
      IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());

      iwc.setOpenMode(OpenMode.CREATE);

      // Optional: for better indexing performance, if you
      // are indexing many documents, increase the RAM
      // buffer. But if you do this, increase the max heap
      // size to the JVM (eg add -Xmx512m or -Xmx1g):
      //
      // iwc.setRAMBufferSizeMB(256.0);

      IndexWriter writer = new IndexWriter(directory, iwc);
      indexDocs(writer, path);

      // NOTE: if you want to maximize search performance,
      // you can optionally call forceMerge here. This can be
      // a terribly costly operation, so generally it's only
      // worth it when your index is relatively static (ie
      // you're done adding documents to it):
      //
      // writer.forceMerge(1);

      writer.close();
    } catch (IOException e) {
      logger.error("Indexing failed!", e);
    }
  }

  /**
   * Indexes the given file using the given writer, or if a directory is given, recurses over files
   * and directories found under the given directory.
   *
   * @param writer Writer to the index where the given file/dir info will be stored
   * @param path The file to index, or the directory to recurse into to find files to index
   * @throws IOException If there is a low-level I/O error
   */
  static void indexDocs(final IndexWriter writer, Path path) throws IOException {
    if (Files.isDirectory(path)) {
      Files.walkFileTree(
          path,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              try {
                indexDoc(writer, file);
              } catch (IOException ignore) {
                // don't index files that can't be read.
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } else {
      indexDoc(writer, path);
    }
  }

  /** Indexes a single document */
  static void indexDoc(IndexWriter writer, Path file) throws IOException {
    InputStream stream = Files.newInputStream(file);
    BufferedReader in = new BufferedReader(new InputStreamReader(stream));
    String line = null;
    while ((line = in.readLine()) != null) {
      Document doc = new Document();
      doc.add(
          new TextField(Const.FIELD_KEY, line.substring(0, line.indexOf(" ")), Field.Store.YES));
      doc.add(new TextField(Const.FIELD_VALUE, line.substring(line.indexOf(" ")), Field.Store.NO));
      writer.updateDocument(new Term(Const.FIELD_KEY, doc.get(Const.FIELD_KEY)), doc);
    }
  }
}
