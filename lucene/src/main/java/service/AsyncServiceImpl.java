package service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
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
  public void asyncIndexFile(Path docDir, String dataset, String branch) {
    try {
      // Do not create index folder/files for empty documents
      if (Files.walk(docDir)
              .filter(Files::isRegularFile)
              .filter(
                  f -> {
                    try {
                      return Files.size(f) > 0;
                    } catch (IOException e) {
                      return false;
                    }
                  })
              .count()
          <= 0) {
        logger.warn("No document or documents are empty. Indexing is skipped");
        Files.walk(docDir)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
        return;
      }

      // Create index folders and index files
      Directory indexDir = FSDirectory.open(Paths.get(Const.INDEX_ROOT + dataset + "/" + branch));
      IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
      iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);

      IndexWriter writer = new IndexWriter(indexDir, iwc);

      Files.walk(docDir)
          .filter(Files::isRegularFile)
          .forEach(
              p -> {
                try {
                  indexDoc(p, writer);
                } catch (Exception e) {
                  logger.error("Indexing failed for file ", p.toString(), e);
                }
              });

      writer.close();
      Files.walk(docDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    } catch (IOException e) {
      logger.error("Indexing failed!", e);
    }
  }

  /** Indexes a single document */
  private void indexDoc(Path file, IndexWriter writer) throws Exception {
    InputStream stream = Files.newInputStream(file);
    BufferedReader in = new BufferedReader(new InputStreamReader(stream));
    String line = null;
    while ((line = in.readLine()) != null) {
      int delim = line.indexOf(",");
      if (delim < 0) throw new Exception("File format is invalid.");
      Document doc = new Document();
      String key = line.substring(0, delim);
      // Not analyzed (case sensitive)
      doc.add(new StringField(Const.FIELD_KEY, key, Field.Store.YES));
      // Analyzed (case insensitive)
      doc.add(new TextField(Const.FIELD_VALUE, line.substring(delim + 1), Field.Store.NO));
      writer.updateDocument(new Term(Const.FIELD_KEY, key), doc);
    }
  }
}
