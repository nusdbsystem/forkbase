package service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import pojo.Const;
import pojo.IndexRequest;
import pojo.SearchRequest;
import pojo.SearchResult;
import pojo.SearchResponse;

@Service
public class IndexServiceImpl implements IndexService {

  @Autowired AsyncService service;

  @Override
  public void indexFile(IndexRequest req) throws IOException {
    final Path docDir = Paths.get(req.getDir());
    if (!Files.isReadable(docDir)) {
      throw new IOException("Document directory does not exist or is not readable");
    }

    service.asyncIndexFile(docDir, req.getDataset(), req.getBranch());
  }

  @Override
  public SearchResponse searchFile(SearchRequest req) throws Exception {
    String indexDir = Const.INDEX_ROOT + req.getDataset() + "/" + req.getBranch();
    IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDir)));
    IndexSearcher searcher = new IndexSearcher(reader);
    QueryParser parser = new QueryParser(Const.FIELD_VALUE, new StandardAnalyzer());

    String queryString = req.getQuery();
    if (queryString == null || queryString.length() == -1) {
      throw new Exception("Invalid query");
    }
    queryString = queryString.trim();
    if (queryString.length() == 0) {
      throw new Exception("Invalid query");
    }

    Query query = parser.parse(queryString);

    TopDocs results = searcher.search(query, reader.numDocs());
    ScoreDoc[] hits = results.scoreDocs;

    List<SearchResult> keys = new ArrayList<>();
    for (int i = 0; i < hits.length; i++) {
      Document doc = searcher.doc(hits[i].doc);
      SearchResult result = new SearchResult(doc.get(Const.FIELD_KEY));
      keys.add(result);
    }
    SearchResponse res = new SearchResponse(Const.SUCCESS, "", keys);

    reader.close();
    return res;
  }
}
