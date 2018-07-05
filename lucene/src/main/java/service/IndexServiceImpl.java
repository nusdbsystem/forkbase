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
import pojo.IndexResponse;
import pojo.SearchRequest;
import pojo.SearchResult;
import pojo.SearchResponse;

@Service
public class IndexServiceImpl implements IndexService {

  @Autowired AsyncService service;

  @Override
  public IndexResponse indexFile(IndexRequest req) {
    try {
      // Validate path
      final Path docDir = Paths.get(req.getDir());
      if (!Files.isReadable(docDir)) {
        throw new Exception(req.getDir() + " does not exist or is not readable");
      }

      // Index file asynchronously
      service.asyncIndexFile(docDir, req.getDataset(), req.getBranch());
      return new IndexResponse(Const.SUCCESS, "");
    } catch (IOException e) {
      return new IndexResponse(Const.IO_EXCEPTION, "[Lucene] " + e.getMessage());
    } catch (Exception e) {
      return new IndexResponse(Const.INVALID_PATH, "[Lucene] " + e.getMessage());
    }
  }

  @Override
  public SearchResponse searchFile(SearchRequest req) {
    try {
      Path indexPath = Paths.get(Const.INDEX_ROOT + req.getDataset() + "/" + req.getBranch());

      // Dataset or branch not indexed, return empty result
      if (!Files.isReadable(indexPath)) {
        return new SearchResponse(Const.SUCCESS, "", new ArrayList<>());
      }

      // Dataset and branch is indexed, proceed search
      IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath));
      IndexSearcher searcher = new IndexSearcher(reader);
      QueryParser parser = new QueryParser(Const.FIELD_VALUE, new StandardAnalyzer());

      // Check query string
      String queryString = req.getQuery();
      if (queryString == null || queryString.length() == -1) {
        throw new Exception("Invalid query: " + req.getQuery());
      }
      queryString = queryString.trim();
      if (queryString.length() == 0) {
        throw new Exception("Invalid query: " + req.getQuery());
      }

      // Perform search
      Query query = parser.parse(queryString);
      TopDocs results = searcher.search(query, reader.numDocs());
      ScoreDoc[] hits = results.scoreDocs;

      // Construct response
      List<SearchResult> keys = new ArrayList<>();
      for (int i = 0; i < hits.length; i++) {
        Document doc = searcher.doc(hits[i].doc);
        SearchResult result = new SearchResult(doc.get(Const.FIELD_KEY));
        keys.add(result);
      }
      SearchResponse res = new SearchResponse(Const.SUCCESS, "", keys);

      reader.close();
      return res;
    } catch (IOException e) {
      return new SearchResponse(Const.IO_EXCEPTION, "[Lucene] " + e.getMessage());
    } catch (Exception e) {
      return new SearchResponse(Const.PARSE_ERROR, "[Lucene] " + e.getMessage());
    }
  }
}
