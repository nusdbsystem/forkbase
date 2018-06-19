package service;

import java.io.IOException;
import pojo.IndexRequest;
import pojo.SearchRequest;
import pojo.SearchResponse;

public interface IndexService {
  public void indexFile(IndexRequest param) throws IOException;

  public SearchResponse searchFile(SearchRequest param) throws Exception;
}
