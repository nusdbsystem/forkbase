package service;

import pojo.IndexRequest;
import pojo.IndexResponse;
import pojo.SearchRequest;
import pojo.SearchResponse;

public interface IndexService {
  public IndexResponse indexFile(IndexRequest param);

  public SearchResponse searchFile(SearchRequest param);
}
