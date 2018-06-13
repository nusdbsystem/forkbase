package service;

import java.io.IOException;

import pojo.SearchResponse;

public interface IndexService {
	public void indexFile(String dir) throws IOException;
	public SearchResponse searchFile(String queryString) throws Exception;
}
