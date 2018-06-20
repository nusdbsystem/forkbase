package controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;

import pojo.IndexRequest;
import pojo.SearchRequest;
import service.IndexService;

@Controller
public class IndexController {

  @Autowired IndexService service;

  /**
   * Index data in files by line
   *
   * @param param The directory of files to be indicated
   * @return JSON string with execution status field
   */
  @RequestMapping(value = "/index", method = RequestMethod.POST)
  @ResponseBody
  public String indexFiles(@RequestBody IndexRequest param) {
    Gson gson = new Gson();
    return gson.toJson(service.indexFile(param));
  }

  /**
   * Search indexed data
   *
   * @param param The Lucene query string
   * @return JSON string with fields status - execution status docs - list of matched results
   */
  @RequestMapping(value = "/search", method = RequestMethod.POST)
  @ResponseBody
  public String searchFiles(@RequestBody SearchRequest param) {
    Gson gson = new Gson();
    return gson.toJson(service.searchFile(param));
  }
}
