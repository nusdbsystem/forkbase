package controller;

import java.io.IOException;

import org.apache.lucene.queryparser.classic.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;

import pojo.Const;
import pojo.IndexResponse;
import pojo.RequestParam;
import pojo.SearchResponse;
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
  public String indexFiles(@RequestBody RequestParam param) {
    Gson gson = new Gson();
    try {
      service.indexFile(param.getParam());
      return gson.toJson(new IndexResponse(Const.SUCCESS, ""));
    } catch (IOException e) {
      return gson.toJson(new IndexResponse(Const.FAIL, e.getMessage()));
    }
  }

  /**
   * Search indexed data
   *
   * @param param The Lucene query string
   * @return JSON string with fields status - execution status docs - list of matched results
   */
  @RequestMapping(value = "/search", method = RequestMethod.POST)
  @ResponseBody
  public String searchFiles(@RequestBody RequestParam param) {
    Gson gson = new Gson();
    try {
      SearchResponse result = service.searchFile(param.getParam());
      return gson.toJson(result);
    } catch (IOException e) {
      return gson.toJson(new SearchResponse(Const.FAIL, e.getMessage()));
    } catch (ParseException e) {
      return gson.toJson(new SearchResponse(Const.FAIL, e.getMessage()));
    } catch (Exception e) {
      return gson.toJson(new SearchResponse(Const.FAIL, e.getMessage()));
    }
  }
}
