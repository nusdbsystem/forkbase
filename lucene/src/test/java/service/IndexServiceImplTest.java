package service;

import static org.junit.Assert.assertEquals;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import application.Application;
import pojo.Const;
import pojo.IndexRequest;
import pojo.IndexResponse;
import pojo.SearchRequest;
import pojo.SearchResponse;

@SpringBootTest(classes = Application.class)
@RunWith(SpringRunner.class)
public class IndexServiceImplTest {

  @Autowired private IndexService service;

  @Test
  public void testIndexAndSearch() {
    try {
      IndexRequest ir = new IndexRequest();
      ir.setDataset("TEMP_TEST_INDEX/dataset1");
      ir.setBranch("branch1");
      ir.setDir("target/temp/");

      SearchRequest sr = new SearchRequest();
      sr.setBranch("branch1");
      sr.setDataset("TEMP_TEST_INDEX/dataset1");
      sr.setQuery("Ronaldo");

      // Empty directory
      createFolder();
      IndexResponse response1 = service.indexFile(ir);
      assertEquals(Const.SUCCESS, response1.getStatus());
      Thread.sleep(3000);

      // Search empty index
      SearchResponse response2 = service.searchFile(sr);
      assertEquals(Const.SUCCESS, response2.getStatus());
      assertEquals(0, response2.getDocs().size());

      // File not exist
      createFolder();
      ir.setDir("target/temp/NotExist");
      IndexResponse response3 = service.indexFile(ir);
      assertEquals(Const.INVALID_PATH, response3.getStatus());
      Thread.sleep(3000);

      // Search empty index
      SearchResponse response4 = service.searchFile(sr);
      assertEquals(Const.SUCCESS, response4.getStatus());
      assertEquals(0, response4.getDocs().size());

      // Empty file
      createFolder();
      Files.createFile(Paths.get("target/temp/empty1"));
      Files.createFile(Paths.get("target/temp/empty2"));
      ir.setDir("target/temp/");
      IndexResponse response5 = service.indexFile(ir);
      assertEquals(Const.SUCCESS, response5.getStatus());
      Thread.sleep(3000);

      // Search empty file
      SearchResponse response6 = service.searchFile(sr);
      assertEquals(Const.SUCCESS, response6.getStatus());
      assertEquals(0, response6.getDocs().size());

      // Index target file by file path
      createFolder();
      createTargetFile("target/temp/target1", "Portugal,Cristiano Ronaldo scored a goal!");
      ir.setDir("target/temp/target1");
      IndexResponse response7 = service.indexFile(ir);
      assertEquals(Const.SUCCESS, response7.getStatus());
      Thread.sleep(3000);

      // Search target file
      SearchResponse response8 = service.searchFile(sr);
      assertEquals(Const.SUCCESS, response8.getStatus());
      assertEquals(1, response8.getDocs().size());

      sr.setQuery("Messi");
      SearchResponse response9 = service.searchFile(sr);
      assertEquals(Const.SUCCESS, response9.getStatus());
      assertEquals(0, response9.getDocs().size());

      // Index target file by folder
      createFolder();
      createTargetFile("target/temp/target2", "Argentina,Messi missed the penalty!");
      ir.setDir("target/temp/");
      IndexResponse response10 = service.indexFile(ir);
      assertEquals(Const.SUCCESS, response10.getStatus());
      Thread.sleep(3000);

      // Search target file
      sr.setQuery("Ronaldo");
      SearchResponse response11 = service.searchFile(sr);
      assertEquals(Const.SUCCESS, response11.getStatus());
      assertEquals(1, response11.getDocs().size());

      sr.setQuery("Messi");
      SearchResponse response12 = service.searchFile(sr);
      assertEquals(Const.SUCCESS, response12.getStatus());
      assertEquals(1, response12.getDocs().size());

      // Not indexed branch
      sr.setBranch("branch2");
      SearchResponse response13 = service.searchFile(sr);
      assertEquals(Const.SUCCESS, response13.getStatus());
      assertEquals(0, response13.getDocs().size());

    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void createFolder() {
    try {
      if (!Files.exists(Paths.get("target/temp/"))) {
        Files.createDirectory(Paths.get("target/temp/"));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void createTargetFile(String dir, String content) throws IOException {
    Path path = Paths.get(dir);
    Files.createFile(path);
    String body = "key,value\n" + content;
    Files.write(path, body.getBytes());
  }

  @After
  public void destroyFolder() {
    try {
      Files.walk(Paths.get("index/TEMP_TEST_INDEX/"))
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
