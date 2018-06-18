package service;

import java.nio.file.Path;

public interface AsyncService {
  public void asyncIndexFile(Path path);
}
