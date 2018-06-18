package pojo;

public class IndexResponse {
  int status;
  String msg;

  public IndexResponse(int status, String msg) {
    this.status = status;
    this.msg = msg;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }
}
