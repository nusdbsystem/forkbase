package pojo;

import java.util.List;

public class SearchResponse {
	int status;
	String msg;
	List<SearchResult> docs;
	
	public SearchResponse(int status, String msg) {
		this.status = status;
		this.msg = msg;
	}
	public SearchResponse(int status, String msg, List<SearchResult> docs) {
		this(status, msg);
		this.docs = docs;
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
	public List<SearchResult> getDocs() {
		return docs;
	}
	public void setDocs(List<SearchResult> docs) {
		this.docs = docs;
	}
}
