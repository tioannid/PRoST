package loader;

public class TableInfo {
	
	private int countAll;
	private int distinctSubjects;
	private int distinctObjects;
	
	
	
	
	public TableInfo(int countAll, int distinctSubjects, int distinctObjects) {
		super();
		this.countAll = countAll;
		this.distinctSubjects = distinctSubjects;
		this.distinctObjects = distinctObjects;
	}
	public int getCountAll() {
		return countAll;
	}
	public void setCountAll(int countAll) {
		this.countAll = countAll;
	}
	public int getDistinctSubjects() {
		return distinctSubjects;
	}
	public void setDistinctSubjects(int distinctSubjects) {
		this.distinctSubjects = distinctSubjects;
	}
	public int getDistinctObjects() {
		return distinctObjects;
	}
	public void setDistinctObjects(int distinctObjects) {
		this.distinctObjects = distinctObjects;
	}
	
	

}
