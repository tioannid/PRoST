package loader;

public class DE9IM implements java.io.Serializable{
	private String 	id1;
	private String	id2;
	private boolean Contains;
	private boolean CoveredBy;
	private boolean Covers;
	private boolean Crosses;
	private boolean Equals;
	private boolean Intersects;
	private boolean Overlaps;
	private boolean Touches;
	private boolean Within;
	public String getId1() {
		return id1;
	}
	public void setId1(String id1) {
		this.id1 = id1;
	}
	public String getId2() {
		return id2;
	}
	public void setId2(String id2) {
		this.id2 = id2;
	}
	public boolean isContains() {
		return Contains;
	}
	public void setContains(boolean contains) {
		Contains = contains;
	}
	public boolean isCoveredBy() {
		return CoveredBy;
	}
	public void setCoveredBy(boolean coveredBy) {
		CoveredBy = coveredBy;
	}
	public boolean isCovers() {
		return Covers;
	}
	public void setCovers(boolean covers) {
		Covers = covers;
	}
	public boolean isCrosses() {
		return Crosses;
	}
	public void setCrosses(boolean crosses) {
		Crosses = crosses;
	}
	public boolean isEquals() {
		return Equals;
	}
	public void setEquals(boolean equals) {
		Equals = equals;
	}
	public boolean isIntersects() {
		return Intersects;
	}
	public void setIntersects(boolean intersects) {
		Intersects = intersects;
	}
	public boolean isOverlaps() {
		return Overlaps;
	}
	public void setOverlaps(boolean overlaps) {
		Overlaps = overlaps;
	}
	public boolean isTouches() {
		return Touches;
	}
	public void setTouches(boolean touches) {
		Touches = touches;
	}
	public boolean isWithin() {
		return Within;
	}
	public void setWithin(boolean within) {
		Within = within;
	}
	public DE9IM(String id1, String id2, boolean contains, boolean coveredBy, boolean covers, boolean crosses, boolean equals,
			boolean intersects, boolean overlaps, boolean touches, boolean within) {
		super();
		this.id1 = id1;
		this.id2 = id2;
		Contains = contains;
		CoveredBy = coveredBy;
		Covers = covers;
		Crosses = crosses;
		Equals = equals;
		Intersects = intersects;
		Overlaps = overlaps;
		Touches = touches;
		Within = within;
	}
}
