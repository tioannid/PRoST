package loader;

public enum DE9IMEnum {
	id1				(0),
	id2				(1),
	Contains 		(2),
	CoveredBy 		(3),
	Covers			(4),
	Crosses			(5),
	Equals			(6),
	Intersects		(7),
	Overlaps		(8),
	Touches			(9),
	Within			(10);
	
    private int value ;

    DE9IMEnum ( int value )
    {
         this . value = value ;
    }

    public int position ( )
    {
		return value;
    }
}
