package loader;

public class RDFStatement {
    private String o;
    private String s;
    private String p;
    private int oType;



    public String getS() {
        return s;
    }

    public String getP() {
        return p;
    }
    
    public String getO() {
        return o;
    }
    
    public int getOType(){
    	return oType;
    }
    

    public RDFStatement(String subj, String pred, String obj, int objType) {
        this.o = obj;
        this.p = pred;
        this.s = subj;
        this.oType=objType;
    }
}
