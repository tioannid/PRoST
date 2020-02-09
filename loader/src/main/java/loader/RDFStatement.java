package loader;

public class RDFStatement {
    private String o;
    private String s;
    private String p;



    public String getS() {
        return s;
    }

    public String getP() {
        return p;
    }
    public String getO() {
        return o;
    }

    public RDFStatement(String subj, String pred, String obj) {
        this.o = obj;
        this.p = pred;
        this.s = subj;

    }
}
