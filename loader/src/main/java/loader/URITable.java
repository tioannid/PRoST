package loader;

import org.apache.spark.sql.Row;

import java.util.List;

public class URITable {
    private String full;
    private String left;
    private String right;

    public String getFull() {
        return full;
    }

    public String getLeft() {
        return left;
    }

    public String getRight() {
        return right;
    }



    public URITable(Row item) {
        full = item.getString(0);
        List<String> l = PrefixEncoder.splitURI(item);
        left = PrefixEncoder.reconURI(l, 1, 6 , "https:/");
        right = PrefixEncoder.reconURI(l, 7, -1, "");



    }
}
