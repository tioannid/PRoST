package loader;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleStatement;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.ntriples.NTriplesParser;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

import java.io.*;
import java.nio.charset.Charset;


public class MyParser extends NTriplesParser implements java.io.Serializable{

    private SimpleStatement st;
    private RDFStatement rdf;

    public SimpleStatement  getStatement() {
        return this.st;
    }

    public RDFStatement getRdf() {
        return rdf;
    }

    RDFStatement parseLine(String line) throws IOException{
        try {
            this.parse((InputStream)(new ByteArrayInputStream(line.getBytes(Charset.forName("UTF-8")))), "HELLO");

        } catch(IOException e) {
            ;
        }
        return this.rdf;
    }

    @Override
    protected Statement createStatement(Resource subj, IRI pred, Value obj) throws RDFParseException {

        this.st = (SimpleStatement)super.createStatement(subj, pred, obj);
        rdf = new RDFStatement(subj.toString(), pred.toString(), obj.toString());
        return null;
    }

}
