package loader;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.NumericLiteral;
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
import java.util.Set;
import org.apache.log4j.Logger;



public class MyParser extends NTriplesParser implements java.io.Serializable{

    private SimpleStatement st;
    private RDFStatement rdf;
    protected static final Logger logger = Logger.getLogger("PRoST");
    

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
    	/* 
    	 * object type:
    	 * 0:unknown
    	 * 1:literal
    	 * 2:resource
    	 */
        this.st = (SimpleStatement)super.createStatement(subj, pred, obj);
        int objectType=0;
        //if it is numeric, string or wkt keep only the value, else keep the whole string
        if(obj instanceof Literal ) {
        	objectType=1;
        	if(obj instanceof NumericLiteral) {
        		NumericLiteral l=(NumericLiteral)obj;
            	rdf = new RDFStatement(subj.toString(), pred.toString(), l.getLabel(), objectType);
        	}
        	else {
        		Literal l=(Literal)obj;
        		if(l.getDatatype().stringValue().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")) {
        			rdf = new RDFStatement(subj.toString(), pred.toString(), l.getLabel(), objectType);
        			//we should also keep the language!
        		}
        		else if(l.getDatatype().stringValue().equals("http://www.opengis.net/ont/geosparql#wktLiteral")) {
        			//wkt
        			rdf = new RDFStatement(subj.toString(), pred.toString(), l.getLabel(), objectType);
        		}
        		else {
        			//keep the whole literal including datatype
        			
        			rdf = new RDFStatement(subj.toString(), pred.toString(), obj.toString(), objectType);
        		}
        	}
        	
        }
        else {
        	objectType=2;
        	rdf = new RDFStatement(subj.toString(), pred.toString(), obj.toString(), objectType);
        }
        
        return null;
    }

}
