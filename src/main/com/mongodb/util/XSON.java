package com.mongodb.util;

import java.util.Map;
import java.util.HashMap;
import java.util.Stack;
import java.util.Date;
import java.util.regex.Pattern;
import java.io.File;
import java.io.FileOutputStream;

import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParserFactory;
import javax.xml.parsers.SAXParser;

import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBList;
import com.mongodb.ObjectId;
import com.mongodb.ByteEncoder;


/**
 *  XSON (XML Serialized 'ocument Notation) - XML
 * 
 */
public class XSON extends DefaultHandler {

    Map<String, Class> _handlerMap = new HashMap<String, Class>() {
        {
            put("twonk", TwonkHandler.class);
            put("string", StringHandler.class);
            put("boolean", BooleanHandler.class);
            put("number", NumberHandler.class);
            put("date", DateHandler.class);
            put("code", CodeHandler.class);
            put("doc", DocHandler.class);
            put("oid", OIDHandler.class);
            put("array", ArrayHandler.class);
            put("int", IntHandler.class);
            put("regex", RegexHandler.class);
            put("null", StringHandler.class);
        }
    };

    Stack<Handler> _handlerStack = new Stack<Handler>();
    DBObject _doc = new BasicDBObject();
    DBObject _currentDoc = _doc;
    Handler _currentHandler = null;

    public static void main (String args[]) throws Exception
    {
        if (args.length != 3) {
            System.out.println("usage : ");
            System.out.println("  to convert xson to bson : --xtob xson_input_file bson_output_file");
            System.out.println("  to convert bson to xson : --btox bson_input_file xson_output_file");
            return;
        }

        if (args[0].equals("--xtob")) {

            SAXParserFactory factory = SAXParserFactory.newInstance();
            XSON xson = new XSON();

            SAXParser saxParser = factory.newSAXParser();
            saxParser.parse( new File(args[1]), xson);

            ByteEncoder enc = ByteEncoder.get();
            enc.putObject((DBObject) xson._doc.get("$root"));

            FileOutputStream fos = new FileOutputStream(new File(args[2]));

            fos.write(enc.getBytes());
            fos.close();
        }
        else if (args[0].equals("--btox")) {
            System.out.println("Unimplemented");
        }
    }

    /*
     *  ------------   SAX  stuff    -------------------------
     */

    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (_currentHandler == null) {
            _currentHandler = getHandler(qName);
        }

        _currentHandler.startElement(uri, localName, qName, attributes);
    }
    
    public void endElement(String uri, String localName, String qName) throws SAXException {
        _currentHandler.endElement(uri, localName, qName);
    }

    public void characters(char[] ch, int start, int length) throws SAXException {
        _currentHandler.characters(ch, start, length);
    }

    /*
     *    -------------  handler stuff ----------------------------
     */

    public Handler getHandler(String t) {

        Class c = _handlerMap.get(t);

        if (c == null) {
            System.err.println("WARNING : no handler for " + t);
            return new Handler();
        }

        try {
            return (Handler) c.getConstructors()[0].newInstance(this);
        }
        catch(Exception e) {
            throw new Error(e);
        }
    }

    public class Handler extends DefaultHandler {

        String _name = null;
        String _value = null;

        public void characters(char[] ch, int start, int length) throws SAXException {
            _value = new String(ch, start, length);
        }

        public void startElement(String uri, String localName, String qName, Attributes att) throws SAXException {
            _name = att.getValue("name");
        }

        public void endElement(String uri, String localName, String qName) throws SAXException {
            if (_handlerStack.size() > 0) {
                _currentHandler = _handlerStack.pop();
            }
        }
    }

    public class OIDHandler extends Handler {
        public void endElement(String uri, String localName, String qName) throws SAXException {
            _currentDoc.put(_name,  new ObjectId(_value.toUpperCase())); 
            super.endElement(uri, localName, qName);
        }
    }

    public class IntHandler extends Handler {
        public void endElement(String uri, String localName, String qName) throws SAXException {
            _currentDoc.put(_name, Integer.parseInt(_value));
            super.endElement(uri, localName, qName);
        }
    }

    public class BooleanHandler extends Handler {
        public void endElement(String uri, String localName, String qName) throws SAXException {
            _currentDoc.put(_name, Boolean.parseBoolean(_value));
            super.endElement(uri, localName, qName);
        }
    }

    public class DateHandler extends Handler {
        public void endElement(String uri, String localName, String qName) throws SAXException {
            _currentDoc.put(_name, new Date(Long.parseLong(_value)));
            super.endElement(uri, localName, qName);
        }
    }
    
    public class CodeHandler extends Handler {
        public void endElement(String uri, String localName, String qName) throws SAXException {
            _currentDoc.put(_name, _value);
            super.endElement(uri, localName, qName);
        }
    }

    public class NumberHandler extends Handler {
        public void endElement(String uri, String localName, String qName) throws SAXException {
            _currentDoc.put(_name, Double.parseDouble(_value));
            super.endElement(uri, localName, qName);
        }
    }

    public class RegexHandler extends Handler {

        String _next = null;
        Map<String, String> _data = new HashMap<String,String>();

        public void startElement(String uri, String localName, String qName, Attributes att) throws SAXException {

            if ("regex".equals(qName)) {
                super.startElement(uri, localName, qName, att);
                return;
            }
            
            _next = qName;
        }

        public void characters(char[] ch, int start, int length) throws SAXException {
            _data.put(_next, new String(ch, start, length));
        }


        public void endElement(String uri, String localName, String qName) throws SAXException {

            if ("regex".equals(qName)) {
                Pattern p = Pattern.compile(_data.get("pattern"));   // TODO - options
                _currentDoc.put(_name, p);
                super.endElement(uri, localName, qName);
            }
            else {
                _next = null;
            }
        }
    }

    public class ArrayHandler extends Handler {

        DBObject _oldDoc = null;
        DBObject _myDoc = new BasicDBList();

        public void startElement(String uri, String localName, String qName, Attributes att) throws SAXException {

            if ("array".equals(qName)) {
                _oldDoc = _currentDoc;
                _currentDoc = _myDoc;
                super.startElement(uri, localName, qName, att);
                return;
            }

            _handlerStack.push(this);
            _currentHandler = getHandler(qName);
            _currentHandler.startElement(uri, localName, qName, att);
        }

        public void endElement(String uri, String localName, String qName) throws SAXException {
            _currentDoc = _oldDoc;
            _currentDoc.put(_name, _myDoc);
            super.endElement(uri, localName, qName);
        }
    }

    public class StringHandler extends Handler {
        
        public void endElement(String uri, String localName, String qName) throws SAXException {
            _currentDoc.put(_name, _value);

            super.endElement(uri, localName, qName);
        }
    }

    public class DocHandler extends Handler {

        DBObject _oldDoc = null;
        BasicDBObject _myDoc = new BasicDBObject();
        
        public void startElement(String uri, String localName, String qName, Attributes att) throws SAXException {

            if ("doc".equals(qName)) {
                _oldDoc = _currentDoc;
                _currentDoc = _myDoc;
                super.startElement(uri, localName, qName, att);
                return;
            }

            _handlerStack.push(this);
            _currentHandler = getHandler(qName);
            _currentHandler.startElement(uri, localName, qName, att);
        }

        public void endElement(String uri, String localName, String qName) throws SAXException {
            _currentDoc = _oldDoc;
            _currentDoc.put(_name == null ? "$root" : _name, _myDoc);
            super.endElement(uri, localName, qName);
        }
    }

    public class XSONHandler extends Handler {

        public void startElement(String uri, String localName, String qName, Attributes att) throws SAXException {
            super.startElement(uri, localName, qName, att);

            if ("xson".equals(qName)) {
                return;
            }

            _handlerStack.push(this);
            _currentHandler = getHandler(qName);
            _currentHandler.startElement(uri, localName, qName, att);
        }

        public void endElement(String uri, String localName, String qName) throws SAXException {
            if (_handlerStack.size() > 0) {
                System.out.println(" - POP DOC - ");
                _currentHandler = _handlerStack.pop();
            }
        }
    }

    public class TwonkHandler extends Handler {

        public void startElement(String uri, String localName, String qName, Attributes att) throws SAXException {
            super.startElement(uri, localName, qName, att);
            if ("doc".equals(qName)) {
                _handlerStack.push(this);
                _currentHandler = getHandler(qName);
                _currentHandler.startElement(uri, localName, qName, att);
            }
        }
    }
}
