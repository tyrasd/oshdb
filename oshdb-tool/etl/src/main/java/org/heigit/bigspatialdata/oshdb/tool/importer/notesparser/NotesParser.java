package org.heigit.bigspatialdata.oshdb.tool.importer.notesparser;

import java.io.File;
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;

public class NotesParser {

  public static void main(String[] args)
      throws ParserConfigurationException, SAXException, IOException {

    File inputFile = new File("planet-notes-190402.osn");
    SAXParserFactory factory = SAXParserFactory.newInstance();
    SAXParser saxParser = factory.newSAXParser();
    NotesHandler notesHandler = new NotesHandler();
    saxParser.parse(inputFile, notesHandler);

  }

}
