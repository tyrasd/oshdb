package org.heigit.bigspatialdata.oshdb.tool.importer.notesparser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.notes.NoteAction;
import org.heigit.bigspatialdata.oshdb.notes.NoteComment;
import org.heigit.bigspatialdata.oshdb.notes.OSMNote;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.ISODateTimeParser;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class NotesHandler extends DefaultHandler {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(NotesHandler.class);

  OSMNote currNote;
  List<NoteComment> currComments;
  NoteComment currComment;
  boolean comment = false;

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes)
      throws SAXException {
    if (qName.equalsIgnoreCase("note")) {

      int id = Integer.valueOf(attributes.getValue("id"));
      double lat = Double.valueOf(attributes.getValue("lat"));
      double lon = Double.valueOf(attributes.getValue("lon"));
      OSHDBTimestamp created = null;
      OSHDBTimestamp closed = null;
      try {
        created = new OSHDBTimestamp(
            Date.from(
                ISODateTimeParser.parseISODateTime(attributes.getValue("created_at")).toInstant())
        );
        closed = new OSHDBTimestamp(
            Date.from(
                ISODateTimeParser.parseISODateTime(attributes.getValue("closed_at")).toInstant())
        );
      } catch (Exception ex) {
        LOG.debug("Could not parse date", ex);
      }
      this.currNote = new OSMNote(
          id,
          (long) (lat * OSHDB.GEOM_PRECISION_TO_LONG),
          (long) (lon * OSHDB.GEOM_PRECISION_TO_LONG),
          created,
          closed,
          null);
      this.currComments = new ArrayList<>();

    } else if (qName.equalsIgnoreCase("comment")) {
      NoteAction action = NoteAction.fromString(attributes.getValue("action"));
      OSHDBTimestamp ts = null;
      try {
        ts = new OSHDBTimestamp(
            Date.from(
                ISODateTimeParser.parseISODateTime(attributes.getValue("timestamp")).toInstant())
        );
      } catch (Exception ex) {
        LOG.debug("Could not parse date", ex);
      }
      String uidString = attributes.getValue("uid");
      Integer uid = null;
      if (uidString != null) {
        uid = Integer.valueOf(uidString);
      }
      this.currComment = new NoteComment(action, ts, uid, null);
      this.comment = true;
    }
  }

  @Override
  public void characters(char ch[], int start, int length) throws SAXException {
    if (this.comment) {
      char[] copyOfRange = Arrays.copyOfRange(ch, start, start + length);
      this.currComment.setComment(String.valueOf(copyOfRange));
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    if (qName.equalsIgnoreCase("note")) {
      this.currNote.setComments(this.currComments);
      //store note here
      System.out.println("Read note :" + this.currNote);
    } else if (qName.equalsIgnoreCase("comment")) {
      this.currComments.add(this.currComment);
      this.comment = false;
    }
  }

}
