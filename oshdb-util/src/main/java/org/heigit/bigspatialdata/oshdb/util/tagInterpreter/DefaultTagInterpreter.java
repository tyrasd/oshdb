package org.heigit.bigspatialdata.oshdb.util.tagInterpreter;

import org.heigit.bigspatialdata.oshdb.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;

/**
 * Default TagInterpreter
 */
public class DefaultTagInterpreter extends BaseTagInterpreter {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTagInterpreter.class);

  private int typeKey = -1;
  private int typeMultipolygonValue = -1;
  private int typeBoundaryValue = -1;
  private int typeRouteValue = -1;

  private final static String defaultAreaTagsDefinitionFile = "json/polygon-features.json";
  private final static String defaultUninterestingTagsDefinitionFile = "json/uninterestingTags.json";

  /**
   *
   * @param conn
   * @throws IOException
   * @throws ParseException
   */
  public DefaultTagInterpreter(Connection conn)
      throws IOException, ParseException, OSHDBKeytablesNotFoundException {
    this(
        new TagTranslator(conn),
        defaultAreaTagsDefinitionFile,
        defaultUninterestingTagsDefinitionFile
    );
  }

  /**
   *
   * @param tagTranslator
   * @throws IOException
   * @throws ParseException
   */
  public DefaultTagInterpreter(TagTranslator tagTranslator) throws IOException, ParseException {
    this(
        tagTranslator,
        defaultAreaTagsDefinitionFile,
        defaultUninterestingTagsDefinitionFile
    );
  }

  /**
   *
   * @param tagTranslator
   * @param areaTagsDefinitionFile
   * @param uninterestingTagsDefinitionFile
   * @throws IOException
   * @throws ParseException
   */
  public DefaultTagInterpreter(
      TagTranslator tagTranslator,
      String areaTagsDefinitionFile, String uninterestingTagsDefinitionFile
  ) throws IOException, ParseException {
    super(-1,-1, null, null, null, -1, -1, -1); // initialize with dummy parameters for now
    // construct list of area tags for ways
    Map<Integer, Set<Integer>> wayAreaTags = new HashMap<>();

    JSONParser parser = new JSONParser();
    JSONArray tagList = (JSONArray)parser.parse(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(areaTagsDefinitionFile)));
    // todo: check json schema for validity

    //noinspection unchecked
    for (JSONObject tag : (Iterable<JSONObject>)tagList) {
      String key = (String)tag.get("key");
      switch ((String)tag.get("polygon")) {
        case "all":
          Set<Integer> valueIds = new InvertedHashSet<>();
          int keyId = tagTranslator.getOSHDBTagKeyOf(key).getIntKey();
          valueIds.add(tagTranslator.getOSHDBTagOf(key, "no").getIntValue());
          wayAreaTags.put(keyId, valueIds);
          break;
        case "whitelist":
          valueIds = new HashSet<>();
          keyId = tagTranslator.getOSHDBTagKeyOf(key).getIntKey();
          JSONArray values = (JSONArray) tag.get("values");
          //noinspection unchecked
          for (String value : (Iterable<String>) values) {
            OSMTag keyValue = new OSMTag(key, value);
            valueIds.add(tagTranslator.getOSHDBTagOf(keyValue).getIntValue());
          }
          valueIds.add(tagTranslator.getOSHDBTagOf(key, "no").getIntValue());
          wayAreaTags.put(keyId, valueIds);
          break;
        case "blacklist":
          valueIds = new InvertedHashSet<>();
          keyId = tagTranslator.getOSHDBTagKeyOf(key).getIntKey();
          values = (JSONArray) tag.get("values");
          //noinspection unchecked
          for (String value : (Iterable<String>) values) {
            OSMTag keyValue = new OSMTag(key, value);
            valueIds.add(tagTranslator.getOSHDBTagOf(keyValue).getIntValue());
          }
          wayAreaTags.put(keyId, valueIds);
          break;
        default:
          throw new ParseException(-13);
      }
    }

    // hardcoded type=multipolygon for relations
    this.typeKey = tagTranslator.getOSHDBTagKeyOf("type").getIntKey();
    this.typeMultipolygonValue = tagTranslator.getOSHDBTagOf("type", "multipolygon").getIntValue();
    this.typeBoundaryValue = tagTranslator.getOSHDBTagOf("type", "boundary").getIntValue();
    this.typeRouteValue = tagTranslator.getOSHDBTagOf("type", "route").getIntValue();

    // we still need to also store relation area tags for isOldStyleMultipolygon() functionality!
    Map<Integer, Set<Integer>> relAreaTags = new TreeMap<>();
    Set<Integer> relAreaTagValues = new TreeSet<>();
    relAreaTagValues.add(this.typeMultipolygonValue);
    relAreaTagValues.add(this.typeBoundaryValue);
    relAreaTags.put(this.typeKey, relAreaTagValues);

    // list of uninteresting tags
    Set<Integer> uninterestingTagKeys = new HashSet<>();
    JSONArray uninterestingTagsList = (JSONArray)parser.parse(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(uninterestingTagsDefinitionFile)));
    // todo: check json schema for validity
    //noinspection unchecked
    for (String tagKey : (Iterable<String>)uninterestingTagsList) {
      uninterestingTagKeys.add(tagTranslator.getOSHDBTagKeyOf(tagKey).getIntKey());
    }

    this.wayAreaTags = wayAreaTags;
    this.relationAreaTags = relAreaTags;
    this.uninterestingTagKeys = uninterestingTagKeys;

    this.areaNoTagKeyId = tagTranslator.getOSHDBTagOf("area", "no").getIntKey();
    this.areaNoTagValueId = tagTranslator.getOSHDBTagOf("area", "no").getIntValue();

    this.outerRoleId = tagTranslator.getOSHDBRoleOf("outer").getIntRole();
    this.innerRoleId = tagTranslator.getOSHDBRoleOf("inner").getIntRole();
    this.emptyRoleId = tagTranslator.getOSHDBRoleOf("").getIntRole();
  }

  @Override
  public boolean isArea(OSMEntity entity) {
    if (entity instanceof OSMRelation) {
      return evaluateRelationForArea((OSMRelation)entity);
    } else {
      return super.isArea(entity);
    }
  }

  @Override
  public boolean isLine(OSMEntity entity) {
    if (entity instanceof OSMRelation) {
      return evaluateRelationForLine((OSMRelation)entity);
    } else {
      return super.isLine(entity);
    }
  }

  // checks if the relation has the tag "type=multipolygon"
  private boolean evaluateRelationForArea(OSMRelation entity) {
    for(OSHDBTag tag : entity.getTags()){
    // skip area=no check, since that doesn't make much sense for multipolygon relations (does it??)
    // the following is slightly faster than running `return entity.hasTagValue(k1,v1) || entity.hasTagValue(k2,v2);`
      if (tag.getIntKey() == typeKey)
        return tag.getIntValue() == typeMultipolygonValue || tag.getIntValue() == typeBoundaryValue;
      else if (tag.getIntKey() > typeKey)
        return false;
    }
    return false;
  }

  // checks if the relation has the tag "type=route"
  private boolean evaluateRelationForLine(OSMRelation entity) {
    return entity.hasTagValue(typeKey, typeRouteValue);
  }
}
