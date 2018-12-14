package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygonal;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.generic.*;
import org.heigit.bigspatialdata.oshdb.api.generic.function.*;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer.Grouping;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.Neighbourhood.GEOMETRY_OPTIONS;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampList;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.jetbrains.annotations.Contract;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.json.simple.parser.ParseException;

/**
 * A MapReducer with built-in aggregation by an arbitrary index
 *
 * This class provides similar functionality as a MapReducer, with the difference that here the `reduce` does
 * automatic aggregation of results by the values returned by an arbitrary indexing function.
 *
 * All results for which the set `indexer` returns the same value are aggregated into separate "bins".
 * This can be used to aggregate results by timestamp, geographic region, user id, osm tag, etc.
 *
 * Internally, this wraps around an existing MapReducer object, which still continues to be responsible for all actual calculations.
 *
 * @param <X> the type that is returned by the currently set of mapper function. the next added mapper function will be called with a parameter of this type as input
 * @param <U> the type of the index values returned by the `mapper function`, used to group results
 */
public class MapAggregator<U extends Comparable<U>, X> implements
    Mappable<X>, MapReducerSettings<MapAggregator<U,X>>, MapReducerAggregations<X>
{
  private MapReducer<Pair<U, X>> _mapReducer;
  private final List<Collection<?>> _zerofill;

  /**
   * basic constructor
   * @param mapReducer mapReducer object which will be doing all actual calculations
   * @param indexer function that returns the index value into which to aggregate the respective result
   * @param zerofill
   */
  MapAggregator(
      MapReducer<X> mapReducer,
      SerializableFunction<X, U> indexer,
      Collection<U> zerofill
  ) {
    this._mapReducer = mapReducer.map(data -> new MutablePair<U, X>(
        indexer.apply(data),
        data
    ));
    this._zerofill = new ArrayList<>(1);
    this._zerofill.add(zerofill);
  }

  // "copy/transform" constructor
  private MapAggregator(MapAggregator<U, ?> obj, MapReducer<Pair<U, X>> mapReducer) {
    this._mapReducer = mapReducer;
    this._zerofill = new ArrayList<>(obj._zerofill);
  }

  /**
   * Creates new mapAggregator object for a specific mapReducer that already contains an aggregation index.
   *
   * Used internally for returning type safe copies of the current mapAggregator object after map/flatMap/filter operations.
   *
   * @param mapReducer
   * @param <R>
   * @return
   */
  @Contract(pure = true)
  private <R> MapAggregator<U, R> copyTransform(MapReducer<Pair<U, R>> mapReducer) {
    return new MapAggregator<>(this, mapReducer);
  }

  @Contract(pure = true)
  private <V extends Comparable<V>> MapAggregator<V, X> copyTransformKey(MapReducer<Pair<V, X>> mapReducer) {
    //noinspection unchecked – we do want to convert the mapAggregator to a different key type "V"
    return new MapAggregator<V, X>((MapAggregator<V, ?>) this, mapReducer);
  }

  /**
   * Gets the tagInterpreter
   *
   * @return tagInterpreter the tagInterpreter object
   */
  @SuppressWarnings("unused")
  @Contract(pure = true)
  public TagInterpreter getTagInterpreter() throws ParseException, SQLException, IOException {
    return this._mapReducer._getTagInterpreter();
  }

  /**
   * Gets the tagTranslator
   *
   * @return tagTranslator the TagTranslator object
   */
  @SuppressWarnings("unused")
  @Contract(pure = true)
  public TagTranslator getTagTranslator() {
    return this._mapReducer._getTagTranslator();
  }

  // -----------------------------------------------------------------------------------------------
  // MapAggregator specific methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Sets up aggregation by another custom index.
   *
   * @param indexer a callback function that returns an index object for each given data
   * @param zerofill a collection of values that are expected to be present in the result
   * @return a MapAggregatorByIndex object with the new index applied as well
   */
  @Contract(pure = true)
  public <V extends Comparable<V>> MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateBy(
      SerializableFunction<X, V> indexer,
      Collection<V> zerofill
  ) {
    MapAggregator<OSHDBCombinedIndex<U, V>, X> res = this
        .mapIndex((existingIndex, data) -> new OSHDBCombinedIndex<U, V>(
            existingIndex,
            indexer.apply(data)
        ));
    res._zerofill.add(zerofill);
    return res;
  }

  /**
   * Sets up aggregation by another custom index.
   *
   * @param indexer a callback function that returns an index object for each given data.
   * @return a MapAggregatorByIndex object with the new index applied as well
   */
  @Contract(pure = true)
  public <V extends Comparable<V>> MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateBy(
      SerializableFunction<X, V> indexer
  ) {
    return this.aggregateBy(indexer, Collections.emptyList());
  }

  /**
   * Sets up aggregation by a custom time index.
   *
   * The timestamps returned by the supplied indexing function are matched to the corresponding
   * time intervals
   *
   * @param indexer a callback function that returns a timestamp object for each given data.
   *                Note that if this function returns timestamps outside of the supplied
   *                timestamps() interval results may be undefined
   * @return a MapAggregatorByTimestampAndIndex object with the equivalent state (settings,
   *         filters, map function, etc.) of the current MapReducer object
   */
  @Contract(pure = true)
  public MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer
  ) {
    final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(this._mapReducer._tstamps.get());
    return this.aggregateBy(data -> {
      // match timestamps to the given timestamp list
      return timestamps.floor(indexer.apply(data));
    }, this._mapReducer.getZerofillTimestamps());
  }
  
  /**
   * Aggregates the results by sub-regions as well, in addition to the timestamps.
   *
   * Cannot be used together with the `groupByEntity()` setting enabled.
   *
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   * @throws UnsupportedOperationException if this is called when the `groupByEntity()` mode has been
   *         activated
   * @throws UnsupportedOperationException when called after any map or flatMap functions are set
   */
  @Contract(pure = true)
  public <V extends Comparable<V>, P extends Geometry & Polygonal>
  MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateByGeometry(Map<V, P> geometries) throws
      UnsupportedOperationException
  {
    if (this._mapReducer._grouping != Grouping.NONE) {
      throw new UnsupportedOperationException(
          "aggregateByGeometry() cannot be used together with the groupByEntity() functionality"
      );
    }

    GeometrySplitter<V> gs = new GeometrySplitter<>(geometries);
    if (this._mapReducer._mappers.size() > 1) {
      // todo: fix
      throw new UnsupportedOperationException(
          "please call aggregateByGeometry before setting any map or flatMap functions"
      );
    } else {
      MapAggregator<OSHDBCombinedIndex<U, V>, ? extends OSHDBMapReducible> ret;
      if (this._mapReducer._forClass.equals(OSMContribution.class)) {
        ret = this.flatMap(x -> gs.splitOSMContribution((OSMContribution) x))
            .aggregateBy(Pair::getKey, geometries.keySet()).map(Pair::getValue);
      } else if (this._mapReducer._forClass.equals(OSMEntitySnapshot.class)) {
        ret = this.flatMap(x -> gs.splitOSMEntitySnapshot((OSMEntitySnapshot) x))
            .aggregateBy(Pair::getKey, geometries.keySet()).map(Pair::getValue);
      } else {
        throw new UnsupportedOperationException(
            "aggregateByGeometry not implemented for objects of type: " + this._mapReducer._forClass.toString()
        );
      }
      //noinspection unchecked – no mapper functions have been applied, so the type is still X
      return (MapAggregator<OSHDBCombinedIndex<U, V>, X>) ret;
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Filtering methods
  // Just forwards everything to the wrapped MapReducer object
  // -----------------------------------------------------------------------------------------------

  /**
   * Set the area of interest to the given bounding box.
   * Only objects inside or clipped by this bbox will be passed on to the analysis' `mapper` function.
   *
   * @param bboxFilter the bounding box to query the data in
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> areaOfInterest(OSHDBBoundingBox bboxFilter) {
    return this.copyTransform(this._mapReducer.areaOfInterest(bboxFilter));
  }

  /**
   * Set the area of interest to the given polygon.
   * Only objects inside or clipped by this polygon will be passed on to the analysis' `mapper` function.
   *
   * @param polygonFilter the bounding box to query the data in
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public <P extends Geometry & Polygonal> MapAggregator<U, X> areaOfInterest(P polygonFilter) {
    return this.copyTransform(this._mapReducer.areaOfInterest(polygonFilter));
  }

  /**
   * Limits the analysis to the given osm entity types.
   *
   * @param typeFilter the set of osm types to filter (e.g. `EnumSet.of(OSMType.WAY)`)
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmType(EnumSet<OSMType> typeFilter) {
    return this.copyTransform(this._mapReducer.osmType(typeFilter));
  }

  /**
   * Adds a custom arbitrary filter that gets executed for each osm entity and determines if it should be considered for this analyis or not.
   *
   * @param f the filter function to call for each osm entity
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmEntityFilter(SerializablePredicate<OSMEntity> f) {
    return this.copyTransform(this._mapReducer.osmEntityFilter(f));
  }


  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * (with an arbitrary value), or this tag key and value.
   *
   * @param tag the tag (key, or key and value) to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmTag(OSMTagInterface tag) {
    return this.copyTransform(this._mapReducer.osmTag(tag));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key (with an arbitrary value).
   *
   * @param key the tag key to filter the osm entities for
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmTag(String key) {
    return this.copyTransform(this._mapReducer.osmTag(key));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key and value.
   *
   * @param key the tag key to filter the osm entities for
   * @param value the tag value to filter the osm entities for
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmTag(String key, String value) {
    return this.copyTransform(this._mapReducer.osmTag(key, value));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key and one of the
   * given values.
   *
   * @param key the tag key to filter the osm entities for
   * @param values an array of tag values to filter the osm entities for
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmTag(String key, Collection<String> values) {
    return this.copyTransform(this._mapReducer.osmTag(key, values));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have a tag with the given key and
   * whose value matches the given regular expression pattern.
   *
   * @param key the tag key to filter the osm entities for
   * @param valuePattern a regular expression which the tag value of the osm entity must match
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmTag(String key, Pattern valuePattern) {
    return this.copyTransform(this._mapReducer.osmTag(key, valuePattern));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have at least one of the supplied
   * tags (key=value pairs)
   *
   * @param keyValuePairs the tags (key/value pairs) to filter the osm entities for
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmTag(Collection<OSMTag> keyValuePairs) {
    return this.copyTransform(this._mapReducer.osmTag(keyValuePairs));
  }

  // -----------------------------------------------------------------------------------------------
  // "Quality of life" helper methods to use the map-reduce functionality more directly and easily for typical queries.
  // Available are: sum, count, average, weightedAverage and uniq.
  // Each one can be used to get results aggregated by timestamp, aggregated by a custom index and not aggregated totals.
  // -----------------------------------------------------------------------------------------------

  /**
   * Sums up the results.
   *
   * The current data values need to be numeric (castable to "Number" type), otherwise a runtime exception will be thrown.
   *
   * @return the sum of the current data
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   */
  @Contract(pure = true)
  public SortedMap<U, Number> sum() throws Exception {
    return this
        .makeNumeric()
        .reduce(
            () -> 0,
            NumberUtils::add
        );
  }

  /**
   * Sums up the results provided by a given `mapper` function.
   *
   * This is a shorthand for `.map(mapper).sum()`, with the difference that here the numerical return type of the `mapper` is ensured.
   *
   * @param mapper function that returns the numbers to sum up
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the summed up results of the `mapper` function
   */
  @Contract(pure = true)
  public <R extends Number> SortedMap<U, R> sum(SerializableFunction<X, R> mapper) throws Exception {
    return this
        .map(mapper)
        .reduce(
            () -> (R) (Integer) 0,
            NumberUtils::add
        );
  }

  /**
   * Counts the number of results.
   *
   * @return the total count of features or modifications, summed up over all timestamps
   */
  @Contract(pure = true)
  public SortedMap<U, Integer> count() throws Exception {
    return this.sum(ignored -> 1);
  }

  /**
   * Gets all unique values of the results.
   *
   * For example, this can be used together with the OSMContributionView to get the total amount of unique users editing specific feature types.
   *
   * @return the set of distinct values
   */
  @Contract(pure = true)
  public SortedMap<U, Set<X>> uniq() throws Exception {
    return this
        .reduce(
            HashSet::new,
            (acc, cur) -> { acc.add(cur); return acc; },
            (a,b) -> { HashSet<X> result = new HashSet<>(a); result.addAll(b); return result; }
        );
  }

  /**
   * Gets all unique values of the results provided by a given mapper function.
   *
   * This is a shorthand for `.map(mapper).uniq()`.
   *
   * @param mapper function that returns some values
   * @param <R> the type that is returned by the `mapper` function
   * @return a set of distinct values returned by the `mapper` function
   */
  @Contract(pure = true)
  public <R> SortedMap<U, Set<R>> uniq(SerializableFunction<X, R> mapper) throws Exception {
    return this.map(mapper).uniq();
  }

  /**
   * Counts all unique values of the results.
   *
   * For example, this can be used together with the OSMContributionView to get the number of unique users editing specific feature types.
   *
   * @return the set of distinct values
   */
  @Contract(pure = true)
  public SortedMap<U, Integer> countUniq() throws Exception {
    return this
        .uniq().entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().size(),
            (v1, v2) -> v1, // can't happen, actually since input is already a map
            TreeMap::new
        ));
  }

  /**
   * Calculates the averages of the results.
   *
   * The current data values need to be numeric (castable to "Number" type), otherwise a runtime exception will be thrown.
   *
   * @return the average of the current data
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   */
  @Contract(pure = true)
  public SortedMap<U, Double> average() throws Exception {
    return this
        .makeNumeric()
        .average(n -> n);
  }

  /**
   * Calculates the average of the results provided by a given `mapper` function.
   *
   * @param mapper function that returns the numbers to average
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the average of the numbers returned by the `mapper` function
   */
  @Contract(pure = true)
  public <R extends Number> SortedMap<U, Double> average(SerializableFunction<X, R> mapper) throws Exception {
    return this.weightedAverage(data -> new WeightedValue<>(mapper.apply(data), 1.0));
  }

  /**
   * Calculates the weighted average of the results provided by the `mapper` function.
   *
   * The mapper must return an object of the type `WeightedValue` which contains a numeric value associated with a (floating point) weight.
   *
   * @param mapper function that gets called for each entity snapshot or modification, needs to return the value and weight combination of numbers to average
   * @return the weighted average of the numbers returned by the `mapper` function
   */
  @Contract(pure = true)
  public SortedMap<U, Double> weightedAverage(SerializableFunction<X, WeightedValue> mapper) throws Exception {
    return this
        .map(mapper)
        .reduce(
            () -> new PayloadWithWeight<>(0.0,0.0),
            (acc, cur) -> {
              acc.num = NumberUtils.add(acc.num, cur.getValue().doubleValue()*cur.getWeight());
              acc.weight += cur.getWeight();
              return acc;
            },
            (a, b) -> new PayloadWithWeight<>(NumberUtils.add(a.num, b.num), a.weight+b.weight)
        ).entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().num / e.getValue().weight,
            (v1, v2) -> v1,
            TreeMap::new
        ));
  }

  // -----------------------------------------------------------------------------------------------
  // "Iterator" like helpers (forEach, collect), mostly intended for testing purposes
  // -----------------------------------------------------------------------------------------------

  /**
   * Iterates over the results of this data aggregation
   *
   * This method can be handy for testing purposes. But note that since the `action` doesn't produce a return value, it must facilitate its own way of producing output.
   *
   * If you'd like to use such a "forEach" in a non-test use case, use `.collect().forEach()` instead.
   *
   * @param action function that gets called for each transformed data entry
   * @deprecated only for testing purposes
   */
  @Deprecated
  public void forEach(SerializableBiConsumer<U, List<X>> action) throws Exception {
    this.collect().forEach(action);
  }

  /**
   * Collects the results of this data aggregation into Lists
   *
   * @return an aggregated map of lists with all results
   */
  @Contract(pure = true)
  public SortedMap<U, List<X>> collect() throws Exception {
    return this.reduce(
        LinkedList::new,
        (acc, cur) -> { acc.add(cur); return acc; },
        (list1, list2) -> { LinkedList<X> combinedLists = new LinkedList<>(list1); combinedLists.addAll(list2); return combinedLists; }
    );
  }

  // -----------------------------------------------------------------------------------------------
  // "map", "flatMap" transformation methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Set an arbitrary `map` transformation function.
   *
   * @param mapper function that will be applied to each data entry (osm entity snapshot or contribution)
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return a modified copy of this MapAggregator object operating on the transformed type (&lt;R&gt;)
   */
  @Contract(pure = true)
  public <R> MapAggregator<U, R> map(SerializableFunction<X, R> mapper) {
    return this.copyTransform(this._mapReducer.map(inData -> {
      //noinspection unchecked – trick/hack to replace mapped values without copying pair objects
      Pair<U,R> outData = (Pair<U,R>)inData;
      outData.setValue(mapper.apply(inData.getValue()));
      return outData;
    }));
  }

  /**
   * Set an arbitrary `mapPair` transformation function.
   *
   * @param mapper1 function that will be applied to each data entry (osm entity snapshot or contribution)
   * @param mapper2 function that will be applied to each data entry (osm entity snapshot or contribution
   * @param <R> an arbitrary data type which is the return type of the first map function mapper1
   * @param <S> an arbitrary data type which is the return type of the second map function mapper2
   * @return a modified copy of this MapAggregator object operating on the transformed type (&lt;R&gt;)
   */
  @Contract(pure = true)
  public <R, S> MapAggregator<U, Pair<R, S>> mapPair(SerializableFunction<X, R> mapper1, SerializableFunction<X, S> mapper2) {
    return this.copyTransform(this._mapReducer.map(inData -> {
      //noinspection unchecked – trick/hack to replace mapped values without copying pair objects
      Pair<U,Pair<R, S>> outData = (Pair<U,Pair<R, S>>)inData;
      outData.setValue(Pair.of(mapper1.apply(inData.getValue()), mapper2.apply(inData.getValue())));
      return outData;
    }));
  }

  /**
   * Set an arbitrary `flatMap` transformation function, which returns list with an arbitrary number of results per input data entry.
   * The results of this function will be "flattened", meaning that they can be for example transformed again by setting additional `map` functions.
   *
   * @param flatMapper function that will be applied to each data entry (osm entity snapshot or contribution) and returns a list of results
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return a modified copy of this MapAggregator object operating on the transformed type (&lt;R&gt;)
   */
  @Contract(pure = true)
  public <R> MapAggregator<U, R> flatMap(SerializableFunction<X, Iterable<R>> flatMapper) {
    return this.copyTransform(this._mapReducer.flatMap(inData -> {
      List<Pair<U, R>> outData = new LinkedList<>();
      flatMapper.apply(inData.getValue()).forEach(flatMappedData ->
          outData.add(new MutablePair<U, R>(
              inData.getKey(),
              flatMappedData
          ))
      );
      return outData;
    }));
  }

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @param f the filter function that determines if the respective data should be passed on (when f returns true) or discarded (when f returns false)
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> filter(SerializablePredicate<X> f) {
    return this.copyTransform(this._mapReducer.filter(data ->
      f.test(data.getValue())
    ));
  }

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @param tagInterpreter
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> tagInterpreter(TagInterpreter tagInterpreter) {
    return this.copyTransform(this._mapReducer.tagInterpreter(tagInterpreter));
  }

  // -----------------------------------------------------------------------------------------------
  // Neighbourhood / neighbouring
  // Functions for querying and filtering objects based on other objects in the neighbourhood
  // -----------------------------------------------------------------------------------------------

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by a call back function
   *
   * @param distance radius that defines neighbourhood in meters
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @param <Y> Return type of mapReduce function
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> neighbourhood(
      Double distance,
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    // Create spatialRelation object
    SpatialRelation<X, Y> spatialRelation = new SpatialRelation<>(
        this._mapReducer._oshdbForTags,
        this._mapReducer._bboxFilter,
        this._mapReducer._tstamps,
        mapReduce
    );
    if (this._mapReducer._forClass == OSMEntitySnapshot.class) spatialRelation.getOSMEntitySnapshotsForComparison(this._mapReducer._tstamps);
    return this.copyTransform(this._mapReducer.map(inData -> {
      try {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        outData.setValue(spatialRelation.neighbouring(inData.getValue(), distance));
        return outData;
      } catch (Exception e) {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        System.out.println(e.getMessage());
        outData.setValue(Pair.of(inData.getValue(),  null));
        return outData;
      }
    }));
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key and value
   *
   * @param distance radius that defines neighbourhood in meters
   * @param key OSM tag key for filtering neighbouring objects
   * @param value OSM tag value for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> neighbourhood(
      Double distance,
      String key,
      String value) throws Exception {
    return this.neighbourhood(
        distance,
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key, value).collect());
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key
   *
   * @param distance radius that defines neighbourhood in meters
   * @param key OSM tag key for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> neighbourhood(
      Double distance,
      String key) throws Exception {
    return this.neighbourhood(
        distance,
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots using a call back function
   *
   * @param distance radius that defines neighbourhood in meters
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of this MapAggregator
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, X> neighbouring(Double distance,
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
      MapAggregator<U, Pair<X, List<Y>>> pairMapReducer = this.neighbourhood(
          distance,
          mapReduce);
      return pairMapReducer.filter(p -> p.getRight().size() > 0).map(p -> p.getKey());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key and value
   *
   * @param distance radius that defines neighbourhood in meters
   * @param key OSM key for filtering neighbouring objects
   * @param value OSM value for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U,X> neighbouring(
      Double distance,
      String key,
      String value) throws Exception {
    return this.neighbouring(
        distance,
        mapReducer -> mapReducer.osmTag(key, value).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key
   *
   * @param distance radius that defines neighbourhood in meters
   * @param key OSM tag key for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> neighbouring(
      Double distance,
      String key) throws Exception {
    return this.neighbouring(
        distance,
        mapReducer -> mapReducer.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots
   *
   * @param distance radius that defines neighbourhood in meters
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> neighbouring(
      Double distance) throws Exception {
    return this.neighbouring(
        distance,
        mapReducer -> mapReducer.collect());
  }

  // -----------------------------------------------------------------------------------------------
  // Containment / contains
  // -----------------------------------------------------------------------------------------------

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @param <Y> Return type of mapReduce function
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> containment(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    // Create spatialRelation object
    SpatialRelation<X, Y> spatialRelation = new SpatialRelation<>(
        this._mapReducer._oshdbForTags,
        this._mapReducer._bboxFilter,
        this._mapReducer._tstamps,
        mapReduce
    );
    if (this._mapReducer._forClass == OSMEntitySnapshot.class) spatialRelation.getOSMEntitySnapshotsForComparison(this._mapReducer._tstamps);
    return this.copyTransform(this._mapReducer.map(inData -> {
      try {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        outData.setValue(spatialRelation.contains(inData.getValue()));
        return outData;
      } catch (Exception e) {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        System.out.println(e.getMessage());
        outData.setValue(Pair.of(inData.getValue(),  null));
        return outData;
      }
    }));
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key and value
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param value OSM tag value for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> containment(
      String key,
      String value) throws Exception {
    return this.containment(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key, value).collect());
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> containment(
      String key) throws Exception {
    return this.containment(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots using a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of this MapAggregator
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, X> contains(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    MapAggregator<U, Pair<X, List<Y>>> pairMapReducer = this.containment(
        mapReduce);
    return pairMapReducer.filter(p -> p.getRight().size() > 0).map(p -> p.getKey());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key and value
   *
   * @param key OSM key for filtering neighbouring objects
   * @param value OSM value for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U,X> contains(
      String key,
      String value) throws Exception {
    return this.contains(
        mapReducer -> mapReducer.osmTag(key, value).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> contains(
      String key) throws Exception {
    return this.contains(
        mapReducer -> mapReducer.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots
   *
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> contains() throws Exception {
    return this.contains(
        mapReducer -> mapReducer.collect());
  }

  // -----------------------------------------------------------------------------------------------
  // Enclosing elements / inside
  // -----------------------------------------------------------------------------------------------

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @param <Y> Return type of mapReduce function
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> enclosingElements(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    // Create spatialRelation object
    SpatialRelation<X, Y> spatialRelation = new SpatialRelation<>(
        this._mapReducer._oshdbForTags,
        this._mapReducer._bboxFilter,
        this._mapReducer._tstamps,
        mapReduce
    );
    if (this._mapReducer._forClass == OSMEntitySnapshot.class) spatialRelation.getOSMEntitySnapshotsForComparison(this._mapReducer._tstamps);
    return this.copyTransform(this._mapReducer.map(inData -> {
      try {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        outData.setValue(spatialRelation.inside(inData.getValue()));
        return outData;
      } catch (Exception e) {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        System.out.println(e.getMessage());
        outData.setValue(Pair.of(inData.getValue(),  null));
        return outData;
      }
    }));
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key and value
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param value OSM tag value for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> enclosingElements(
      String key,
      String value) throws Exception {
    return this.enclosingElements(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key, value).collect());
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> enclosingElements(
      String key) throws Exception {
    return this.enclosingElements(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots using a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of this MapAggregator
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, X> inside(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    MapAggregator<U, Pair<X, List<Y>>> pairMapReducer = this.enclosingElements(
        mapReduce);
    return pairMapReducer.filter(p -> p.getRight().size() > 0).map(p -> p.getKey());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key and value
   *
   * @param key OSM key for filtering neighbouring objects
   * @param value OSM value for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U,X> inside(
      String key,
      String value) throws Exception {
    return this.inside(
        mapReducer -> mapReducer.osmTag(key, value).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> inside(
      String key) throws Exception {
    return this.inside(
        mapReducer -> mapReducer.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots
   *
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> inside() throws Exception {
    return this.inside(
        mapReducer -> mapReducer.collect());
  }

  // -----------------------------------------------------------------------------------------------
  // Equal elements / equals
  // -----------------------------------------------------------------------------------------------

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @param <Y> Return type of mapReduce function
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> equalElements(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    // Create spatialRelation object
    SpatialRelation<X, Y> spatialRelation = new SpatialRelation<>(
        this._mapReducer._oshdbForTags,
        this._mapReducer._bboxFilter,
        this._mapReducer._tstamps,
        mapReduce
    );
    if (this._mapReducer._forClass == OSMEntitySnapshot.class) spatialRelation.getOSMEntitySnapshotsForComparison(this._mapReducer._tstamps);
    return this.copyTransform(this._mapReducer.map(inData -> {
      try {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        outData.setValue(spatialRelation.equalTo(inData.getValue()));
        return outData;
      } catch (Exception e) {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        System.out.println(e.getMessage());
        outData.setValue(Pair.of(inData.getValue(),  null));
        return outData;
      }
    }));
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key and value
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param value OSM tag value for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> equalElements(
      String key,
      String value) throws Exception {
    return this.equalElements(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key, value).collect());
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> equalElements(
      String key) throws Exception {
    return this.equalElements(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots using a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of this MapAggregator
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, X> equals(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    MapAggregator<U, Pair<X, List<Y>>> pairMapReducer = this.equalElements(
        mapReduce);
    return pairMapReducer.filter(p -> p.getRight().size() > 0).map(p -> p.getKey());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key and value
   *
   * @param key OSM key for filtering neighbouring objects
   * @param value OSM value for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U,X> equals(
      String key,
      String value) throws Exception {
    return this.equals(
        mapReducer -> mapReducer.osmTag(key, value).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> equals(
      String key) throws Exception {
    return this.equals(
        mapReducer -> mapReducer.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots
   *
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> equals() throws Exception {
    return this.equals(
        mapReducer -> mapReducer.collect());
  }

  // -----------------------------------------------------------------------------------------------
  // Overlapping elements / overlaps
  // -------------------------------------------------------------  ----------------------------------

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @param <Y> Return type of mapReduce function
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> overlap(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    // Create spatialRelation object
    SpatialRelation<X, Y> spatialRelation = new SpatialRelation<>(
        this._mapReducer._oshdbForTags,
        this._mapReducer._bboxFilter,
        this._mapReducer._tstamps,
        mapReduce
    );
    if (this._mapReducer._forClass == OSMEntitySnapshot.class) spatialRelation.getOSMEntitySnapshotsForComparison(this._mapReducer._tstamps);
    return this.copyTransform(this._mapReducer.map(inData -> {
      try {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        outData.setValue(spatialRelation.overlaps(inData.getValue()));
        return outData;
      } catch (Exception e) {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        System.out.println(e.getMessage());
        outData.setValue(Pair.of(inData.getValue(),  null));
        return outData;
      }
    }));
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key and value
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param value OSM tag value for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> overlap(
      String key,
      String value) throws Exception {
    return this.overlap(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key, value).collect());
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> overlap(
      String key) throws Exception {
    return this.overlap(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots using a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of this MapAggregator
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, X> overlaps(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    MapAggregator<U, Pair<X, List<Y>>> pairMapReducer = this.overlap(
        mapReduce);
    return pairMapReducer.filter(p -> p.getRight().size() > 0).map(p -> p.getKey());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key and value
   *
   * @param key OSM key for filtering neighbouring objects
   * @param value OSM value for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U,X> overlaps(
      String key,
      String value) throws Exception {
    return this.overlaps(
        mapReducer -> mapReducer.osmTag(key, value).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> overlaps(
      String key) throws Exception {
    return this.overlaps(
        mapReducer -> mapReducer.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots
   *
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> overlaps() throws Exception {
    return this.overlaps(
        mapReducer -> mapReducer.collect());
  }

  // -----------------------------------------------------------------------------------------------
  // Touching elements / touches
  // -----------------------------------------------------------------------------------------------

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @param <Y> Return type of mapReduce function
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> touchingElements(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    // Create spatialRelation object
    SpatialRelation<X, Y> spatialRelation = new SpatialRelation<>(
        this._mapReducer._oshdbForTags,
        this._mapReducer._bboxFilter,
        this._mapReducer._tstamps,
        mapReduce
    );
    if (this._mapReducer._forClass == OSMEntitySnapshot.class) spatialRelation.getOSMEntitySnapshotsForComparison(this._mapReducer._tstamps);
    return this.copyTransform(this._mapReducer.map(inData -> {
      try {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        outData.setValue(spatialRelation.touches(inData.getValue()));
        return outData;
      } catch (Exception e) {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        System.out.println(e.getMessage());
        outData.setValue(Pair.of(inData.getValue(),  null));
        return outData;
      }
    }));
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key and value
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param value OSM tag value for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> touchingElements(
      String key,
      String value) throws Exception {
    return this.touchingElements(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key, value).collect());
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> touchingElements(
      String key) throws Exception {
    return this.touchingElements(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots using a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of this MapAggregator
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, X> touches(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    MapAggregator<U, Pair<X, List<Y>>> pairMapReducer = this.touchingElements(
        mapReduce);
    return pairMapReducer.filter(p -> p.getRight().size() > 0).map(p -> p.getKey());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key and value
   *
   * @param key OSM key for filtering neighbouring objects
   * @param value OSM value for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U,X> touches(
      String key,
      String value) throws Exception {
    return this.touches(
        mapReducer -> mapReducer.osmTag(key, value).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> touches(
      String key) throws Exception {
    return this.touches(
        mapReducer -> mapReducer.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots
   *
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> touches() throws Exception {
    return this.touches(
        mapReducer -> mapReducer.collect());
  }

  // -----------------------------------------------------------------------------------------------
  // Covering elements / coveredBy
  // -----------------------------------------------------------------------------------------------

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @param <Y> Return type of mapReduce function
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> coveringElements(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    // Create spatialRelation object
    SpatialRelation<X, Y> spatialRelation = new SpatialRelation<>(
        this._mapReducer._oshdbForTags,
        this._mapReducer._bboxFilter,
        this._mapReducer._tstamps,
        mapReduce
    );
    if (this._mapReducer._forClass == OSMEntitySnapshot.class) spatialRelation.getOSMEntitySnapshotsForComparison(this._mapReducer._tstamps);
    return this.copyTransform(this._mapReducer.map(inData -> {
      try {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        outData.setValue(spatialRelation.coveredBy(inData.getValue()));
        return outData;
      } catch (Exception e) {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        System.out.println(e.getMessage());
        outData.setValue(Pair.of(inData.getValue(),  null));
        return outData;
      }
    }));
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key and value
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param value OSM tag value for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> coveringElements(
      String key,
      String value) throws Exception {
    return this.coveringElements(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key, value).collect());
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> coveringElements(
      String key) throws Exception {
    return this.coveringElements(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots using a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of this MapAggregator
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, X> coveredBy(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    MapAggregator<U, Pair<X, List<Y>>> pairMapReducer = this.coveringElements(
        mapReduce);
    return pairMapReducer.filter(p -> p.getRight().size() > 0).map(p -> p.getKey());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key and value
   *
   * @param key OSM key for filtering neighbouring objects
   * @param value OSM value for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U,X> coveredBy(
      String key,
      String value) throws Exception {
    return this.coveredBy(
        mapReducer -> mapReducer.osmTag(key, value).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> coveredBy(
      String key) throws Exception {
    return this.coveredBy(
        mapReducer -> mapReducer.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots
   *
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> coveredBy() throws Exception {
    return this.coveredBy(
        mapReducer -> mapReducer.collect());
  }

  // -----------------------------------------------------------------------------------------------
  // Covered elements / covers
  // -----------------------------------------------------------------------------------------------

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @param <Y> Return type of mapReduce function
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> coveredElements(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    // Create spatialRelation object
    SpatialRelation<X, Y> spatialRelation = new SpatialRelation<>(
        this._mapReducer._oshdbForTags,
        this._mapReducer._bboxFilter,
        this._mapReducer._tstamps,
        mapReduce
    );
    if (this._mapReducer._forClass == OSMEntitySnapshot.class) spatialRelation.getOSMEntitySnapshotsForComparison(this._mapReducer._tstamps);
    return this.copyTransform(this._mapReducer.map(inData -> {
      try {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        outData.setValue(spatialRelation.covers(inData.getValue()));
        return outData;
      } catch (Exception e) {
        Pair<U, Pair<X, List<Y>>> outData = (Pair<U, Pair<X, List<Y>>>)inData;
        System.out.println(e.getMessage());
        outData.setValue(Pair.of(inData.getValue(),  null));
        return outData;
      }
    }));
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key and value
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param value OSM tag value for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> coveredElements(
      String key,
      String value) throws Exception {
    return this.coveredElements(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key, value).collect());
  }

  /**
   * Get all OSMEntitySnapshots in the neighbourhood of a central OSHDB object (OSMEntitySnapshot
   * or OSMContribution) filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, Pair<X, List<Y>>> coveredElements(
      String key) throws Exception {
    return this.coveredElements(
        (SerializableFunctionWithException<MapReducer<Y>, List<Y>>) mapReduce -> mapReduce.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots using a call back function
   *
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param <Y> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of this MapAggregator
   **/
  @Contract(pure = true)
  public <Y> MapAggregator<U, X> covers(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) throws Exception {
    MapAggregator<U, Pair<X, List<Y>>> pairMapReducer = this.coveredElements(
        mapReduce);
    return pairMapReducer.filter(p -> p.getRight().size() > 0).map(p -> p.getKey());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key and value
   *
   * @param key OSM key for filtering neighbouring objects
   * @param value OSM value for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U,X> covers(
      String key,
      String value) throws Exception {
    return this.covers(
        mapReducer -> mapReducer.osmTag(key, value).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots filtered by key
   *
   * @param key OSM tag key for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> covers(
      String key) throws Exception {
    return this.covers(
        mapReducer -> mapReducer.osmTag(key).collect());
  }

  /**
   * Filter by neighbouring OSMEntitySnapshots
   *
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapAggregator<U, X> covers() throws Exception {
    return this.covers(
        mapReducer -> mapReducer.collect());
  }


  // -----------------------------------------------------------------------------------------------
  // Exposed generic reduce.
  // Can be used by experienced users of the api to implement complex queries.
  // These offer full flexibility, but are potentially a bit tricky to work with (see javadoc).
  // -----------------------------------------------------------------------------------------------

  /**
   * Map-reduce routine with built-in aggregation
   *
   * This can be used to perform an arbitrary reduce routine whose results are aggregated separately according to some custom index value.
   *
   * The combination of the used types and identity/reducer functions must make "mathematical" sense:
   * <ul>
   *   <li>the accumulator and combiner functions need to be associative,</li>
   *   <li>values generated by the identitySupplier factory must be an identity for the combiner function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   *   <li>the combiner function must be compatible with the accumulator function: `combiner(u, accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a> interface.
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;) and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter (mutate) the state of the accumulation value (e.g. directly adding new values to an existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function must be pure (have no side effects), and is not allowed to alter the state of the two input objects it gets!</b>
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the `combiner` function, after all `mapper` results have been aggregated (in the `accumulator` and `combiner` steps)
   */
  @Contract(pure = true)
  public <S> SortedMap<U, S> reduce(SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    SortedMap<U, S> result = this._mapReducer.reduce(
        TreeMap::new,
        (TreeMap<U, S> m, Pair<U, X> r) -> {
          m.put(r.getKey(), accumulator.apply(
              m.getOrDefault(r.getKey(), identitySupplier.get()),
              r.getValue()
          ));
          return m;
        },
        (a,b) -> {
          TreeMap<U, S> combined = new TreeMap<U, S>(a);
          for (SortedMap.Entry<U, S> entry: b.entrySet()) {
            combined.merge(entry.getKey(), entry.getValue(), combiner);
          }
          return combined;
        }
    );
    // fill nodata entries with "0"
    //noinspection unchecked – all zerofills must "add up" to <U>
    Collection<U> zerofill = (Collection<U>) this._completeZerofill(
        result.keySet(),
        Lists.reverse(this._zerofill)
    );
    zerofill.forEach(zerofillKey -> {
      if (!result.containsKey(zerofillKey)) {
        result.put(zerofillKey, identitySupplier.get());
      }
    });
    return result;
  }

  /**
   * Map-reduce routine with built-in aggregation (shorthand syntax)
   *
   * This can be used to perform an arbitrary reduce routine whose results are aggregated separately according to some custom index value.
   *
   * This variant is shorter to program than `reduce(identitySupplier, accumulator, combiner)`, but can only be used if
   * the result type is the same as the current `map`ped type &lt;X&gt;. Also this variant can be less efficient
   * since it cannot benefit from the mutability freedoms the accumulator+combiner approach has.
   *
   * The combination of the used types and identity/reducer functions must make "mathematical" sense:
   * <ul>
   *   <li>the accumulator function needs to be associative,</li>
   *   <li>values generated by the identitySupplier factory must be an identity for the accumulator function: `accumulator(identitySupplier(),x)` must be equal to `x`,</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-T-java.util.function.BinaryOperator-">reduce(identity,accumulator)</a> interface.
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;X&gt;) and an accumulation value (also of type &lt;X&gt;, e.g. the result of `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this function is not to alter (mutate) the state of the accumulation value (e.g. directly adding new values to an existing Set object)
   * @return the result of the map-reduce operation, the final result of the last call to the `combiner` function, after all `mapper` results have been aggregated (in the `accumulator` and `combiner` steps)
   */
  @Contract(pure = true)
  public SortedMap<U, X> reduce(SerializableSupplier<X> identitySupplier, SerializableBinaryOperator<X> accumulator) throws Exception {
    return this.reduce(identitySupplier, accumulator::apply, accumulator);
  }

  // -----------------------------------------------------------------------------------------------
  // Some helper methods for internal use in the mapReduce functions
  // -----------------------------------------------------------------------------------------------

  // casts current results to a numeric type, for summing and averaging
  @Contract(pure = true)
  private MapAggregator<U, Number> makeNumeric() {
    return this.map(x -> {
      if (!Number.class.isInstance(x)) // todo: slow??
        throw new UnsupportedOperationException("Cannot convert to non-numeric values of type: " + x.getClass().toString());
      return (Number)x;
    });
  }

  // maps from one index type to a different one
  @Contract(pure = true)
  private <V extends Comparable<V>> MapAggregator<V, X> mapIndex(SerializableBiFunction<U, X, V> keyMapper) {
    return this.copyTransformKey(this._mapReducer.map(inData -> new MutablePair<>(
        keyMapper.apply(inData.getKey(), inData.getValue()),
        inData.getValue()
    )));
  }

  // calculate complete set of indices to use for zerofilling
  private Collection<?> _completeZerofill(Set<?> keys, List<Collection<?>> zerofills) {
    if (zerofills.isEmpty()) return Collections.emptyList();
    SortedSet<Object> seen = new TreeSet<>(zerofills.get(0));
    SortedSet<Object> nextLevelKeys = new TreeSet<>();
    boolean last = false;
    for (Object index : keys) {
      Object v;
      if (index instanceof OSHDBCombinedIndex) {
        v = ((OSHDBCombinedIndex) index).getSecondIndex();
        nextLevelKeys.add(((OSHDBCombinedIndex) index).getFirstIndex());
      } else {
        last = true;
        v = index;
      }
      seen.add(v);
    }
    if (last) {
      return seen;
    } else {
      Collection<?> nextLevel = this._completeZerofill(
          nextLevelKeys,
          zerofills.subList(1, zerofills.size())
      );
      return nextLevel.stream().flatMap(u ->
          seen.stream().map(v -> new OSHDBCombinedIndex<>(u, v))
      ).collect(Collectors.toList());
    }
  }
}
