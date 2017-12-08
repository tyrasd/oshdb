package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Implementation;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.api.utils.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReducer_JDBC_multithread<X> extends MapReducer<X> {
  private static final Logger LOG = LoggerFactory.getLogger(MapReducer.class);

  public MapReducer_JDBC_multithread(OSHDB_Implementation oshdb, Class<?> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  private MapReducer_JDBC_multithread(MapReducer_JDBC_multithread obj) {
    super(obj);
  }

  @NotNull
  @Override
  protected MapReducer<X> copy() {
    return new MapReducer_JDBC_multithread<X>(this);
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    TagInterpreter tagInterpreter = this._getTagInterpreter(); //load tag interpreter helper which is later used for geometry building

    final List<CellId> cellIdsList = new ArrayList<>();
    this._getCellIds().forEach(cellIdsList::add);

    return cellIdsList.parallelStream()
        .flatMap(cell -> {
          try {
            String sqlQuery = this._typeFilter.stream().map(osmType ->
              TableNames.forOSMType(osmType).map(tn -> tn.toString(this._oshdb.prefix()))
            ).filter(Optional::isPresent)
            .map(Optional::get)
            .map(tn -> "(select data from "+tn+" where level = ?1 and id = ?2)")
            .collect(Collectors.joining(" union all "));
            // fetch data from H2 DB
            PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(sqlQuery);
            pstmt.setInt(1, cell.getZoomLevel());
            pstmt.setLong(2, cell.getId());
            ResultSet oshCellsRawData = pstmt.executeQuery();

            // iterate over the result
            List<GridOSHEntity> cellsData = new ArrayList<>();
            while (oshCellsRawData.next()) {
              // get one cell from the raw data stream
              GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();
              cellsData.add(oshCellRawData);
            }
            return cellsData.stream();
          } catch (SQLException | IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return Stream.empty();
          }
        }).map(oshCell -> {
          // iterate over the history of all OSM objects in the current cell
          AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
          CellIterator.iterateAll(
              oshCell,
              this._bboxFilter,
              this._getPolyFilter(),
              new CellIterator.TimestampInterval(this._tstamps.getTimestamps().get(0), this._tstamps.getTimestamps().get(this._tstamps.getTimestamps().size()-1)),
              tagInterpreter,
              this._getPreFilter(),
              this._getFilter(),
              false
          ).forEach(contribution -> {
            OSMContribution osmContribution = new OSMContribution(
                new OSHDBTimestamp(contribution.timestamp),
                contribution.nextTimestamp != null ? new OSHDBTimestamp(contribution.nextTimestamp) : null,
                contribution.previousGeometry,
                contribution.geometry,
                contribution.previousOsmEntity,
                contribution.osmEntity,
                contribution.activities
            );
            accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(osmContribution)));
          });
          return accInternal.get();
        }).reduce(identitySupplier.get(), combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(SerializableFunction<List<OSMContribution>, List<R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    TagInterpreter tagInterpreter = this._getTagInterpreter(); //load tag interpreter helper which is later used for geometry building

    final List<CellId> cellIdsList = new ArrayList<>();
    this._getCellIds().forEach(cellIdsList::add);

    return cellIdsList.parallelStream()
        .flatMap(cell -> {
          try {
            String sqlQuery = this._typeFilter.stream().map(osmType ->
                TableNames.forOSMType(osmType).map(tn -> tn.toString(this._oshdb.prefix()))
            ).filter(Optional::isPresent)
            .map(Optional::get)
            .map(tn -> "(select data from "+tn+" where level = ?1 and id = ?2)")
            .collect(Collectors.joining(" union all "));
            // fetch data from H2 DB
            PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(sqlQuery);
            pstmt.setInt(1, cell.getZoomLevel());
            pstmt.setLong(2, cell.getId());
            ResultSet oshCellsRawData = pstmt.executeQuery();

            // iterate over the result
            List<GridOSHEntity> cellsData = new ArrayList<>();
            while (oshCellsRawData.next()) {
              // get one cell from the raw data stream
              GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();
              cellsData.add(oshCellRawData);
            }
            return cellsData.stream();
          } catch (SQLException | IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return Stream.empty();
          }
        }).map(oshCell -> {
          // iterate over the history of all OSM objects in the current cell
          AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
          List<OSMContribution> contributions = new ArrayList<>();
          CellIterator.iterateAll(
              oshCell,
              this._bboxFilter,
              this._getPolyFilter(),
              new CellIterator.TimestampInterval(this._tstamps.getTimestamps().get(0), this._tstamps.getTimestamps().get(this._tstamps.getTimestamps().size()-1)),
              tagInterpreter,
              this._getPreFilter(),
              this._getFilter(),
              false
          ).forEach(contribution -> {
            OSMContribution thisContribution = new OSMContribution(
                new OSHDBTimestamp(contribution.timestamp),
                contribution.nextTimestamp != null ? new OSHDBTimestamp(contribution.nextTimestamp) : null,
                contribution.previousGeometry,
                contribution.geometry,
                contribution.previousOsmEntity,
                contribution.osmEntity,
                contribution.activities
            );
            if (contributions.size() > 0 && thisContribution.getEntityAfter().getId() != contributions.get(contributions.size()-1).getEntityAfter().getId()) {
              // immediately fold the results
              for(R r : mapper.apply(contributions)) {
                accInternal.set(accumulator.apply(accInternal.get(), r));
              }
              contributions.clear();
            }
            contributions.add(thisContribution);
          });
          // apply mapper and fold results one more time for last entity in current cell
          if (contributions.size() > 0) {
            for(R r : mapper.apply(contributions)) {
              accInternal.set(accumulator.apply(accInternal.get(), r));
            }
          }
          return accInternal.get();
        }).reduce(identitySupplier.get(), combiner);
  }

  
  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    TagInterpreter tagInterpreter = this._getTagInterpreter(); //load tag interpreter helper which is later used for geometry building

    final List<CellId> cellIdsList = new ArrayList<>();
    this._getCellIds().forEach(cellIdsList::add);

    return cellIdsList.parallelStream()
    .flatMap(cell -> {
      try {
        String sqlQuery = this._typeFilter.stream().map(osmType ->
            TableNames.forOSMType(osmType).map(tn -> tn.toString(this._oshdb.prefix()))
        ).filter(Optional::isPresent)
        .map(Optional::get)
        .map(tn -> "(select data from "+tn+" where level = ?1 and id = ?2)")
        .collect(Collectors.joining(" union all "));
        // fetch data from H2 DB
        PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(sqlQuery);
        pstmt.setInt(1, cell.getZoomLevel());
        pstmt.setLong(2, cell.getId());
        ResultSet oshCellsRawData = pstmt.executeQuery();

        // iterate over the result
        List<GridOSHEntity> cellsData = new ArrayList<>();
        while (oshCellsRawData.next()) {
          // get one cell from the raw data stream
          GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();
          cellsData.add(oshCellRawData);
        }
        return cellsData.stream();
      } catch (SQLException | IOException | ClassNotFoundException e) {
        e.printStackTrace();
        return Stream.empty();
      }
    }).map(oshCell -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      CellIterator.iterateByTimestamps(
          oshCell,
          this._bboxFilter,
          this._getPolyFilter(),
          this._tstamps.getTimestamps(),
          tagInterpreter,
          this._getPreFilter(),
          this._getFilter(),
          false
      ).forEach(result -> result.forEach((key, value) -> {
        OSHDBTimestamp tstamp = new OSHDBTimestamp(key);
        Geometry geometry = value.getRight();
        OSMEntity entity = value.getLeft();
        OSMEntitySnapshot snapshot = new OSMEntitySnapshot(tstamp, geometry, entity);
        // immediately fold the result
        accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(snapshot)));
      }));
      return accInternal.get();
    }).reduce(identitySupplier.get(), combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    TagInterpreter tagInterpreter = this._getTagInterpreter(); //load tag interpreter helper which is later used for geometry building

    final List<CellId> cellIdsList = new ArrayList<>();
    this._getCellIds().forEach(cellIdsList::add);

    return cellIdsList.parallelStream()
        .flatMap(cell -> {
          try {
            String sqlQuery = this._typeFilter.stream().map(osmType ->
                TableNames.forOSMType(osmType).map(tn -> tn.toString(this._oshdb.prefix()))
            ).filter(Optional::isPresent)
            .map(Optional::get)
            .map(tn -> "(select data from "+tn+" where level = ?1 and id = ?2)")
            .collect(Collectors.joining(" union all "));
            // fetch data from H2 DB
            PreparedStatement pstmt = ((OSHDB_H2) this._oshdb).getConnection().prepareStatement(sqlQuery);
            pstmt.setInt(1, cell.getZoomLevel());
            pstmt.setLong(2, cell.getId());
            ResultSet oshCellsRawData = pstmt.executeQuery();

            // iterate over the result
            List<GridOSHEntity> cellsData = new ArrayList<>();
            while (oshCellsRawData.next()) {
              // get one cell from the raw data stream
              GridOSHEntity oshCellRawData = (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();
              cellsData.add(oshCellRawData);
            }
            return cellsData.stream();
          } catch (SQLException | IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return Stream.empty();
          }
        }).map(oshCell -> {
          // iterate over the history of all OSM objects in the current cell
          AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
          CellIterator.iterateByTimestamps(
              oshCell,
              this._bboxFilter,
              this._getPolyFilter(),
              this._tstamps.getTimestamps(),
              tagInterpreter,
              this._getPreFilter(),
              this._getFilter(),
              false
          ).forEach(snapshots -> {
            List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>(snapshots.size());
            snapshots.forEach((key, value) -> {
              OSHDBTimestamp tstamp = new OSHDBTimestamp(key);
              Geometry geometry = value.getRight();
              OSMEntity entity = value.getLeft();
              osmEntitySnapshots.add(new OSMEntitySnapshot(tstamp, geometry, entity));
            });
            // immediately fold the results
            for(R r : mapper.apply(osmEntitySnapshots)) {
              accInternal.set(accumulator.apply(accInternal.get(), r));
            }
          });
          return accInternal.get();
        }).reduce(identitySupplier.get(), (cur, acc) -> {
          return combiner.apply(acc, cur);
        });
  }
}
