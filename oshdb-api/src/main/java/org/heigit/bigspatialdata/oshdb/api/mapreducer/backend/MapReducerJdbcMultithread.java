package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampInterval;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReducerJdbcMultithread<X> extends MapReducer<X> {
  private static final Logger LOG = LoggerFactory.getLogger(MapReducer.class);

  public MapReducerJdbcMultithread(OSHDBDatabase oshdb,
      Class<? extends OSHDBMapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  private MapReducerJdbcMultithread(MapReducerJdbcMultithread obj) {
    super(obj);
  }

  @NotNull
  @Override
  protected MapReducer<X> copy() {
    return new MapReducerJdbcMultithread<X>(this);
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    OSHDBTimestampInterval timestampInterval = new OSHDBTimestampInterval(this._tstamps.get());

    final List<Pair<CellId, CellId>> cellIdRanges = new ArrayList<>();
    this._getCellIdRanges().forEach(cellIdRanges::add);

    return cellIdRanges.parallelStream().flatMap(cellIdRange -> {
      try {
        String sqlQuery = this._typeFilter.stream()
            .map(osmType -> TableNames.forOSMType(osmType)
                .map(tn -> tn.toString(this._oshdb.prefix())))
            .filter(Optional::isPresent).map(Optional::get)
            .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
            .collect(Collectors.joining(" union all "));
        // fetch data from H2 DB
        PreparedStatement pstmt =
            ((OSHDBJdbc)this._oshdb).getConnection().prepareStatement(sqlQuery);
        pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
        pstmt.setLong(2, cellIdRange.getLeft().getId());
        pstmt.setLong(3, cellIdRange.getRight().getId());

        ResultSet oshCellsRawData = pstmt.executeQuery();

        // iterate over the result
        List<GridOSHEntity> cellsData = new ArrayList<>();
        while (oshCellsRawData.next()) {
          // get one cell from the raw data stream
          GridOSHEntity oshCellRawData =
              (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                  .readObject();
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
      cellIterator.iterateByContribution(oshCell, timestampInterval)
          .forEach(contribution -> {
            OSMContribution osmContribution = new OSMContribution(contribution);
            accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(osmContribution)));
          });
      return accInternal.get();
    }).reduce(identitySupplier.get(), combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    OSHDBTimestampInterval timestampInterval = new OSHDBTimestampInterval(this._tstamps.get());

    final List<Pair<CellId, CellId>> cellIdRanges = new ArrayList<>();
    this._getCellIdRanges().forEach(cellIdRanges::add);

    return cellIdRanges.parallelStream().flatMap(cellIdRange -> {
      try {
        String sqlQuery = this._typeFilter.stream()
            .map(osmType -> TableNames.forOSMType(osmType)
                .map(tn -> tn.toString(this._oshdb.prefix())))
            .filter(Optional::isPresent).map(Optional::get)
            .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
            .collect(Collectors.joining(" union all "));
        // fetch data from H2 DB
        PreparedStatement pstmt =
            ((OSHDBJdbc)this._oshdb).getConnection().prepareStatement(sqlQuery);
        pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
        pstmt.setLong(2, cellIdRange.getLeft().getId());
        pstmt.setLong(3, cellIdRange.getRight().getId());

        ResultSet oshCellsRawData = pstmt.executeQuery();

        // iterate over the result
        List<GridOSHEntity> cellsData = new ArrayList<>();
        while (oshCellsRawData.next()) {
          // get one cell from the raw data stream
          GridOSHEntity oshCellRawData =
              (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                  .readObject();
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
      cellIterator.iterateByContribution(oshCell, timestampInterval)
          .forEach(contribution -> {
            OSMContribution thisContribution = new OSMContribution(contribution);
            if (contributions.size() > 0 && thisContribution.getEntityAfter()
                .getId() != contributions.get(contributions.size() - 1).getEntityAfter().getId()) {
              // immediately fold the results
              for (R r : mapper.apply(contributions)) {
                accInternal.set(accumulator.apply(accInternal.get(), r));
              }
              contributions.clear();
            }
            contributions.add(thisContribution);
          });
      // apply mapper and fold results one more time for last entity in current cell
      if (contributions.size() > 0) {
        for (R r : mapper.apply(contributions)) {
          accInternal.set(accumulator.apply(accInternal.get(), r));
        }
      }
      return accInternal.get();
    }).reduce(identitySupplier.get(), combiner);
  }

  private static long t0 = 0;
  private static long t1 = 0;
  private static long t2 = 0;
  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    long __t = System.nanoTime();
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    SortedSet<OSHDBTimestamp> timestamps = this._tstamps.get();

    final List<Pair<CellId, CellId>> cellIdRanges = new ArrayList<>();
    this._getCellIdRanges().forEach(cellIdRanges::add);

    S ret = cellIdRanges.parallelStream().flatMap(cellIdRange -> {
      long _t = System.nanoTime();
      try {
        String sqlQuery = this._typeFilter.stream()
            .map(osmType -> TableNames.forOSMType(osmType)
                .map(tn -> tn.toString(this._oshdb.prefix())))
            .filter(Optional::isPresent).map(Optional::get)
            .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
            .collect(Collectors.joining(" union all "));
        // fetch data from H2 DB
        PreparedStatement pstmt =
            ((OSHDBJdbc) this._oshdb).getConnection().prepareStatement(sqlQuery);
        pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
        pstmt.setLong(2, cellIdRange.getLeft().getId());
        pstmt.setLong(3, cellIdRange.getRight().getId());
        ResultSet oshCellsRawData = pstmt.executeQuery();

        // iterate over the result
        List<GridOSHEntity> cellsData = new ArrayList<>();
        while (oshCellsRawData.next()) {
          // get one cell from the raw data stream
          GridOSHEntity oshCellRawData =
              (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                  .readObject();
          cellsData.add(oshCellRawData);
        }
        t1 += System.nanoTime() - _t;
        return cellsData.stream();
      } catch (SQLException | IOException | ClassNotFoundException e) {
        e.printStackTrace();
        return Stream.empty();
      }
    }).map(oshCell -> {
      long _t = System.nanoTime();
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      cellIterator.iterateByTimestamps(oshCell, timestamps).forEach(data -> {
        OSMEntitySnapshot snapshot = new OSMEntitySnapshot(data);
        // immediately fold the result
        accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(snapshot)));
      });
      t2 += System.nanoTime() - _t;
      return accInternal.get();
    }).reduce(identitySupplier.get(), combiner);
    t0 += System.nanoTime() - __t;
    {
      System.out.println();
      System.out.println();
      System.out.println(t0/1E9 + "\t" + t1/1E9 + "\t" + t2/1E9);
      System.out.println();
      System.out.println();
    }
    return ret;
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    SortedSet<OSHDBTimestamp> timestamps = this._tstamps.get();

    final List<Pair<CellId, CellId>> cellIdRanges = new ArrayList<>();
    this._getCellIdRanges().forEach(cellIdRanges::add);

    return cellIdRanges.parallelStream().flatMap(cellIdRange -> {
      try {
        String sqlQuery = this._typeFilter.stream()
            .map(osmType -> TableNames.forOSMType(osmType)
                .map(tn -> tn.toString(this._oshdb.prefix())))
            .filter(Optional::isPresent).map(Optional::get)
            .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
            .collect(Collectors.joining(" union all "));
        // fetch data from H2 DB
        PreparedStatement pstmt =
            ((OSHDBJdbc)this._oshdb).getConnection().prepareStatement(sqlQuery);
        pstmt.setInt(1, cellIdRange.getLeft().getZoomLevel());
        pstmt.setLong(2, cellIdRange.getLeft().getId());
        pstmt.setLong(3, cellIdRange.getRight().getId());
        ResultSet oshCellsRawData = pstmt.executeQuery();

        // iterate over the result
        List<GridOSHEntity> cellsData = new ArrayList<>();
        while (oshCellsRawData.next()) {
          // get one cell from the raw data stream
          GridOSHEntity oshCellRawData =
              (GridOSHEntity) (new ObjectInputStream(oshCellsRawData.getBinaryStream(1)))
                  .readObject();
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
      List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>();
      cellIterator.iterateByTimestamps(oshCell, timestamps).forEach(data -> {
        OSMEntitySnapshot thisSnapshot = new OSMEntitySnapshot(data);
        if (osmEntitySnapshots.size() > 0
            && thisSnapshot.getEntity().getId() != osmEntitySnapshots
            .get(osmEntitySnapshots.size() - 1).getEntity().getId()) {
          // immediately fold the results
          for (R r : mapper.apply(osmEntitySnapshots)) {
            accInternal.set(accumulator.apply(accInternal.get(), r));
          }
          osmEntitySnapshots.clear();
        }
        osmEntitySnapshots.add(thisSnapshot);
      });
      // apply mapper and fold results one more time for last entity in current cell
      if (osmEntitySnapshots.size() > 0) {
        for (R r : mapper.apply(osmEntitySnapshots)) {
          accInternal.set(accumulator.apply(accInternal.get(), r));
        }
      }
      return accInternal.get();
    }).reduce(identitySupplier.get(), (cur, acc) -> {
      return combiner.apply(acc, cur);
    });
  }
}
