package org.heigit.bigspatialdata.oshdb.tool.importer;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.TransformerTagRoles;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform2.Transform;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.cellmapping.CellDataSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.plain.PlainIdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform2.TransformNode;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Stopwatch;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;

public class TransformMain {

	private static enum Step {
		Node, Way, Relation
	}

	public static class StepConverter implements IStringConverter<Step>, IParameterValidator {

		@Override
		public void validate(String name, String value) throws ParameterException {
			Step step = convert(value);
			if (step == null)
				throw new ParameterException(value + " for parameter " + name
						+ " is not a valid value. Allowed values are (n,node,w,way,r,relation)");
		}

		@Override
		public Step convert(String value) {
			final String step = value.trim().toLowerCase();
			switch (step) {
			case "n":
			case "node":
				return Step.Node;
			case "w":
			case "way":
				return Step.Way;
			case "r":
			case "relation":
				return Step.Relation;
			default:
				return null;
			}
		}

	}

	private static class Args {
		@ParametersDelegate
		Transform.Args transformArgs = new Transform.Args();

		@Parameter(names = { "-s", "--step" }, description = "step for transformation (node|way|relation)", validateWith = StepConverter.class, converter = StepConverter.class, required = true, order = 1)
		Step step;

	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		Args config = new Args();
		JCommander jcom = JCommander.newBuilder().addObject(config).build();

		try {
			jcom.parse(args);
		} catch (ParameterException e) {
			System.out.println("");
			System.out.println(e.getLocalizedMessage());
			System.out.println("");
			jcom.usage();
			return;
		}

		final Step step = config.step;
		final Path workDir = config.transformArgs.workDir;
		final int workerId = config.transformArgs.worker;

		final Stopwatch stopwatch = Stopwatch.createUnstarted();

		final TagToIdMapper tagToId;

		System.out.print("loading tagToIdMap ...");
		stopwatch.reset().start();
		tagToId = TransformerTagRoles.getTagToIdMapper(workDir);
		System.out.println(" in " + stopwatch);

		final long availableHeapMemory = SizeEstimator.estimateAvailableMemory();
		System.out.println("available memory for transformation: " + availableHeapMemory / (1024L * 1024L) + "mb");

		ByteBuffer zoomCellId = ByteBuffer.allocate(5);

		switch (step) {
		case Node: {
			final Path id2CellPath = workDir.resolve(String.format("transform_id2cell_node_%02d", workerId));
			try (CellDataSink cellDataSink = new CellDataMap(workDir, String.format("transform_node_%02d", workerId),
					availableHeapMemory / 2)) {
				try (CountingOutputStream id2Cell = new CountingOutputStream(
						Files.asByteSink(id2CellPath.toFile()).openBufferedStream());
						DataOutputStream id2CellIdx = new DataOutputStream(Files
								.asByteSink(Paths.get(id2CellPath.toString() + ".idx").toFile()).openBufferedStream());
						IdToCellSink idToCellSink = new PlainIdToCellSink((index, page, size) -> {
							long filePos = id2Cell.getCount();
							for (long cellId : page) {
								final int z = ZGrid.getZoom(cellId);
								final int id = Math.toIntExact(ZGrid.getIdWithoutZoom(cellId));
								zoomCellId.clear();
								zoomCellId.put((byte) z);
								zoomCellId.putInt(id);
								id2Cell.write(zoomCellId.array(), 0, zoomCellId.capacity());
							}
							id2CellIdx.writeInt(index);
							id2CellIdx.writeLong(filePos);
						})) {

					TransformNode.transform(config.transformArgs, tagToId, cellDataSink, idToCellSink);
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		}
	}

}
