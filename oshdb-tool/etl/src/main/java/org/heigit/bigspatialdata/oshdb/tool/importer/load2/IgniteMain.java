package org.heigit.bigspatialdata.oshdb.tool.importer.load2;

import java.io.File;
import java.nio.file.Paths;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.loader.IgniteLoader;

import com.google.common.collect.Lists;

public class IgniteMain {

	public static void main(String[] args) throws IgniteCheckedException {
		final File igniteXML = Paths.get("./ohsome.xml").toFile();
		
		Ignition.setClientMode(true);
		IgniteConfiguration cfg = IgnitionEx.loadConfiguration(igniteXML.toString()).get1();
		cfg.setIgniteInstanceName("IgniteImportClientInstance");

		try (Ignite ignite = Ignition.start(cfg)) {
			ignite.cluster().active(true);
			//ignite.cluster().active(false);
			ignite.cacheNames().forEach(System.out::println);
			
			IgniteCache<Long, GridOSHWays> wayCache = ignite.cache("global_v4_grid_way");
			System.out.println(wayCache.size(CachePeekMode.PRIMARY));
				
		//	ignite.destroyCaches(Lists.newArrayList("planet_grid_relation","planet_grid_node","planet_grid_way"));
		}

	}

}
