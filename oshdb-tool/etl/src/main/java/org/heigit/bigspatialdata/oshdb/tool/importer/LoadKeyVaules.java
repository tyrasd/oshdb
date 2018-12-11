package org.heigit.bigspatialdata.oshdb.tool.importer;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract.KeyValuePointer;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.Role;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.VF;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;
import com.google.common.io.MoreFiles;

public class LoadKeyVaules implements Closeable {

	public static final String TABLE_KEY = "key";
	public static final String TABLE_KEYVALUE = "keyvalue";
	public static final String TABLE_ROLE = "role";

	private static final int MAX_BATCH_SIZE = 100_000;

	private final Connection conn;
	private final PreparedStatement insertKey;
	private final PreparedStatement insertValue;
	private final PreparedStatement insertRole;

	public LoadKeyVaules(String url, String user, String password) throws SQLException {
		this.conn = DriverManager.getConnection(url, user, password);

		insertKey = conn.prepareStatement("insert into " + TABLE_KEY + " (id,txt) values (?,?)");
		insertValue = conn.prepareStatement("insert into " + TABLE_KEYVALUE + " ( keyId, valueId, txt ) values(?,?,?)");
		insertRole = conn.prepareStatement("insert into " + TABLE_ROLE + " (id,txt) values(?,?)");
	}

	private void prepareTags(Connection conn) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("drop table if exists " + TABLE_ROLE + "; create table if not exists " + TABLE_ROLE
					+ "(id int primary key, txt varchar)");
		}
	}

	private void prepareRoles(Connection conn) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("drop table if exists " + TABLE_ROLE + "; create table if not exists " + TABLE_ROLE
					+ "(id int primary key, txt varchar)");
		}
	}

	@Override
	public void close() throws IOException {
		try {
			insertKey.close();
			insertValue.close();
			insertRole.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static InputStream openBufferedStream(Path path) throws IOException {
		return MoreFiles.asByteSource(path).openBufferedStream();
	}

	public void loadTags(Path keys, Path keyValues) {
		try (final DataInputStream keyIn = new DataInputStream(openBufferedStream(keys));
				final RandomAccessFile raf = new RandomAccessFile(keyValues.toFile(), "r");
				final FileChannel valuesChannel = raf.getChannel();) {

			final int length = keyIn.readInt();
			final Stopwatch stopwatch = Stopwatch.createUnstarted();
			int batch = 0;
			try {

				prepareTags(conn);

				for (int keyId = 0; keyId < length; keyId++) {
					final KeyValuePointer kvp = KeyValuePointer.read(keyIn);
					final String key = kvp.key;

					System.out.printf("load key:%6d(%s)[%d]  ", keyId, key, kvp.valuesNumber);
					stopwatch.reset().start();

					insertKey.setInt(1, keyId);
					insertKey.setString(2, key);
					insertKey.executeUpdate();
					valuesChannel.position(kvp.valuesOffset);

					DataInputStream valueStream = new DataInputStream(Channels.newInputStream(valuesChannel));

					long chunkSize = (long) Math.ceil((double) (kvp.valuesNumber / 10.0));
					int valueId = 0;
					for (int i = 0; i < 10; i++) {
						long chunkEnd = valueId + Math.min(kvp.valuesNumber - valueId, chunkSize);
						for (; valueId < chunkEnd; valueId++) {
							final VF vf = VF.read(valueStream);
							final String value = vf.value;

							insertValue.setInt(1, keyId);
							insertValue.setInt(2, valueId);
							insertValue.setString(3, value);
							insertValue.addBatch();
							batch++;

							if (batch >= MAX_BATCH_SIZE) {
								// insertValue.executeBatch();
								batch = 0;
							}
						}
						System.out.print(".");
					}
					System.out.println(". in " + stopwatch);
				}
			} catch (SQLException e) {
				throw new IOException(e);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void loadRoles(Path roles){
		try (DataInputStream roleIn = new DataInputStream(openBufferedStream(roles))) {
		      try {
		    	 prepareRoles(conn);
		   
		        for (int roleId = 0; true; roleId++) {
		          final Role role = Role.read(roleIn);
		          
		          insertRole.setInt(1,roleId);
		          insertRole.setString(2, role.role);
		          insertRole.executeUpdate();
		          System.out.printf("load role:%6d(%s)%n", roleId, role.role);
		          
		        }
		      } catch (SQLException e) {
				e.printStackTrace();
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public static class Args {
		@Parameter(names = { "-workDir",
				"--workingDir" }, description = "path to store the result files.", validateWith = DirExistValidator.class, required = true, order = 10)
		public Path workDir;

		@Parameter(names = { "--prefix" }, description = "cache table prefix", required = false)
		public String prefix;
	}

	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {
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

		Path workDirectory = config.workDir;
		String prefix = config.prefix;

		final String url;
		// url = "jdbc:h2:" + h2.toString();
		url = "jdbc:postgresql://10.11.12.21:5432/keytables-" + prefix;
		final String user;
		// user = "sa";
		user = "ohsome";
		final String password;
		// password = "";
		password = "7NTxWVeDyrzAvJQb";

		Class.forName("org.postgresql.Driver");
		try (LoadKeyVaules loader = new LoadKeyVaules(url, user, password)) {
			System.out.println("loading key values:");
			Stopwatch stopwatch = Stopwatch.createStarted();
			loader.loadTags(workDirectory.resolve("extract_keys"), workDirectory.resolve("extract_keyvalues"));
			System.out.println("key values done in "+stopwatch);
			
			System.out.println("loading roles:");
			stopwatch.reset().start();
			loader.loadRoles(workDirectory.resolve("extract_roles"));
			System.out.println("roles done in "+stopwatch);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
