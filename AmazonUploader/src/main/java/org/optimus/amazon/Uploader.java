package org.optimus.amazon;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Calendar;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

public class Uploader {

	public static void main(String[] args) {
		final Path localEncodedPath = Paths.get(args[0]);
		final Path acdEncodedPath = Paths.get(args[1]);

		if (!Files.exists(localEncodedPath)) {
			System.out.println("Input path doesn't exist");
			System.exit(0);
		}

		try {
			sync();

			Files.walkFileTree(localEncodedPath, new SimpleFileVisitor<Path>() {

				@Override
				public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs)
						throws IOException {
					if (!dir.equals(localEncodedPath) && !StringUtils.startsWith(dir.getFileName().toString(), ".")) {
						System.out.println("Process directory " + dir);
						Path toCreate = acdEncodedPath.resolve(localEncodedPath.relativize(dir));

						CommandLine cmdLine = new CommandLine("acd_cli");
						cmdLine.addArgument("mkdir");
						cmdLine.addArgument(toCreate.toAbsolutePath().toString());

						ByteArrayOutputStream outputStream = null;
						try {
							System.out.println("Create directory " + toCreate.toAbsolutePath().toString());
							DefaultExecutor executor = new DefaultExecutor();
							outputStream = new ByteArrayOutputStream();
							PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
							executor.setStreamHandler(streamHandler);
							executor.execute(cmdLine);
							System.out.println(outputStream.toString());
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							IOUtils.closeQuietly(outputStream);
						}
					}
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
					System.out.println("Process file " + file);
					Path toUpload = acdEncodedPath.resolve(localEncodedPath.relativize(file)).getParent();

					CommandLine cmdLine = new CommandLine("acd_cli");
					cmdLine.addArgument("upload");
					cmdLine.addArgument(file.toAbsolutePath().toString());
					cmdLine.addArgument(toUpload.toAbsolutePath().toString());

					ByteArrayOutputStream outputStream = null;
					try {
						System.out.println("Upload file to " + toUpload.toAbsolutePath().toString());
						DefaultExecutor executor = new DefaultExecutor();
						outputStream = new ByteArrayOutputStream();
						PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
						executor.setStreamHandler(streamHandler);
						long start = Calendar.getInstance().getTimeInMillis();
						executor.execute(cmdLine);
						System.out.println(outputStream.toString() + "("
								+ (Calendar.getInstance().getTimeInMillis() - start) / 1000 + "sec)");
						Files.delete(file);
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						IOUtils.closeQuietly(outputStream);
					}

					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					if (!dir.equals(localEncodedPath)) {
						System.out.println("Directory " + dir + " processed");
						try {
							Files.delete(dir);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
					exc.toString();
					return FileVisitResult.CONTINUE;
				}

			});

			sync();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void sync() {
		CommandLine cmdLine = new CommandLine("acd_cli");
		cmdLine.addArgument("sync");

		ByteArrayOutputStream outputStream = null;
		try {
			System.out.println("Sync node");
			DefaultExecutor executor = new DefaultExecutor();
			outputStream = new ByteArrayOutputStream();
			PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
			executor.setStreamHandler(streamHandler);
			executor.execute(cmdLine);
			System.out.println(outputStream.toString());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(outputStream);
		}
	}
}