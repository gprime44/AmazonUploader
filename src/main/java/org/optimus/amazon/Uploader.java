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

	static int localPathOffset;

	public static void main(String[] args) {
		final Path localEncodedPath = Paths.get(args[0]);
		final Path acdEncodedPath = Paths.get(args[1]);

		if (!Files.exists(localEncodedPath)) {
			System.out.println("Input path doesn't exist");
			System.exit(0);
		}

		System.out.println("Upload " + localEncodedPath + " to " + acdEncodedPath);

		localPathOffset = localEncodedPath.getNameCount();

		try {
			sync();

			Files.walkFileTree(localEncodedPath, new SimpleFileVisitor<Path>() {

				@Override
				public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
					if (!dir.equals(localEncodedPath) && !StringUtils.startsWith(dir.getFileName().toString(), ".")) {
						log(dir, "Enter " + dir.getFileName().toString());

						Path toCreate = acdEncodedPath.resolve(localEncodedPath.relativize(dir));

						CommandLine cmdLine = new CommandLine("acd_cli");
						cmdLine.addArgument("mkdir");
						cmdLine.addArgument(toCreate.toAbsolutePath().toString());

						ByteArrayOutputStream outputStream = null;
						try {
							log(dir, "Create " + toCreate.toAbsolutePath().toString());
							DefaultExecutor executor = new DefaultExecutor();
							outputStream = new ByteArrayOutputStream();
							PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
							executor.setStreamHandler(streamHandler);
							executor.execute(cmdLine);
							log(dir, outputStream.toString());
						} catch (Exception e) {
							log(dir, e.getMessage());
						} finally {
							IOUtils.closeQuietly(outputStream);
						}
					}
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
					log(file, file.getFileName().toString());

					Path toUpload = acdEncodedPath.resolve(localEncodedPath.relativize(file)).getParent();

					CommandLine cmdLine = new CommandLine("acd_cli");
					cmdLine.addArgument("upload");
					cmdLine.addArgument(file.toAbsolutePath().toString());
					cmdLine.addArgument(toUpload.toAbsolutePath().toString());

					ByteArrayOutputStream outputStream = null;
					int nbAttempt = 0;
					try {
						log(file, "Upload to " + toUpload.toAbsolutePath().toString() + " attempt " + ++nbAttempt);
						DefaultExecutor executor = new DefaultExecutor();
						outputStream = new ByteArrayOutputStream();
						PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
						executor.setStreamHandler(streamHandler);
						long start = Calendar.getInstance().getTimeInMillis();
						executor.execute(cmdLine);
						log(file, outputStream.toString() + "(" + (Calendar.getInstance().getTimeInMillis() - start) / 1000 + " sec)");
						log(file, "Delete " + file.getFileName().toString());
						Files.delete(file);
					} catch (Exception e) {
						log(file, e.getMessage());
					} finally {
						IOUtils.closeQuietly(outputStream);
					}

					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					if (!dir.equals(localEncodedPath)) {
						log(dir, "Exit " + dir.getFileName().toString());
						try {
							log(dir, "Delete " + dir.getFileName().toString());
							Files.delete(dir);
						} catch (Exception e) {
							log(dir, e.getMessage());
						}
					}
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
					log(file, e.getMessage());
					return FileVisitResult.CONTINUE;
				}

			});

			sync();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void log(Path processedPath, String message) {
		StringBuilder log = new StringBuilder();
		for (int i = 0; i < processedPath.getNameCount() - localPathOffset; i++) {
			log.append(" ...");
		}
		System.out.println(log.toString() + " " + message);
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
