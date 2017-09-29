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
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.util.MapUtils;
import org.apache.commons.io.IOUtils;

public class Uploader {

	static int localPathOffset;

	static int deletedFolder = 0;

	public static void main(String[] args) throws IOException {
		doRclone(Paths.get(args[0]), args[1], Integer.parseInt(args[2]));
	}

	private static void doRclone(final Path toUploadPath, final String remotePath, Integer nbFileToUpload) throws IOException {
		System.out.println("Upload " + toUploadPath + " to " + remotePath);

		System.out.println("Get files to upload");
		final TreeMap<Path, String> filesToUpload = new TreeMap<>(new Comparator<Path>() {

			public int compare(Path o1, Path o2) {
				try {
					return Files.getLastModifiedTime(o1).compareTo(Files.getLastModifiedTime(o2));
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(0);
				}
				return 0;
			}
		});

		Files.walkFileTree(toUploadPath, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
				filesToUpload.put(file, remotePath + toUploadPath.relativize(file).getParent());
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
				log(file, e.getMessage());
				return FileVisitResult.CONTINUE;
			}

		});

		System.out.println("Found " + filesToUpload.size() + " files to upload");

		if (nbFileToUpload == -1) {
			nbFileToUpload = filesToUpload.size();
		}

		System.out.println("Start upload");
		int i = 0;
		for (Entry<Path, String> entry : filesToUpload.entrySet()) {
			if (++i > nbFileToUpload) {
				System.out.println("Max file to upload reached, stop upload !");
				System.exit(0);
			}

			System.out.println("(" + i + "/" + filesToUpload.size() + ") - " + //
					Files.getLastModifiedTime(entry.getKey()) + //
					" - Upload " + entry.getKey().toAbsolutePath().toString() + //
					" to " + entry.getValue());

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}

			CommandLine cmdLine = new CommandLine("rclone");
			cmdLine.addArgument("copy");
			cmdLine.addArgument(entry.getKey().toAbsolutePath().toString());
			cmdLine.addArgument(entry.getValue());

			ByteArrayOutputStream outputStream = null;
			try {
				DefaultExecutor executor = new DefaultExecutor();
				outputStream = new ByteArrayOutputStream();
				PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
				executor.setStreamHandler(streamHandler);
				long start = Calendar.getInstance().getTimeInMillis();
				executor.execute(cmdLine);
				System.out.println(outputStream.toString() + "(" + (Calendar.getInstance().getTimeInMillis() - start) / 1000 + " sec)");
				System.out.println("Delete " + entry.getKey().getFileName().toString());
				Files.delete(entry.getKey());
			} catch (Exception e) {
				System.out.println(e.getMessage());
			} finally {
				IOUtils.closeQuietly(outputStream);
			}
		}

		System.out.println("Clean empty folder");

		Files.walkFileTree(toUploadPath, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				if (!dir.equals(toUploadPath)) {
					try {
						log(dir, "Delete " + dir.getFileName().toString());
						Files.delete(dir);
					} catch (Exception e) {
						log(dir, e.getMessage());
					}
				}
				return FileVisitResult.CONTINUE;
			}

		});
	}

	private static void log(Path processedPath, String message) {
		StringBuilder log = new StringBuilder();
		for (int i = 0; i < processedPath.getNameCount() - localPathOffset; i++) {
			log.append(" ...");
		}
		System.out.println(log.toString() + " " + message);
	}
}
