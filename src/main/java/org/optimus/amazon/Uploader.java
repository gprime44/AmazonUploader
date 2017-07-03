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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

public class Uploader {

	static int localPathOffset;

	static int deletedFolder = 0;

	public static void main(String[] args) throws IOException {
		doRclone(Paths.get(args[0]), args[1], Boolean.getBoolean(args[2]));
	}

	private static void doRclone(final Path toUploadPath, final String remotePath, final Boolean testMode) throws IOException {
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

		System.out.println("Start upload");
		int i = 0;
		for (Entry<Path, String> entry : filesToUpload.entrySet()) {
			System.out.println("(" + i++ + "/" + filesToUpload.size() + ") - " + //
					Files.getLastModifiedTime(entry.getKey()) + //
					" - Upload " + entry.getKey().toAbsolutePath().toString() + //
					" to " + entry.getValue());

			if (testMode) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}

			if (!testMode) {
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
		}

		System.out.println("Clean empty folder");

		Files.walkFileTree(toUploadPath, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				if (!dir.equals(toUploadPath)) {
					try {
						log(dir, "Delete " + dir.getFileName().toString());
						if (!testMode) {
							Files.delete(dir);
						}
					} catch (Exception e) {
						log(dir, e.getMessage());
					}
				}
				return FileVisitResult.CONTINUE;
			}

		});
	}

	private static void doAcdCli(final Path localEncodedPath, final Path acdEncodedPath) {
		if (!Files.exists(localEncodedPath)) {
			System.out.println("Local path doesn't exist");
			System.exit(0);
		}

		System.out.println("####### Upload " + localEncodedPath + " to " + acdEncodedPath + " #######");

		localPathOffset = localEncodedPath.getNameCount();

		long start = Calendar.getInstance().getTimeInMillis();

		try {
			System.out.println("Get files to upload");
			final TreeMap<Path, Path> filesToUpload = new TreeMap<Path, Path>(new Comparator<Path>() {

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

			Files.walkFileTree(localEncodedPath, new SimpleFileVisitor<Path>() {

				@Override
				public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
					if (!dir.equals(localEncodedPath) && !StringUtils.startsWith(dir.getFileName().toString(), ".")) {
						log(dir, "Enter " + dir.getFileName().toString());

						if (!isDirectoryExist(dir)) {
							log(dir, "Path " + dir.toAbsolutePath().toString() + " doesn't exist on ACD, create it");

							Path toCreate = acdEncodedPath.resolve(localEncodedPath.relativize(dir));

							createDirectoryACD(dir, toCreate);
						}
					}
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
					filesToUpload.put(file, acdEncodedPath.resolve(localEncodedPath.relativize(file)).getParent());
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
					log(file, e.getMessage());
					return FileVisitResult.CONTINUE;
				}

			});

			System.out.println("####### Found " + filesToUpload.size() + " files to upload in " + (Calendar.getInstance().getTimeInMillis() - start) / 1000 + " sec #######");

			start = Calendar.getInstance().getTimeInMillis();
			System.out.println("####### Start upload #######");
			int i = 0;
			for (Entry<Path, Path> entry : filesToUpload.entrySet()) {
				System.out.println(i++ + "/" + filesToUpload.size() + " - " + Files.getLastModifiedTime(entry.getKey()).toString() + " - Upload " + entry.getKey() + " to " + entry.getValue());
				uploadFileACD(entry.getKey(), entry.getValue());
			}

			System.out.println("####### " + filesToUpload.size() + " files processed in " + (Calendar.getInstance().getTimeInMillis() - start) / 1000 + " sec #######");

			start = Calendar.getInstance().getTimeInMillis();
			System.out.println("####### Clean empty folder #######");

			Files.walkFileTree(localEncodedPath, new SimpleFileVisitor<Path>() {

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					if (!dir.equals(localEncodedPath)) {
						log(dir, "Exit " + dir.getFileName().toString());
						try {
							log(dir, "Delete " + dir.getFileName().toString());
							Files.delete(dir);
							deletedFolder++;
						} catch (Exception e) {
							log(dir, e.getMessage());
						}
					}
					return FileVisitResult.CONTINUE;
				}

			});

			System.out.println("####### " + deletedFolder + " folder deleted in " + (Calendar.getInstance().getTimeInMillis() - start) / 1000 + " sec #######");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void uploadFileACD(Path fileToUpload, Path remotePathToUpload) {
		CommandLine cmdLine = new CommandLine("acd_cli");
		cmdLine.addArgument("upload");
		cmdLine.addArgument(fileToUpload.toAbsolutePath().toString());
		cmdLine.addArgument(remotePathToUpload.toAbsolutePath().toString());

		ByteArrayOutputStream outputStream = null;
		try {
			DefaultExecutor executor = new DefaultExecutor();
			outputStream = new ByteArrayOutputStream();
			PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
			executor.setStreamHandler(streamHandler);
			long start = Calendar.getInstance().getTimeInMillis();
			executor.execute(cmdLine);
			System.out.println(outputStream.toString() + "(" + (Calendar.getInstance().getTimeInMillis() - start) / 1000 + " sec)");
			System.out.println("Delete " + fileToUpload.getFileName().toString());
			Files.delete(fileToUpload);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			IOUtils.closeQuietly(outputStream);
		}
	}

	private static void createDirectoryACD(final Path dir, Path toCreate) {
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

	private static boolean isDirectoryExist(Path toFind) {
		CommandLine cmdLine = new CommandLine("acd_cli");
		cmdLine.addArgument("find");
		cmdLine.addArgument(toFind.getFileName().toString());

		ByteArrayOutputStream outputStream = null;
		try {
			log(toFind, "Find " + toFind.getFileName().toString());
			DefaultExecutor executor = new DefaultExecutor();
			outputStream = new ByteArrayOutputStream();
			PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
			executor.setStreamHandler(streamHandler);
			executor.execute(cmdLine);
			return StringUtils.isNotEmpty(outputStream.toString());
		} catch (Exception e) {
			log(toFind, e.getMessage());
			return false;
		} finally {
			IOUtils.closeQuietly(outputStream);
		}
	}

	private static void log(Path processedPath, String message) {
		StringBuilder log = new StringBuilder();
		for (int i = 0; i < processedPath.getNameCount() - localPathOffset; i++) {
			log.append(" ...");
		}
		System.out.println(log.toString() + " " + message);
	}
}
