FROM maven:3.2-jdk-7-onbuild
CMD ["java", "-jar", "/target/app.jar"]