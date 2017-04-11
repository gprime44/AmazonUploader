FROM maven:3.2-jdk-7-onbuild

VOLUME /toupload

CMD ["java", "-jar", "/usr/src/app/target/AmazonUploader-0.0.1-SNAPSHOT-jar-with-dependencies.jar /toupload /encoded_media_data "]