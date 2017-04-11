FROM maven:3.2-jdk-7-onbuild

ENV TARGET=/encoded_media_data

VOLUME /toupload

CMD ["java", "-jar", "/target/app.jar /toupload ${TARGET} "]