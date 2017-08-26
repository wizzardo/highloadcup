#FROM ubuntu:16.04
FROM clearlinux:base

WORKDIR /opt

#RUN apt-get update && apt-get install -y locales openssl libssl-dev apache2-utils && rm -rf /var/lib/apt/lists/* \
#    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
#ENV LANG en_US.utf8

#ADD jdk1.8.0_101.tar.gz ./
#ADD jdk-8u144-linux-x64.tar.gz ./
ADD openjdk-shenandoah-jdk8-release-2017-08-14.tar.gz ./

ADD wrk ./

#ENV PATH=${PATH}:/opt/jdk1.8.0_101/bin JAVA_HOME=/opt/jdk1.8.0_101
#ENV PATH=${PATH}:/opt/jdk1.8.0_144/bin JAVA_HOME=/opt/jdk1.8.0_144
ENV PATH=${PATH}:/opt/openjdk-shenandoah-jdk8-release-2017-08-14/bin JAVA_HOME=/opt/openjdk-shenandoah-jdk8-release-2017-08-14

ENV LANG=en_US.UTF-8

EXPOSE 80

CMD mkdir ./lib
COPY koloboke*.jar ./lib/

ADD build/libs/solution-all-1.0-SNAPSHOT.jar /opt/solution.jar
CMD java -XX:+UseCompressedOops -XX:+UseG1GC -Xmx3500m -Xms3500m -XX:+AggressiveOpts -verbose:gc -server -cp "solution.jar:/opt/lib/*" ru.highloadcup.App env=prod

