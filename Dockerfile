# Наследуемся от CentOS 7
FROM ubuntu:16.04

# Выбираем рабочую папку
WORKDIR /opt

# Устанавливаем wget и скачиваем Go
#RUN yum install -y wget && \
#    wget http://localhost:8080/static/jdk1.8.0_101.tar.gz

RUN apt-get update && apt-get install -y locales openssl libssl-dev && rm -rf /var/lib/apt/lists/* \
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
ENV LANG en_US.utf8

ADD jdk1.8.0_101.tar.gz ./

RUN ls -lh /opt/

#RUN tar -xzf /opt/jdk1.8.0_101.tar.gz

# Задаем переменные окружения для работы Go
ENV PATH=${PATH}:/opt/jdk1.8.0_101/bin JAVA_HOME=/opt/jdk1.8.0_101

# Копируем наш исходный main.go внутрь контейнера, в папку go/src/dumb
ADD build/libs/solution-all-1.0-SNAPSHOT.jar /opt/solution.jar

# Открываем 80-й порт наружу
EXPOSE 80

# Запускаем наш сервер
CMD java -jar /opt/solution.jar env=prod

