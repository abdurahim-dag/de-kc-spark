Практическое задание: Spark
Подготовка
Создать S3 бакет (Object Storage).
Скопировать данные желтого такси за 2020 год в созданный s3 бакет (см. Практика №1. по 1 уроку «Основы HADOOP. HDFS»).
(если выполнялось ДЗ №1 — указанные шаги можно пропустить).

Задание:
Создать таблицу-витрину для данных за январь 2020 года вида:

Payment type	Date	Average trip cost	Avg trip km cost
Cash	2020-01-31	999.99	8.53
Требования к скриптам:
кол-во файлов — 1
формат — .csv
сортировка — Date по убыванию, Payment type по возрастанию

Необходимо предварительно настроить локальный спарк
https://sparkbyexamples.com/pyspark/how-to-install-and-run-pyspark-on-windows/

Затем submit:
spark-submit main.py --master yarn --deploy-mode cluster
