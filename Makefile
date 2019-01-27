
all:
	hadoop com.sun.tools.javac.Main -d enedis_classes  *.java 
	jar -cvf Enedis.jar -C enedis_classes  .

run:
	hadoop jar *.jar Enedis input output

clean:
	rm -rf enedis_classes output output-final
