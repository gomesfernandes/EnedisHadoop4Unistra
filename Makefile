
all:
	hadoop com.sun.tools.javac.Main -d enedis_by_sector_classes  EnedisBySector.java 
	jar -cvf EnedisBySector.jar -C enedis_by_sector_classes  .

run:
	hadoop jar EnedisBySector.jar EnedisBySector input sector-output

clean:
	rm -rf enedis_by_sector_classes sector-output sector-output-final
