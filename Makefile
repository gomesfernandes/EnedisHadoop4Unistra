
RED="\x1B[31m"
RESET="\x1B[0m"

all: sector conso

sector:
	@echo $(RED)"\n>>>Building Sector classes\n"$(RESET)
	hadoop com.sun.tools.javac.Main -d enedis_by_sector_classes  EnedisBySector.java  
	jar -cvf EnedisBySector.jar -C enedis_by_sector_classes  .

conso:
	@echo $(RED)"\n>>>Building Consumption classes\n"$(RESET)
	hadoop com.sun.tools.javac.Main -d enedis_by_conso_classes  EnedisByConsumption.java  
	jar -cvf EnedisByConsumption.jar -C enedis_by_conso_classes  .

run: cleanOutput runSector runConso
	@echo $(RED)"\n>>>Printing results:\n"$(RESET)
	cat all_outputs/sector-output-final/part-r-00000 
	@echo "\n"
	cat all_outputs/conso-output-final/part-r-00000 

runSector:
	@echo $(RED)"\n>>>Running Sector analysis\n"$(RESET)
	hadoop jar EnedisBySector.jar EnedisBySector input sector-output
	@echo $(RED)"\n>>>End of Sector analysis\n"$(RESET)
	mv sector-output/ all_outputs/
	mv sector-output-final/ all_outputs/


runConso:
	@echo $(RED)"\n>>>Running Consumption analysis\n"$(RESET)
	hadoop jar EnedisByConsumption.jar EnedisByConsumption input conso-output
	@echo $(RED)"\n>>>End of Consumption analysis\n"$(RESET)
	mv conso-output/ all_outputs/
	mv conso-output-final/ all_outputs/

clean:
	rm -rf *.jar *_classes *-output *-output-final all_outputs

cleanOutput:
	rm -rf *-output *-output-final all_outputs