# CsvFileSorter 
A practice project that means to design and produce a program that can sort larg csv files, with short amount of memory useg.

For the synchronized version please execute 
csvFileSort.jar

For the Asynchronous version please execute
acsvFileSort.jar

Example: java -jar csvFileSort.jar D:\CSVFileSortTesting\MOCK_DATA.csv -head -b5 -ki0

For running the program please run the jar files or at your own ide with these parameters:
java -jar csvFileSort.jar <File-path> options?
options:
	--help: Optional, See this help
	-head: Indicate file has header record at first row
	-nohead: Indicate file has no header record at first row
	-b<Size>: Optional, Indicate the max number of records to read at the same time. Defualt is 5. Example: -b6 will indicate that 6 records will be read at each interval
	-ki<index>: Optional, Indicate the key index in the csv record row. Default is 0(first column). Example: -ki11 will indicate the key is in the 12th column
	-t<type>: Inform of key value type for correct sorting. Mainly string or long.Defualt is Long. Suports:string|double|longExample: -tstring
  
  In a personal notice.
  I may have made many crimes against the java and programing nation.
  Please feel free to contribute.

