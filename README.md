# CsvFileSorter 
A practice project that means to design and produce a program that can sort larg csv files, with shortest amount of memory useg.
The program must not exceade the amount of records read at the same time and must keep the storing of key's in memory at the minimum, about double the times of read records.

This program is intended to run by a O(logn) complexity, unfortunently for me my AVL implrmentation is too buggy to publish so at the worst case it runs in an O(n)* complexity.

Reading the file twice takes n lines o(1) once at the begining and second time at the end.
Sorting by batches takes n/batch O(logn) times by using a priority indexing algorithm.
Sorting by a binary sorted tree on a low sized index node that contains the minimum of data to run is n/batch O(logn)*.
The "Secound time" is done for butting the lines each in its own batch.
The "Third time" is done form the batches at the sorted order to create the final sorted file.

*worst case

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

