# Python teradata - load & unload

## This is a simple python source code to unload and load Teradata tables using text files
### Typical loads fail when the delimiters are present in datafields . using this you can specify your own delimiter and avoid failures. This is best suited for smaller data sets 

To unload:
  1) Insert the credentials into Copy2file.py
  2) Plugin the location where you want to place the unload file
  3) Execute the script
  
      python Copy2file.py
      
To Load:
  1) Insert Credentials into loadfile2tables.py and specify the output table name
  2) Specify the filename to be loaded
  3) Execute the script
  
      python loadfile2tables.py
      
To format Data:

  Table data types should be converted into Python readable types before written into file
  
  DataFormat.py contains an array which will be built based on the Columns of your input table.
  
  Add or modify the array mapper to your convenience. 

This is a quick beta version. Please leverage as needed
