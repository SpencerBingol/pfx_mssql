# pfx_mssql
PFX_MSSQL is a PitchFX parser for Microsoft SQL Server. It accepts beginning and ending dates of a date range
using the syntax -s <yyyy-mm-dd> -e <yyyy-mm-dd> , or just an argument that indicates to pull yesterday's games (-y).

Upon execution, the program determines a list of game directories on MLBAM's site, but removes any whose game ids already
exist in the SQL Server DB. With that list, it launches an HTTP Manager thread and a SQL Manager thread that share a queue.
The HTTP Manager creates a pool of threads that visits each directory in the list of games, pulling and preparing the 
appropriate files before adding them to the queue. The SQL Manager creates its own pool of threads that waits for data to 
enter the queue. When data is found, it is processed and inserted to the SQL database. The process repeats until every 
SQL manager thread finds the keyword 'quit' in the queue. 

This project also provides a simple SQL Server 2016 DB Schema to use for the inserts. The Import stored procedures are no
longer used in this program, but work if directly provided the text of the prepared XML. 

Anyone using this program accepts the responsiblity of not irresponsibly sending MLBAM an unwarranted number of HTTP requests.