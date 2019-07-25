# txt_2_pg
_python_  
Made for upload txt files or geojson quickly in a postgresql DB  
  
try directly the *.exe : ![here](https://github.com/benno-p/txt_2_pg/blob/master/dist/txt_2_pg.exe)  
  
With this freeware you can easily load many *.txt file in a postgrsql db with a beautifull interface ;) :  
![txt_2_pg](https://github.com/benno-p/txt_2_pg/blob/master/capture_txt_2_pg.png)
  
Your connections strings (user, port, dbname, password,schema ...) are store in a sqlite db in the user current repository  
_ex : C:/username/.txt_2_pg_  

For txt files :  
 - select the directory that contain txt files
 - choose a predefine format
   - text,integer,integer,...  
   - text,text,text,text...
   - other structure that you define (you must specify each columns for this case)
 - test the db connection
 - click on "Import" button

For geojson files :  
 - choose "geojson"  
 - test the db connection  
 - click on "JSON Import" button  
(the first entity in the geojson file must contain all properties)
