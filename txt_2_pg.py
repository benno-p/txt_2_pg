# -*- coding: utf-8 -*-
#    """ ----
#    This file is part of Txt_2_pg.
#
#    Txt_2_pg is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Txt_2_pg is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Txt_2_pg.  If not, see <http://www.gnu.org/licenses/>
#    Author : Benoit Perceval, 2013 
#    """ ----
import sys, os, fnmatch, re, subprocess, json
from PyQt4 import QtGui, QtCore
import psycopg2
from import_GUI_PY import Ui_Dialog
from import_format_GUI import Dialog_liste_v2
import sqlite3
import io




try:
    _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
    def _fromUtf8(s):
        return s



try:
    _encoding = QtGui.QApplication.UnicodeUTF8
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig)


class appli_import(QtGui.QMainWindow, Ui_Dialog):
    # SIGNAUX
    tree_model_indexSig = QtCore.pyqtSignal()
    def __init__(self):
        QtGui.QMainWindow.__init__(self)
        Ui_Dialog.__init__(self)
        
        # Configure l'interface utilisateur.
        self.setupUi(self)
        
        #List des formats definis par l'utilisateur
        self.list_format_user = {}
        #self.read_list_format_user()
        
        #Dictionnaire des LineEdit de connexion
        self.dict_le_flag = {}
        self.dict_le_init = {}
        
        #Creation QStringList pour filtrer les fichiers *.txt
        self.stringList = QtCore.QStringList()
        self.stringList.append("*.txt")
        self.stringList.append("*.json")
        self.stringList.append("*.geojson")
        
        #Creation d'un dictionnaire stockant le format des tables + le delimiter
        self.field_structure = {}
        self.dict_delimiter = {}
        self.delimiter = ""
        for i in range(self.comboBox_3.count()):
            for j in self.comboBox_3.itemText(i):
                if i == 0 :
                    self.dict_delimiter[i] = "\t"
                else :
                    self.dict_delimiter[i] = str(j)
                    
        #Liste des ficiers (table) present dans le r�pertoire
        self.liste_file_table = []
        self.extension = ".txt"
        
        # connect les signaux a leurs slots
        self.tree_model_indexSig.connect(self.indexSlot)
        #Generation d'un FilesystemModele Tree et View
        model_TreeView = QtGui.QFileSystemModel()
        model_TreeView.setRootPath(QtCore.QDir.rootPath())
        model_ListView = QtGui.QFileSystemModel()
        model_ListView.setRootPath(QtCore.QDir.rootPath())
        # Create model for QTreeview and QListview
        self.treeView.setModel(model_TreeView)
        self.treeView.setRootIndex(model_TreeView.index(QtCore.QDir.rootPath()))
        self.listView_files.setModel(model_ListView)
        self.listView_files.setRootIndex(model_ListView.index(QtCore.QDir.rootPath()))
        #Create FilesystemModele to obtain QModelIndex used for QTreeview and QListview
        self.model_TreeView = QtGui.QFileSystemModel()
        self.model_TreeView.setRootPath(QtCore.QDir.rootPath())
        self.model_listview = QtGui.QFileSystemModel()
        self.model_listview.setRootPath(QtCore.QDir.rootPath())
        self.model_listview.setNameFilters(self.stringList)
        self.model_listview.setNameFilterDisables(False)
        
        # clicked.CONNECT
        self.treeView.clicked.connect(self.treeClicked)
        self.connect(self.b_parcourir, QtCore.SIGNAL("clicked()"),self.parcourir)
        self.connect(self.b_connexion, QtCore.SIGNAL("clicked()"),self.test_connect)
        self.connect(self.b_sql, QtCore.SIGNAL("clicked()"), self.open_scriptSQL_SQLite)
        #GEOJSON
        self.connect(self.b_sql_json, QtCore.SIGNAL("clicked()"), self.import_execute_psyco_SQLite_geojson)
        
        self.connect(self.b_import, QtCore.SIGNAL("clicked()"),self.import_execute_psyco_SQLite)
        
        self.connect(self.cb_list_connect, QtCore.SIGNAL("activated (int)"),self.load_param_connection)
        self.connect(self.cb_format, QtCore.SIGNAL("activated (int)"),self.load_format_user)
        #Apply two signals foreach QLineEdit
        for o in self.gridLayoutWidget.children():
            if type(o) == QtGui.QLineEdit:
                self.dict_le_flag[o.objectName()] = 0
                self.connect(o, QtCore.SIGNAL("selectionChanged()"), lambda who=o: self.erase_value(who))
                self.connect(o, QtCore.SIGNAL("editingFinished()"), lambda who=o: self.le_init_value(who))
        #QtCore.QObject.connect(self.rb_3,QtCore.SIGNAL("toggled(bool)"),self.load_format_spec)
        
        # rb check Not Usefull // after a clic on new structure button is better 
        #self.connect(self.rb_3,QtCore.SIGNAL("released()"),self.load_format_spec)
        self.connect(self.b_add_format,QtCore.SIGNAL("clicked()"),self.load_format_spec)
        
        self.connect(self.rb_3,QtCore.SIGNAL("released()"),lambda who=self.rb_3.text(): self.show_active_format(who))
        self.connect(self.rb_3,QtCore.SIGNAL("released()"),self.active_list_format)
        self.connect(self.rb_2,QtCore.SIGNAL("released()"),lambda who=self.rb_2.text(): self.show_active_format(who))
        self.connect(self.rb_1,QtCore.SIGNAL("released()"),lambda who=self.rb_1.text(): self.show_active_format(who))
        self.show_active_format(self.rb_1.text())
        self.connect(self.rb_4,QtCore.SIGNAL("released()"),lambda who=self.rb_4.text(): self.show_active_format(who))
        self.connect(self.b_save_connect,QtCore.SIGNAL("clicked()"),self.add_connection)
        self.connect(self.b_del_connection,QtCore.SIGNAL("clicked()"),self.del_connection)
        #self.connect(self.b_add_format,QtCore.SIGNAL("clicked()"),self.add_format_user)
        self.connect(self.b_del_format,QtCore.SIGNAL("clicked()"),self.del_format_user)
        
        #Path of SQLite
        directory = os.path.dirname(os.path.expanduser('~')+"\\.txt_2_pg\\")
        #print os.path.expanduser('~')+"\\.txt_2_pg\\"
        if not os.path.exists(directory):
            os.mkdir(directory)
        self.dbfile = os.path.expanduser('~')+"\\.txt_2_pg\\dbfile.sqlite"
        #self.dbfile = os.getcwd() + "\\dbfile.sqlite"
        self.initDB()
        
        #List des connections existantes
        self.list_connection = {}
        self.update_combo_box()
        
        #Charger les dict depuis BDD
        #INIT 
        self.init_dictionnary()
        
        
    def erase_value(self, le_object):
        """ Erase the default value when text change"""
        if self.dict_le_flag[le_object.objectName()] == 0 :
            self.dict_le_init[le_object.objectName()] = le_object.text()
            le_object.setText("")
            self.dict_le_flag[le_object.objectName()] = 1
        if le_object.text() == self.dict_le_init[le_object.objectName()]:
            le_object.setText("")
    
    def le_init_value(self, le_object):
        """ Give the default value when the lineEdit is Empty"""
        if le_object.text() == "":
            le_object.setText(self.dict_le_init[le_object.objectName()])
        
    def load_format_spec(self):
        """ Run a Dialog to define a specific structure"""        
        Dialog = QtGui.QDialog()
        di = Dialog_liste_v2()
        di.setupUi(Dialog, self)
        Dialog.exec_()
        #self.rb_3.isChecked()
        #self.add_format_user()
                
    def progress(self,data, *args):
        """ Run a Dialog to show copy status""" 
        it=iter(data)
        widget = QtGui.QProgressDialog(*args+(0,it.__length_hint__()))
        widget.setWindowTitle("Copying")
        widget.show()
        c=0
        for v in it:
                QtCore.QCoreApplication.instance().processEvents()
                if widget.wasCanceled():
                    raise StopIteration
                c+=1
                widget.setValue(c)
                yield(v)
        
    def treeClicked(self, checked=False):
        # On emet les signaux pour les utiliser
        self.tree_model_indexSig.emit()
        
    def indexSlot(self):
        modele_index_current = self.treeView.currentIndex()
        path_index_treeview = self.model_TreeView.filePath(modele_index_current)
        self.listView_files.setModel(self.model_listview)
        self.listView_files.setRootIndex(self.model_listview.index( path_index_treeview ))
        self.l_path.setText(path_index_treeview)
          
    def parcourir(self):
        """ Load a Browse Window to obtain the correct directory""" 
        #Fenetre_select_Dir = Fenetre(QtGui.QWidget())
        self.dial = QtGui.QFileDialog()
        path_directory = self.dial.getExistingDirectory(self, 'Select Files Location')
        self.l_path.setText(path_directory)
        self.listView_files.setModel(self.model_listview)
        self.listView_files.setRootIndex(self.model_listview.index(path_directory))
        
        #new model for treeView
        new_model = QtGui.QFileSystemModel()
        new_model.setRootPath(os.path.split(unicode(path_directory))[0])
        self.treeView.setModel(new_model)
        self.treeView.setRootIndex(new_model.index(os.path.split(unicode(path_directory))[0]))

    def test_connect(self):
        """ Test connection WITH psycopg2 """
        # Try to connect
        try:
            conn_string = "host='"+str(self.le_hote.text())+"' dbname='"+ str(self.le_database.text()) + "' user='"+ str(self.le_user.text()) + "' password='"+ str(self.le_mdp.text()) + "'"
            conn=psycopg2.connect(conn_string)
            QtGui.QMessageBox.information(self,"Information","connection to : \t\t\n '%s \n is valid " % (
            self.le_database.text()+"' on '"+self.le_hote.text()+"' " ))
            conn.close()
        except Exception:
            QtGui.QMessageBox.critical(self,"Error","Connect to : \t\t\n '%s' failed \n\n" % (
            self.le_database.text()))

    def test_exists_table(self, conXion, cursor,table_name):
        """ Test table exists WITH psycopg2 """
        con = conXion
        cur = cursor
        cur.execute("SELECT tablename FROM pg_tables where tablename = '"+ table_name +"' and schemaname ~* '"+ str(self.le_schema.text()) +"'   ;")
        if cur.fetchone() != None:
            return True
        else:
            return None
        QtGui.QMessageBox.critical(self,"Error","Connect to : \t\t\n '%s' failed \n\n" % (
        self.le_database.text()))
                    
    def import_execute_psyco_SQLite(self):
        """execute SQL script import, if the server is distant use copy STDIN  ||WITH psycopg2"""
        # Write SQL Command Without running notepad
        # Try to connect then execute queries      
        #Test connection to PostgreSQL
        test_connect_pg = False
        tables_property = []
        script_sql = ""
        try :
            #Open conneXion to Postgres local or Server    
            conn_string_PG = "host='"+str(self.le_hote.text())+"' dbname='"+ str(self.le_database.text()) + "' user='"+ str(self.le_user.text()) + "' password='"+ str(self.le_mdp.text()) + "'"
            conn_PG = psycopg2.connect(conn_string_PG)
            cur_PG = conn_PG.cursor()
            test_connect_pg = True
        except Exception, e :
            QtGui.QMessageBox.critical(self,"Error","Please check connection parameters." + str(e.pgerror))
        
        if test_connect_pg :
            try :
                #SEARCH OPTIONS FOR COPY STDIN IF SERVER iS DISTANT
                flag_distant = False
                if (unicode(self.le_hote.text()) != "127.0.0.1") and (unicode(self.le_hote.text()) != "localhost"):
                    flag_distant = True
                 
                #Connect SQLite for search * tables
                self.connSQLite = sqlite3.connect(self.dbfile)
                self.curSQLite = self.connSQLite.cursor()
                self.liste_file(self.l_path.text(), self.extension)
                self.script_SQL_dict_SQLite(False)
                
                self.connSQLite = sqlite3.connect(self.dbfile)
                self.curSQLite = self.connSQLite.cursor()
                self.curSQLite.execute("""Select table_id, table_name, table_schema, path_txt, drop_t,create_t, copy_t_fromtxt, copy_t_stdin   from liste_table;""")
                tables_property = self.curSQLite.fetchall()
                if tables_property != []:
                    for table_property in tables_property :
                        #if table exists ask drop or not
                        table_name_ok = table_property[1].replace("_txt","")
                        if self.test_exists_table(conn_PG, cur_PG,table_name_ok) :
                            answer = QtGui.QMessageBox.question(self.parent(),"",
                            str(self.le_schema.text()) + "." + str(table_name_ok) + """ already exists, would you like to drop and create this table ?
                            """, QtGui.QMessageBox.Ok, QtGui.QMessageBox.Cancel)
                            if answer == QtGui.QMessageBox.Ok :
                                #Execute normal copy or copy stdin
                                #drop
                                cur_PG.execute( self.uncomment_sql(table_property[4])  )
                                conn_PG.commit()
                                script_sql += self.uncomment_sql(table_property[4])
                                #create
                                cur_PG.execute( self.uncomment_sql(table_property[5])  )
                                conn_PG.commit()
                                script_sql += self.uncomment_sql(table_property[5]) 
                                #update
                                self.curSQLite.execute("UPDATE liste_table set bool_01_drop_exe = 0 where table_name = '"+ table_property[1] +"' ;")
                                self.connSQLite.commit()
                                
                                #PROGRESS DIALOG
                                count = sum(1 for _ in open(self.uncomment_sql(table_property[3]),'r'))
                                for x in self.progress(xrange(count),"Copy table : "+ str(self.le_schema.text()) + "." + table_name_ok, "Cancel"):
                                    continue


                                if flag_distant :
                                    copy_stdin_content = open(table_property[3], "r" )
                                    #SEARCH OPTIONS FOR COPY COMMAND (DELIMITER + HEADER)
                                    options = ""
                                    self.delimiter = self.dict_delimiter[self.comboBox_3.currentIndex()]
                                    options += " WITH DELIMITER AS '" + str(self.delimiter) + "' "
                                    if self.cb_entete.isChecked():
                                        options += " CSV HEADER "
                                    cur_PG.copy_expert("COPY " + str(self.le_schema.text()) + "." + table_name_ok + " FROM STDIN" + options +" " , copy_stdin_content)
                                    conn_PG.commit()
                                    script_sql += "COPY " + str(self.le_schema.text()) + "." + table_name_ok + " FROM STDIN" + options +""" ;\n\n"""
                                    script_sql += self.uncomment_sql(table_property[7])+""" \n\n"""
                                    copy_stdin_content.close()
                                else :
                                    cur_PG.execute( self.uncomment_sql(table_property[6])  )
                                    conn_PG.commit()
                                    script_sql += self.uncomment_sql(table_property[6])
                                    
                            else :
                                #Don't Execute normal copy or copy stdin --> NOT DROP = 1
                                self.curSQLite.execute("UPDATE liste_table set bool_01_drop_exe = 1 where table_name = '"+ table_property[1] +"' ;")
                                self.connSQLite.commit()
                                
                        #table don't exists --> create 
                        else :
                            # Table don't exists DROP THEN CREATE
                            cur_PG.execute( self.uncomment_sql(table_property[4])  )
                            conn_PG.commit()
                            script_sql += self.uncomment_sql(table_property[4])
                            cur_PG.execute( self.uncomment_sql(table_property[5])  )
                            conn_PG.commit()
                            script_sql += self.uncomment_sql(table_property[5])
                            self.curSQLite.execute("UPDATE liste_table set bool_01_drop_exe = 0 where table_name = '"+ table_property[1] +"' ;")
                            self.connSQLite.commit()
                            
                            #PROGRESS DIALOG
                            count = sum(1 for _ in open(self.uncomment_sql(table_property[3]),'r'))
                            for x in self.progress(xrange(count),"Copy table : "+self.le_schema.text() + "." + table_name_ok, "Cancel"):
                                    continue
                            
                            if flag_distant :
                                copy_stdin_content = open(table_property[3], "r" )
                                #SEARCH OPTIONS FOR COPY COMMAND (DELIMITER + HEADER)
                                options = ""
                                self.delimiter = self.dict_delimiter[self.comboBox_3.currentIndex()]
                                options += " WITH DELIMITER AS '" + str(self.delimiter) + "' "
                                if self.cb_entete.isChecked():
                                    options += " CSV HEADER "
                                cur_PG.copy_expert("COPY " +str(self.le_schema.text()) + "." + table_name_ok + " FROM STDIN" + options +" " , copy_stdin_content)
                                conn_PG.commit()
                                script_sql += "COPY " +str(self.le_schema.text()) + "." + table_name_ok + " FROM STDIN" + options +""" ; \n\n"""
                                script_sql += self.uncomment_sql(table_property[7])+""" \n\n"""
                                copy_stdin_content.close()
                            else :
                                cur_PG.execute( self.uncomment_sql(table_property[6])  )
                                conn_PG.commit()
                                script_sql += self.uncomment_sql(table_property[6])
                self.curSQLite.close()
                self.connSQLite.close()
                cur_PG.close()
                conn_PG.close()
                
                # Write Script SQL
                path = self.model_TreeView.filePath(self.treeView.currentIndex())
                script_file = open(path+ "/global.sql",'w')
                script_file.write(script_sql)
                script_file.close()
                
                if os.path.isfile(r"C:\Program Files (x86)\Notepad++\notepad++.exe"):
                    nf = r"C:\Program Files (x86)\Notepad++\notepad++.exe"
                    arg = unicode(path)+ "/"+"global.sql"
                elif os.path.isfile(r"C:\Program\Notepad++\notepad++.exe"):
                    nf = r"C:\Program\Notepad++\notepad++.exe"
                    arg = unicode(path)+ "/"+"global.sql"
                elif os.path.isfile(r"C:\Programmes\Notepad++\notepad++.exe"):
                    nf = r"C:\Programmes\Notepad++\notepad++.exe"
                    arg = unicode(path)+ "/"+"global.sql"
                elif os.path.isfile(r"C:\Program Files\Notepad++\notepad++.exe"):
                    nf = r"C:\Program Files\Notepad++\notepad++.exe"
                    arg = unicode(path)+ "/"+"global.sql"
                else :
                    nf = r"C:\Windows\System32\notepad.exe"
                    arg = unicode(path)+ "/" + "global.sql"
                subprocess.call([nf, arg])

                
            except Exception, e :
                QtGui.QMessageBox.information(self,"Information",str(e))
                pass
    
    def import_execute_psyco_SQLite_geojson(self):
        """execute SQL script import, if the server is distant use copy STDIN  ||WITH psycopg2"""
        # Write SQL Command Without running notepad
        # Try to connect then execute queries      
        #Test connection to PostgreSQL
        test_connect_pg = False
        try :
            #Open conneXion to Postgres local or Server    
            conn_string_PG = "host='"+str(self.le_hote.text())+"' dbname='"+ str(self.le_database.text()) + "' user='"+ str(self.le_user.text()) + "' password='"+ str(self.le_mdp.text()) + "'"
            conn_PG = psycopg2.connect(conn_string_PG)
            cur_PG = conn_PG.cursor()
            test_connect_pg = True
        except Exception, e :
            QtGui.QMessageBox.critical(self,"Error","Please check connection parameters." + str(e.pgerror))
        
        if test_connect_pg :
            try :
                str_complete =""
                for file_ in os.listdir(self.l_path.text()):
                    sql_script_create = ""
                    sql_script_insert = ""
                    if fnmatch.fnmatch(file_, "*"+"json"):
                        #with open(str(self.l_path.text())+"/"+file_,"r") as f:
                        with io.open(str(self.l_path.text())+"/"+file_, mode="r", encoding="utf-8-sig") as f:#, encoding="utf-8-sig"
                            
                            sql_script_drop = "DROP TABLE IF EXISTS "+str(self.le_schema.text())+"."+self.replace_car_spec(os.path.basename(f.name)).lower()+" ; "
                            sql_script_create = "CREATE TABLE "+str(self.le_schema.text())+"."+self.replace_car_spec(os.path.basename(f.name)).lower()+" ( "
                            sql_script_insert = "INSERT INTO "+str(self.le_schema.text())+"."+self.replace_car_spec(os.path.basename(f.name)).lower() + " ( "
                            sql_script_insert_adapt = "INSERT INTO "+str(self.le_schema.text())+"."+self.replace_car_spec(os.path.basename(f.name)).lower() + " ( "
                            jsonToPython = json.load(f)
                            #print jsonToPython['features'][0]['properties']
                            colonnes_p = jsonToPython['features'][0]['properties'].keys()
                            #LOOP ON First entity to read properties (that will be column name)
                            for key in colonnes_p:
                                sql_script_create += self.replace_car_spec(key).lower()+ " text ,"
                                sql_script_insert += self.replace_car_spec(key).lower()+ " ,"
                            sql_script_create = sql_script_create.rstrip(',')+" ,geom geometry );"
                            sql_script_insert = sql_script_insert.rstrip(',')+" ,geom ) VALUES (____);"
                            
                            #DROP TABLE IF EXISTS
                            cur_PG.execute( sql_script_drop  )
                            conn_PG.commit()
                            
                            #CREATE THE TABLE
                            cur_PG.execute( sql_script_create  )
                            conn_PG.commit()
                            
                            #PROGRESS DIALOG
                            f_count =0
                            f_count = sum(1 for _ in jsonToPython['features'])
                            for x in self.progress(xrange(f_count),"Load Features in Memory ", "Cancel"):
                                continue
                            
                            for feature in jsonToPython['features']:
                                #print feature['properties']
                                values_ =""
                                entity_ =""
                                sql_script_insert_final =""
                                sql_script_insert_adapt_final = ""
                                
                                #get properties fields 
                                column_entity = feature['properties'].keys()
                                sql_script_insert_adapt_c = sql_script_insert_adapt
                                for key in column_entity:
                                    sql_script_insert_adapt_c +=  self.replace_car_spec(key).lower()+ " ,"
                                sql_script_insert_adapt_c = sql_script_insert_adapt_c.rstrip(',')+" ,geom ) VALUES (____);"
                                
                                for properties_ in feature['properties']:
                                    # IF NULL (None in python) set null
                                    if feature['properties'][properties_] is not None :
                                        #values_ += repr(self.uncomment_sql_classique(self.make_unicode_(str(feature['properties'][properties_]))).replace("'", r"\""))+","
                                        #values_ += repr(self.uncomment_sql_classique(str(feature['properties'][properties_])).replace("'", r"\""))+","
                                        if isinstance(feature['properties'][properties_], unicode) :
                                            values_ += "'"+feature['properties'][properties_].replace("'", r"\"")+"'"+","
                                        else :
                                            values_ += "'"+str(feature['properties'][properties_]).decode("utf-8")+"'"+","
                                        
                                    else :
                                        values_ += u"null,"
                                entity_ = values_+" ST_GeomFromGeoJSON('"+json.dumps(feature['geometry'])+"') "
                                sql_script_insert_final = sql_script_insert.replace("____", entity_)
                                
                                sql_script_insert_adapt_final = sql_script_insert_adapt_c.replace("____", entity_)
                                
                                #INSERT INTO FOR EACH FEATURE
                                cur_PG.execute( sql_script_insert_adapt_final  )
                                conn_PG.commit()
                            str_complete +=  str(f_count)+" entities loaded from "+str(self.l_path.text())+"/"+file_+"""\n"""
                        f.close()
                cur_PG.close()
                conn_PG.close()
                QtGui.QMessageBox.information(self,"Information",str_complete)
                pass

            except Exception, e :
                QtGui.QMessageBox.information(self,"Information",str(e))
                pass
            
    def make_unicode_(self,input):
        if type(input) != unicode:
            input =  input.decode('utf-8')
            return input
        else:
            return input
                 
    def open_scriptSQL_SQLite(self):
        """execute SQL script import, if the server is distant use copy STDIN  ||WITH psycopg2"""
        #Write SQL Command and running notepad
        test_connect_pg = False
        tables_property = []
        script_sql = ""
        try :
            #Open conneXion to Postgres local or Server    
            conn_string_PG = "host='"+str(self.le_hote.text())+"' dbname='"+ str(self.le_database.text()) + "' user='"+ str(self.le_user.text()) + "' password='"+ str(self.le_mdp.text()) + "'"
            conn_PG = psycopg2.connect(conn_string_PG)
            cur_PG = conn_PG.cursor()
            test_connect_pg = True
        except Exception, e :
            QtGui.QMessageBox.critical(self,"Error","Please check connection parameters." + str(e.pgerror))
        
        if test_connect_pg :
            try :
                #SEARCH OPTIONS FOR COPY STDIN IF SERVER iS DISTANT
                flag_distant = False
                if (unicode(self.le_hote.text()) != "127.0.0.1") and (unicode(self.le_hote.text()) != "localhost"):
                    flag_distant = True

                #Connect SQLite for search * tables
                self.connSQLite = sqlite3.connect(self.dbfile)
                self.curSQLite = self.connSQLite.cursor()
                self.liste_file(self.l_path.text(), self.extension)
                self.script_SQL_dict_SQLite(False)
                
                self.connSQLite = sqlite3.connect(self.dbfile)
                self.curSQLite = self.connSQLite.cursor()
                self.curSQLite.execute("""Select table_id, table_name, table_schema, path_txt, drop_t,create_t, copy_t_fromtxt, copy_t_stdin  from liste_table;""")
                tables_property = self.curSQLite.fetchall()
                if tables_property != []:
                    for table_property in tables_property :

                        #if table exists ask drop or not
                        table_name_ok = table_property[1].replace("_txt","")
                        if self.test_exists_table(conn_PG, cur_PG,table_name_ok) :
                            print cur_PG
                            answer = QtGui.QMessageBox.question(self.parent(),"",
                            str(self.le_schema.text()) + "." + str(table_name_ok) + """ already exists, would you like to drop and create this table ?
                            """, QtGui.QMessageBox.Ok, QtGui.QMessageBox.Cancel)
                            if answer == QtGui.QMessageBox.Ok :
                                #Execute normal copy or copy stdin
                                #drop
                                cur_PG.execute( self.uncomment_sql(table_property[4])  )
                                conn_PG.commit()
                                script_sql += self.uncomment_sql(table_property[4])
                                #create
                                cur_PG.execute( self.uncomment_sql(table_property[5])  )
                                conn_PG.commit()
                                script_sql += self.uncomment_sql(table_property[5]) 
                                #update
                                self.curSQLite.execute("UPDATE liste_table set bool_01_drop_exe = 0 where table_name = '"+ table_property[1] +"' ;")
                                self.connSQLite.commit()
                                if flag_distant :
                                    copy_stdin_content = open(table_property[3], "r" )
                                    #SEARCH OPTIONS FOR COPY COMMAND (DELIMITER + HEADER)
                                    options = ""
                                    self.delimiter = self.dict_delimiter[self.comboBox_3.currentIndex()]
                                    options += " WITH DELIMITER AS '" + str(self.delimiter) + "' "
                                    if self.cb_entete.isChecked():
                                        options += " CSV HEADER "
                                    script_sql += "COPY " +str(self.le_schema.text()) + "." + table_name_ok + " FROM STDIN" + options +""" ; \n\n"""
                                    script_sql += self.uncomment_sql(table_property[7])+""" \n\n"""
                                    copy_stdin_content.close()
                                else :
                                    script_sql += self.uncomment_sql(table_property[6])
                            else :

                                #Don't Execute normal copy or copy stdin --> NOT DROP = 1
                                self.curSQLite.execute("UPDATE liste_table set bool_01_drop_exe = 1 where table_name = '"+ table_property[1] +"' ;")
                                self.connSQLite.commit()
                        #table don't exists --> create 
                        else :
                            # Table don't exists DROP THEN CREATE
                            script_sql += self.uncomment_sql(table_property[4])
                            script_sql += self.uncomment_sql(table_property[5])
                            self.curSQLite.execute("UPDATE liste_table set bool_01_drop_exe = 0 where table_name = '"+ table_property[1] +"' ;")
                            self.connSQLite.commit()
                            if flag_distant :
                                copy_stdin_content = open(table_property[3], "r" )
                                #SEARCH OPTIONS FOR COPY COMMAND (DELIMITER + HEADER)
                                options = ""
                                self.delimiter = self.dict_delimiter[self.comboBox_3.currentIndex()]
                                options += " WITH DELIMITER AS '" + str(self.delimiter) + "' "
                                if self.cb_entete.isChecked():
                                    options += " CSV HEADER "
                                script_sql += "COPY " +str(self.le_schema.text()) + "." + table_name_ok + " FROM STDIN" + options +""" ; \n\n"""
                                script_sql += self.uncomment_sql(table_property[7])+""" \n\n"""
                                copy_stdin_content.close()
                            else :
                                script_sql += self.uncomment_sql(table_property[6])
                self.curSQLite.close()
                self.connSQLite.close()
                cur_PG.close()
                conn_PG.close()

                # Write Script SQL
                path = self.model_TreeView.filePath(self.treeView.currentIndex())
                script_file = open(path+ "/global.sql",'w')
                script_file.write(script_sql)     
                script_file.close()
                
                #open in notepad if bool is TRUE
                if os.path.isfile(r"C:\Program Files (x86)\Notepad++\notepad++.exe"):
                    nf = r"C:\Program Files (x86)\Notepad++\notepad++.exe"
                    arg = unicode(path)+ "/"+"global.sql"
                elif os.path.isfile(r"C:\Program\Notepad++\notepad++.exe"):
                    nf = r"C:\Program\Notepad++\notepad++.exe"
                    arg = unicode(path)+ "/"+"global.sql"
                elif os.path.isfile(r"C:\Programmes\Notepad++\notepad++.exe"):
                    nf = r"C:\Programmes\Notepad++\notepad++.exe"
                    arg = unicode(path)+ "/"+"global.sql"
                elif os.path.isfile(r"C:\Program Files\Notepad++\notepad++.exe"):
                    nf = r"C:\Program Files\Notepad++\notepad++.exe"
                    arg = unicode(path)+ "/"+"global.sql"
                else :
                    nf = r"C:\Windows\System32\notepad.exe"
                    arg = unicode(path)+ "/" + "global.sql"
                subprocess.call([nf, arg])

            except Exception, e :
                QtGui.QMessageBox.information(self,"Information",str(e))
                pass

    def read_line_field_structure(self,line,delimiter):
        """ Read Field structure from file when default format selected """
        self.field_structure.clear()
        idx = 0
        if self.rb_2.isChecked():
            var_float = "text"
        else :
            var_float = "float"
        
        if self.cb_entete.isChecked():
            for nom_champ in line.split(delimiter):
                if idx == 0 :
                    self.field_structure[idx] = self.replace_car_spec(str(nom_champ)) + "_|_" +"text"
                else :
                    self.field_structure[idx] = self.replace_car_spec(str(nom_champ)) + "_|_" + var_float
                idx += 1
        else :
            for nom_champ in line.split(delimiter):
                if idx == 0 :
                    self.field_structure[idx] = "field_"+str(idx) + "_|_" +"text"
                else :
                    self.field_structure[idx] = "field_"+str(idx) + "_|_" + var_float
                idx += 1
        return self.field_structure
    
    def liste_file(self,path,extension):
        """ List of files with '*.txt' extension """
        del(self.liste_file_table)
        self.liste_file_table = []
        #For SQLite
        self.connSQLite = sqlite3.connect(self.dbfile)
        self.curSQLite = self.connSQLite.cursor()
        idx = 0
        self.curSQLite.execute("""DELETE FROM liste_table ;""")
        
        for file_ in os.listdir(path):
            if fnmatch.fnmatch(file_, "*"+ extension):
                self.liste_file_table.append(str(file_))
                #SQLITE ADD
                table_wo_ext = str(self.replace_car_spec(file_.replace("_txt","")))
                self.curSQLite.execute("""INSERT INTO liste_table(table_id, table_name, table_schema, path_txt, bool_01_drop_exe) values
                ( """ + str(idx) + """, '"""+ str(self.replace_car_spec(file_))+ """', 
                '"""+  str(self.le_schema.text()) +"""', '"""+ str(path+"/"+file_) +"""'
                , 0  );""")
                self.connSQLite.commit()
                idx += 1
        self.curSQLite.close()
    
  
    def script_SQL_dict_SQLite(self, bool):
        """ Script to create table (STRUCTURE) with the field_structure dictionnary
         all SQL command into global.txt """
        
        path = self.model_TreeView.filePath(self.treeView.currentIndex())
        self.liste_file(path, self.extension)
        self.delimiter = self.dict_delimiter[self.comboBox_3.currentIndex()]
        #Instance connection and cursor
        self.connSQLite = sqlite3.connect(self.dbfile)
        self.curSQLite = self.connSQLite.cursor()
        
        if self.treeView.selectedIndexes() == []:
            QtGui.QMessageBox.information(self,"Information","Select first a folder with text files")
        else :
            script_SQL_table = ""
            script_SQL_schema = "set client_encoding = '"+ self.comboBox.currentText() + "'; \n"
            #Foreach file use structure and write SQL command
            if self.rb_3.isChecked():
                #structure is defined
                for table in self.liste_file_table :
                    table_name = self.replace_car_spec(table.replace(self.extension,""))
                    table_name_w_ext = self.replace_car_spec(table)
                    dict_structure = self.field_structure
                    #Stockage script d'import foreach table
                    list_command = self.write_SQL_table_SQLite(table_name, dict_structure, path+"/"+table)
                    self.curSQLite.executescript("""
                                                UPDATE liste_table set sql_command = '_', sql_command_not_drop = '_', 
                                                drop_t = '""" + self.comment_sql(list_command[0]) + """', 
                                                create_t = '""" + self.comment_sql(list_command[1]) + """', 
                                                copy_t_fromtxt = '""" + self.comment_sql(list_command[2]) + """', 
                                                copy_t_stdin = '""" + self.comment_sql(list_command[3]) + """' 
                                                WHERE table_name = '"""+ str(table_name_w_ext) +"""'
                                                 ;""")
                    self.connSQLite.commit()
                    
            elif self.rb_2.isChecked():
                #structure not defined --> VAR VAR VAR and column number = alea
                for table in self.liste_file_table :
                    table_name = self.replace_car_spec(table.replace(self.extension,""))
                    table_name_w_ext = self.replace_car_spec(table)
                    line_1 = self.get_line1(path, table)
                    dict_structure = self.read_line_field_structure(line_1, self.delimiter)
                    #Stockage script d'import foreach table
                    list_command = self.write_SQL_table_SQLite(table_name, dict_structure, path+"/"+table)
                    self.curSQLite.executescript("""
                                                UPDATE liste_table set sql_command = '_', sql_command_not_drop = '_', 
                                                drop_t = '""" + self.comment_sql(list_command[0]) + """', 
                                                create_t = '""" + self.comment_sql(list_command[1]) + """', 
                                                copy_t_fromtxt = '""" + self.comment_sql(list_command[2]) + """', 
                                                copy_t_stdin = '""" + self.comment_sql(list_command[3]) + """' 
                                                WHERE table_name = '"""+ str(table_name_w_ext) +"""'
                                                 ;""")
                    self.connSQLite.commit()
            else :
                #structure not defined --> VAR FLOAT INT DATE, etc and column number = alea
                for table in self.liste_file_table :
                    table_name = self.replace_car_spec(table.replace(self.extension,""))
                    table_name_w_ext = self.replace_car_spec(table)
                    line_1 = self.get_line1(path, table)
                    dict_structure = self.read_line_field_structure(line_1, self.delimiter)
                    #Stockage script d'import foreach table
                    list_command = self.write_SQL_table_SQLite(table_name, dict_structure, path+"/"+table)
                    self.curSQLite.executescript("""
                                                UPDATE liste_table set sql_command = '_', sql_command_not_drop = '_', 
                                                drop_t = '""" + self.comment_sql(list_command[0]) + """', 
                                                create_t = '""" + self.comment_sql(list_command[1]) + """', 
                                                copy_t_fromtxt = '""" + self.comment_sql(list_command[2]) + """', 
                                                copy_t_stdin = '""" + self.comment_sql(list_command[3]) + """' 
                                                WHERE table_name = '"""+ str(table_name_w_ext) +"""'
                                                 ;""")
                    self.connSQLite.commit()
            
            self.connSQLite.commit()
            script_SQL = script_SQL_schema.replace("##","'") + script_SQL_table.replace("##","'")

            # Write Script SQL
            script_file = open(path+ "/global.sql",'w')
            script_file.write(script_SQL)     
            script_file.close()
            #Close connection+Cursor
            self.curSQLite.close()
            self.connSQLite.close()
            #open in notepad if bool is TRUE
            if bool :
                if os.path.isfile(r"C:\Program Files (x86)\Notepad++\notepad++.exe"):
                    nf = r"C:\Program Files (x86)\Notepad++\notepad++.exe"
                    arg = unicode(path)+ "/"+"global.sql"
                else :
                    nf = r"C:\Windows\System32\notepad.exe"
                    arg = path + "\\" + "global.sql"
                subprocess.call([nf, arg])
                
    def script_SQL_dict_SQLite_json(self, bool):
        """ Script to create table (STRUCTURE) with the field_structure dictionnary
         all SQL command into global.txt """
        
        path = self.model_TreeView.filePath(self.treeView.currentIndex())
        self.liste_file(path, "json")
        self.delimiter = self.dict_delimiter[self.comboBox_3.currentIndex()]
        #Instance connection and cursor
        self.connSQLite = sqlite3.connect(self.dbfile)
        self.curSQLite = self.connSQLite.cursor()
        
        if self.treeView.selectedIndexes() == []:
            QtGui.QMessageBox.information(self,"Information","Select first a folder with text files")
        else :
            script_SQL_table = ""
            script_SQL_schema = "set client_encoding = '"+ self.comboBox.currentText() + "'; \n"
            #Foreach file use structure and write SQL command
            if self.rb_3.isChecked():
                #structure is defined
                for table in self.liste_file_table :
                    table_name = self.replace_car_spec(table.replace("json",""))
                    table_name_w_ext = self.replace_car_spec(table)
                    dict_structure = self.field_structure
                    #Stockage script d'import foreach table
                    list_command = self.write_SQL_table_SQLite(table_name, dict_structure, path+"/"+table)
                    self.curSQLite.executescript("""
                                                UPDATE liste_table set sql_command = '_', sql_command_not_drop = '_', 
                                                drop_t = '""" + self.comment_sql(list_command[0]) + """', 
                                                create_t = '""" + self.comment_sql(list_command[1]) + """', 
                                                copy_t_fromtxt = '""" + self.comment_sql(list_command[2]) + """', 
                                                copy_t_stdin = '""" + self.comment_sql(list_command[3]) + """' 
                                                WHERE table_name = '"""+ str(table_name_w_ext) +"""'
                                                 ;""")
                    self.connSQLite.commit()
                    
            elif self.rb_2.isChecked():
                #structure not defined --> VAR VAR VAR and column number = alea
                for table in self.liste_file_table :
                    table_name = self.replace_car_spec(table.replace("json",""))
                    table_name_w_ext = self.replace_car_spec(table)
                    line_1 = self.get_line1(path, table)
                    dict_structure = self.read_line_field_structure(line_1, self.delimiter)
                    #Stockage script d'import foreach table
                    list_command = self.write_SQL_table_SQLite(table_name, dict_structure, path+"/"+table)
                    self.curSQLite.executescript("""
                                                UPDATE liste_table set sql_command = '_', sql_command_not_drop = '_', 
                                                drop_t = '""" + self.comment_sql(list_command[0]) + """', 
                                                create_t = '""" + self.comment_sql(list_command[1]) + """', 
                                                copy_t_fromtxt = '""" + self.comment_sql(list_command[2]) + """', 
                                                copy_t_stdin = '""" + self.comment_sql(list_command[3]) + """' 
                                                WHERE table_name = '"""+ str(table_name_w_ext) +"""'
                                                 ;""")
                    self.connSQLite.commit()
            else :
                #structure not defined --> VAR FLOAT INT DATE, etc and column number = alea
                for table in self.liste_file_table :
                    table_name = self.replace_car_spec(table.replace("json",""))
                    table_name_w_ext = self.replace_car_spec(table)
                    line_1 = self.get_line1(path, table)
                    dict_structure = self.read_line_field_structure(line_1, self.delimiter)
                    #Stockage script d'import foreach table
                    list_command = self.write_SQL_table_SQLite(table_name, dict_structure, path+"/"+table)
                    self.curSQLite.executescript("""
                                                UPDATE liste_table set sql_command = '_', sql_command_not_drop = '_', 
                                                drop_t = '""" + self.comment_sql(list_command[0]) + """', 
                                                create_t = '""" + self.comment_sql(list_command[1]) + """', 
                                                copy_t_fromtxt = '""" + self.comment_sql(list_command[2]) + """', 
                                                copy_t_stdin = '""" + self.comment_sql(list_command[3]) + """' 
                                                WHERE table_name = '"""+ str(table_name_w_ext) +"""'
                                                 ;""")
                    self.connSQLite.commit()
            
            self.connSQLite.commit()
            script_SQL = script_SQL_schema.replace("##","'") + script_SQL_table.replace("##","'")

            # Write Script SQL
            script_file = open(path+ "/global.sql",'w')
            script_file.write(script_SQL)     
            script_file.close()
            #Close connection+Cursor
            self.curSQLite.close()
            self.connSQLite.close()
            #open in notepad if bool is TRUE
            if bool :
                if os.path.isfile(r"C:\Program Files (x86)\Notepad++\notepad++.exe"):
                    nf = r"C:\Program Files (x86)\Notepad++\notepad++.exe"
                    arg = unicode(path)+ "/"+"global.sql"
                else :
                    nf = r"C:\Windows\System32\notepad.exe"
                    arg = path + "\\" + "global.sql"
                subprocess.call([nf, arg])
                         
    def write_SQL_table_SQLite(self,table_name,field_structure,path_file):
        """ To obtain the correct SQl Command for each table"""
        table_name_schema = self.le_schema.text()+'.'+table_name
        options = ""
        #SEARCH OPTIONS FOR COPY COMMAND (DELIMITER + HEADER)
        options += " WITH DELIMITER '" + self.delimiter + "' "
        if self.cb_entete.isChecked():
            options += " CSV HEADER "
        str_drop_t = " DROP TABLE IF EXISTS " + table_name_schema + " ;\n"
        str_create_t = "CREATE TABLE " + table_name_schema + " ( \n"
        for field in field_structure.keys() :
            str_create_t += field_structure[field].replace("_|_","  ").replace(":"," ")+ ",\n"
        str_create_t += "--andboucle"
        str_create_t = str_create_t.replace(",\n--andboucle", "); \n\n")
        str_copy_t_fromtxt = "COPY "+ table_name_schema + " FROM '" + path_file + "' " + options +" ;--\n\n"
        str_copy_t_stdin =  """/*----\n The server is distant, this script CANNOT import data\n"""
        str_copy_t_stdin += """Copy files on the server and execute copy from in PGADMIN III or with pgSQL like that :\n"""
        str_copy_t_stdin += "QUERY : COPY "+ table_name_schema + " FROM '/home/.../.../" + os.path.split(unicode(path_file))[1] + "' ;"
        str_copy_t_stdin += "\nclick on IMPORT DATA  will create your table(s) and execute the copy with another way\n----*/\n\n\n"
        return [ str_drop_t , str_create_t , str_copy_t_fromtxt , str_copy_t_stdin ]
           
    def get_line1(self,path,file_name_ext):
        """ get the first line of a text file if a standard format is selected"""
        fichier_lecture = open(path +"/"+ file_name_ext ,'r')
        return fichier_lecture.readline().rstrip('\n\r').replace('\"','') # Supprime les caractere " et fin de ligne
        fichier_lecture.close()
        
    def replace_car_spec(self,str_):
        """ Replace * specials characters"""
        return re.sub(r"([^a-zA-Z0-9_])", r"_", str(str_))

    def read_list_format_user(self):
        """ Reading the specific format load in the combobox """
        self.cb_format.clear()
        for format_key in self.list_format_user.keys():
            self.cb_format.addItem(self.list_format_user[format_key])
      
    def load_param_connection(self,idx):
        """ load the param connection from the SQLite DB """
        string_connection = self.cb_list_connect.currentText().replace(" || ", "_|_").split("_|_")
        #For SQLite 0
        value_pipe = self.cb_list_connect.currentText()
        self.connSQLite = sqlite3.connect(self.dbfile)
        self.curSQLite = self.connSQLite.cursor()
        self.curSQLite.execute("""select value from list_connection_dict 
        where value_pipe = '"""+ str(value_pipe) + """' ;
        """)
        value_ = self.curSQLite.fetchone()[0]
        self.curSQLite.executescript("""
        UPDATE list_connection_dict set bool_01 = 0;
        UPDATE list_connection_dict set bool_01 = 1 where 
        value = '""" + str(value_) + """' ;""" )
        self.connSQLite.commit()
        self.curSQLite.close()
        
        self.le_hote.setText(string_connection[0])
        self.le_user.setText(string_connection[1])
        self.le_database.setText(string_connection[2])
        
        # For SQLite1
        self.connSQLite = sqlite3.connect(self.dbfile)
        self.curSQLite = self.connSQLite.cursor()
        self.curSQLite.execute("""
        SELECT mdp from list_connection_dict  
        where 
        value = '""" + str(value_) + """' ; 
        """ )
        mdp =  str(self.curSQLite.fetchone()[0])
        self.le_mdp.setText(mdp)
        self.connSQLite.commit()
        self.curSQLite.close()
        self.cb_list_connect.setCurrentIndex(idx)

    def load_format_user(self,idx):
        """ load the user specific format from the SQLite DB """
        idx = 0
        for split in self.cb_format.currentText().split(" | "):
            if split != unicode(""):
                self.field_structure[idx] = split
                idx += 1
        #For SQLite
        self.connSQLite = sqlite3.connect(self.dbfile)
        self.curSQLite = self.connSQLite.cursor()
        self.curSQLite.execute("""select value,id_key from list_format_user_dict
        where value = '"""+ str(self.cb_format.currentText()) + """' ;
        """)
        data = self.curSQLite.fetchone()
        value_ = data[0]
        #int_ = data[1]
        self.curSQLite.executescript("""
        UPDATE list_format_user_dict set bool_01 = 0;
        UPDATE list_format_user_dict set bool_01 = 1 where 
        value = '""" + str(value_) + """' ;""" )
        self.connSQLite.commit()
        self.curSQLite.close()
        # UPDATE field_structure
        fields = ""
        self.field_structure.clear()
        list_field = value_.split(" | ")
        for idx in range(len(list_field)-1) :
            self.field_structure[idx] = list_field[idx]
            fields += list_field[idx] + " | "
        #self.update_dict_tables()
        self.l_value_selected_format.setText(fields)

    def crypt_passwd_list_connect(self,str_connect):
        str_ = ""
        pass_w = str_connect.split("_|_")[3]
        for car in  range(len(pass_w)) :
            str_ += "*"
            car = car
        new_str_connect = str_connect.replace(pass_w,str_)
        return new_str_connect
    
    def crypt_passwd_word(self,str_pass):
        str_ = ""
        for car in  range(len(str_pass)) :
            str_ += "*"
            #car = car
        return str_

    def comment_sql(self,str_command):
        return str("/*---- "+str_command.replace("'","##")+" ----*/")
    
    def uncomment_sql(self,str_command):
        return str(str_command.replace("##","'").replace("/*---- ","").replace(" ----*/", ""))
    def uncomment_sql_classique(self,str_command):
        return str(str_command.replace("'","\'"))
    
    def add_connection(self): 
        """ add connection into the SQLite DB"""         
        #For SQLite
        new_connect = self.le_hote.text() +" || "+self.le_user.text()+" || "+self.le_database.text()+" || "+self.le_mdp.text()
        jdx=0
        while jdx < (len(self.list_connection.items())+1):
            if (jdx in self.list_connection.keys()) == False:
                self.list_connection[jdx] = new_connect
                self.update_dict_tables()
                self.load_param_connection(jdx)
                jdx +=100
            else:
                jdx += 1

    def del_connection(self):
        """ delete connection into the SQLite DB"""
        #For SQLite
        self.connSQLite = sqlite3.connect(self.dbfile)
        self.curSQLite = self.connSQLite.cursor()
        self.curSQLite.execute(""" select value, id_key from list_connection_dict where bool_01 = 1""" )
        value_to_del = self.curSQLite.fetchone()[1]
        del self.list_connection[value_to_del]
        self.curSQLite.close()
        self.update_dict_tables()
        self.update_combo_box()
        self.load_param_connection(0)
        
    def add_format_user(self):
        """ add specific format into the SQLite DB"""
        #For SQLite
        new_format = self.cb_format.currentText()
        jdx=0
        while jdx < (len(self.list_format_user.items())+1):
            if (jdx in self.list_format_user.keys()) == False:
                self.list_format_user[jdx] = new_format
                self.update_dict_tables()
                self.load_format_user(jdx)
                jdx +=100
            else:
                jdx += 1
        self.show_active_format(self.rb_3.text())
                 
    def del_format_user(self):
        """ delete specific format into the SQLite DB"""
        #For SQLite
        self.connSQLite = sqlite3.connect(self.dbfile)
        self.curSQLite = self.connSQLite.cursor()
        self.curSQLite.execute(""" select value, id_key from list_format_user_dict where bool_01 = 1""" )
        value_to_del = self.curSQLite.fetchone()[1]
        del self.list_format_user[value_to_del]
        self.curSQLite.close()
        self.update_dict_tables()
        self.update_combo_box()
        self.load_format_user(0)

    def show_active_format(self, value):
        """ show active specific format """
        fields = ""
        if value != "Specific format :" :
            fields = value
            self.cb_format.setEnabled(False)
            self.b_add_format.setEnabled(False)
            self.b_del_format.setEnabled(False)
        else :
            if self.field_structure != [] :
                for keys in self.field_structure.keys():
                    fields += self.field_structure[keys].replace("_|_"," ") + " | "
            else :
                fields += "specific format not validated"
        self.l_value_selected_format.setText(fields)

    def active_list_format(self):
        self.cb_format.setEnabled(True)
        self.b_add_format.setEnabled(True)
        self.b_del_format.setEnabled(True)

    def update_dict_tables(self):
        """ DELETE * FROM TABLE AND RELOAD IT IN COMBOBOX """
        self.connSQLite = sqlite3.connect(self.dbfile)
        self.curSQLite = self.connSQLite.cursor()
        self.curSQLite.executescript("""
            DELETE FROM list_connection_dict;
            DELETE FROM list_format_user_dict;
            DELETE FROM field_structure_dict;
            """)
        self.connSQLite.commit()
        
        #MAJ list_connection table
        for key in self.list_connection.keys():
            e_split = self.list_connection[key].split(" || ")
            view_string = e_split[0]+" || "+e_split[1]+" || "+e_split[2]+  " || "+str(self.crypt_passwd_word(e_split[3]))
            self.curSQLite.executescript("""
            INSERT INTO list_connection_dict
            (id_key ,value ,bool_01 
            ,user ,mdp ,hote ,
            database ,port ,schema ,
            mdp_crypt, 
            value_pipe)
            VALUES
            ("""+ str(key) +""",'"""+ str(self.list_connection[key]) +"""',0
            ,'"""+ str(e_split[1])+"""', '"""+  str(e_split[3]) +"""','"""+  str(e_split[0]) +"""',
            '"""+  str(e_split[2]) +"""','"""+ str(self.le_port.text())+"""','"""+ str(self.le_schema.text()) +"""',
            '"""+  str(self.crypt_passwd_word(e_split[3])) +"""', 
            '"""+  str(view_string) + """'
            ); """)
            self.connSQLite.commit()
        #MAJ list_format_user table
        for key in self.list_format_user.keys():
            self.curSQLite.executescript("""
            INSERT INTO list_format_user_dict VALUES
            ("""+ str(key) +""",'"""+ str(self.list_format_user[key]) +"""', 0) ; """)
            self.connSQLite.commit()
        #MAJ list_list_connection table
        for key in self.field_structure.keys():
            self.curSQLite.executescript("""
            INSERT INTO field_structure_dict VALUES
            ("""+ str(key) +""",'"""+ str(self.field_structure[key]) +"""');
            """)
            self.connSQLite.commit()
        self.curSQLite.close()
        self.connSQLite.close()
        self.update_combo_box()
        
    def update_combo_box(self):
        self.connSQLite = sqlite3.connect(self.dbfile)
        self.curSQLite = self.connSQLite.cursor()
        #MAJ Combobox connection
        self.cb_list_connect.clear()
        self.curSQLite.execute("""select id_key,value_pipe from list_connection_dict;""")
        for each_connection in self.curSQLite.fetchall():
            self.cb_list_connect.addItem(each_connection[1],each_connection[0])
        self.connSQLite.commit()
        #MAJ Combobox Format
        self.cb_format.clear()
        self.curSQLite.execute("""select id_key,value from list_format_user_dict;""")
        for each_connection in self.curSQLite.fetchall():
            self.cb_format.addItem(each_connection[1],each_connection[0])
        self.connSQLite.commit()
        self.curSQLite.close()
        self.connSQLite.close()
        
    def init_dictionnary(self):
        """initializes dictionaries """
        self.connSQLite = sqlite3.connect(self.dbfile)
        self.curSQLite = self.connSQLite.cursor()
        #MAJ Combobox connection
        self.list_connection.clear()
        self.curSQLite.execute("""select id_key,value from list_connection_dict;""")
        for each_connection in self.curSQLite.fetchall():
            self.list_connection[each_connection[0]] = each_connection[1]
        self.connSQLite.commit()
        #MAJ Combobox Format
        self.list_format_user.clear()
        self.curSQLite.execute("""select id_key,value from list_format_user_dict;""")
        for each_connection in self.curSQLite.fetchall():
            self.list_format_user[each_connection[0]] = each_connection[1]
        self.connSQLite.commit()
        self.curSQLite.close()
        self.connSQLite.close()
        
    def initDB(self):
        """ BDD Init if not exists Create """
        if not os.path.exists(self.dbfile):
            self.connSQLite = sqlite3.connect(self.dbfile)
            self.curSQLite = self.connSQLite.cursor()
            # Create tables
            self.curSQLite.executescript("""
            CREATE TABLE IF NOT EXISTS liste_table
            (table_id text, table_name text, table_schema text, sql_command text,sql_command_not_drop text,path_txt text, drop_t text, create_t text, copy_t_fromtxt text, copy_t_stdin text, bool_01_drop_exe integer );
            
            CREATE TABLE IF NOT EXISTS structure_table
            (table_id text, field_name text, field_type text);
            
            CREATE TABLE IF NOT EXISTS user_connection
            (id INTEGER, user text, mdp text, hote text, database text, port text, schema text);
            
            CREATE TABLE IF NOT EXISTS user_format
            (id INTEGER, string text);
            
            CREATE TABLE IF NOT EXISTS user_format_active
            (id integer, field_name text, field_type text);
            
            CREATE TABLE IF NOT EXISTS list_connection_dict
            (id_key integer, 
            value text, 
            bool_01 integer, 
            user text, 
            mdp text, 
            hote text, 
            database text, 
            port text, 
            schema text,
            mdp_crypt text,
            value_pipe text
            );
            
            CREATE TABLE IF NOT EXISTS list_format_user_dict
            (id_key integer, value text, bool_01 integer);
            
            INSERT INTO list_format_user_dict(id_key , value , bool_01 ) values
            (100,'field_1 : Integer | field_2 : Integer | field_3 : Integer | field_4 : Integer | ',0);
            
            CREATE TABLE IF NOT EXISTS field_structure_dict
            (id_key integer, value text);
            
            CREATE TABLE IF NOT EXISTS sql_command
            (commands_id text,commands text, commands_not_drop);
            
            INSERT INTO sql_command(commands_id) values('schema');
            """)
            
            self.connSQLite.commit()
            self.curSQLite.close()
        else :
            self.connSQLite = sqlite3.connect(self.dbfile)
            self.curSQLite = self.connSQLite.cursor()
            self.curSQLite.execute("""
            DELETE FROM liste_table;
            """)
            self.connSQLite.commit()
            self.curSQLite.close()

if __name__ == "__main__":
    app = QtGui.QApplication(sys.argv)
    locale = QtCore.QLocale.system().name()
    translator = QtCore.QTranslator ()
    translator.load(QtCore.QString("qt_en") , QtCore.QLibraryInfo.location(QtCore.QLibraryInfo.TranslationsPath) )
    app.installTranslator(translator)
    window = appli_import()
    window.show()
    sys.exit(app.exec_())