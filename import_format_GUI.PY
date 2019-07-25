# -*- coding: utf-8 -*-
#    """ ----
#    
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

from PyQt4 import QtCore, QtGui
import re
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


class Dialog_liste_v2(object):
    def setupUi(self, Dialog, sender):
        Dialog.setObjectName(_fromUtf8("Dialog"))
        Dialog.resize(670, 670)
        self.sender = sender
        self.str_structure = ""
        self.dict_structure = {}
        self.groupBox = QtGui.QGroupBox(Dialog)
        self.groupBox.setGeometry(QtCore.QRect(10, 20, 600, 460))
        self.groupBox.setObjectName(_fromUtf8("groupBox"))
        self.gl_global = QtGui.QGridLayout(self.groupBox)
        self.gl_global.setMargin(10)
        self.gl_global.setObjectName(_fromUtf8("gl_global"))
        self.scrollArea = QtGui.QScrollArea(self.groupBox)
        self.scrollArea.setWidgetResizable(True)
        self.scrollArea.setObjectName(_fromUtf8("scrollArea"))
        self.sa_gl = QtGui.QGridLayout()
        self.sa_gl.setMargin(0)
        self.sa_gl.setObjectName(_fromUtf8("sa_gl"))
        self.widget_empty = QtGui.QWidget()
        self.widget_empty.setLayout(self.sa_gl)
       
        self.l_format = QtGui.QLabel(self.groupBox)
        self.l_format.setMinimumSize(QtCore.QSize(60, 20))
        self.l_format.setAlignment(QtCore.Qt.AlignCenter)
        self.l_format.setObjectName(_fromUtf8("l_format"))
        self.sa_gl.addWidget(self.l_format, 0, 0, 1, 1)
        self.l_value_format = QtGui.QLabel(self.groupBox)
        self.l_value_format.setAlignment(QtCore.Qt.AlignCenter)
        self.l_value_format.setObjectName(_fromUtf8("l_value_format"))
        self.sa_gl.addWidget(self.l_value_format, 0, 1, 1, 1)
        self.l_value_type = QtGui.QLabel(self.groupBox)
        self.l_value_type.setAlignment(QtCore.Qt.AlignCenter)
        self.l_value_type.setObjectName(_fromUtf8("l_value_type"))
        self.sa_gl.addWidget(self.l_value_type, 0, 2, 1, 1)
        
        self.scrollArea.setWidget(self.widget_empty)
        spacerItemVert = QtGui.QSpacerItem(10, 10, QtGui.QSizePolicy.Minimum, QtGui.QSizePolicy.Expanding)
        self.sa_gl.addItem(spacerItemVert, 150, 1, 1, 1)
        
        # INTO gl_global
        spacerItemVert_2 = QtGui.QSpacerItem(10, 10, QtGui.QSizePolicy.Minimum, QtGui.QSizePolicy.Expanding)
        self.gl_global.addItem(spacerItemVert_2,  1, 1, 1, 2)
        self.gl_global.addWidget(self.scrollArea, 0, 0, 4, 1)
        self.b_add = QtGui.QPushButton(self.groupBox)
        self.b_add.setObjectName(_fromUtf8("b_add"))
        self.gl_global.addWidget(self.b_add, 0, 1, 1, 1)
        self.b_del = QtGui.QPushButton(self.groupBox)
        self.b_del.setObjectName(_fromUtf8("b_del"))
        self.gl_global.addWidget(self.b_del, 0, 2, 1, 1)
        self.b_valide_modele = QtGui.QPushButton(self.groupBox)
        self.b_valide_modele.setObjectName(_fromUtf8("b_valide_modele"))
        self.gl_global.addWidget(self.b_valide_modele, 2, 1, 1, 2)
        self.b_annule_modele = QtGui.QPushButton(self.groupBox)
        self.b_annule_modele.setObjectName(_fromUtf8("b_annule_modele"))
        self.gl_global.addWidget(self.b_annule_modele, 3, 1, 1, 2)
        
        self.retranslateUi(Dialog)
        QtCore.QMetaObject.connectSlotsByName(Dialog)
        
        #SLOTs de la DIALOG
        self.b_add.connect(self.b_add, QtCore.SIGNAL("clicked()"),self.add_field)
        self.b_del.connect(self.b_del, QtCore.SIGNAL("clicked()"),self.del_field)
        self.init_field()
        self.b_valide_modele.connect(self.b_valide_modele, QtCore.SIGNAL("clicked()"),self.field_structure_func)
        self.b_valide_modele.clicked.connect(Dialog.accept)
        self.b_annule_modele.clicked.connect(Dialog.reject)

        Dialog.adjustSize()
        
    def init_field(self):
        """ Load a Field when initialize"""
        #  with an empty dictionnary
        self.index_line =1
        self.liste_line = [1]
        i = self.index_line
        label = QtGui.QLabel("l_"+str(i))
        label.setAlignment(QtCore.Qt.AlignCenter)
        ledit = QtGui.QLineEdit("le_"+str(i))
        cb = QtGui.QComboBox()
        cb.setObjectName("b_"+str(i))
        label.setText(str(i))
        ledit.setText("field_"+str(i))
        liste_type = [u"Integer",u"Float",u"text",u"Date",u"Geometry"]
        cb.addItems(liste_type)
        self.sa_gl.addWidget(label, i, 0, 1, 1)
        self.sa_gl.addWidget(ledit, i, 1, 1, 1)
        self.sa_gl.addWidget(cb, i, 2, 1, 1)
        # New SLOT 
        self.sa_gl.itemAtPosition(1,1).widget().connect(self.sa_gl.itemAtPosition(1,1).widget(), QtCore.SIGNAL("editingFinished()"),lambda who=str(self.sa_gl.itemAtPosition(1,0).widget().text()): self.verif_champ(who))
        #  with an non-empty dictionnary (specific format):
        
    def add_field(self):
        """ Add field function"""
        idx = self.index_line + 1
        label = QtGui.QLabel("l_"+str(idx))
        label.setAlignment(QtCore.Qt.AlignCenter)
        ledit = QtGui.QLineEdit("le_"+str(idx))
        cb = QtGui.QComboBox()
        cb.setObjectName("b_"+str(idx))
        label.setText(str(idx))
        ledit.setText("field_"+str(idx))
        liste_type = [u"Integer",u"Float",u"text",u"Date",u"Geometry"]
        cb.addItems(liste_type)
        self.sa_gl.addWidget(label, idx, 0, 1, 1)
        self.sa_gl.addWidget(ledit, idx, 1, 1, 1)
        self.sa_gl.addWidget(cb, idx, 2, 1, 1)
        # New SLOT 
        self.sa_gl.itemAtPosition(idx,1).widget().connect(self.sa_gl.itemAtPosition(idx,1).widget(), QtCore.SIGNAL("editingFinished()"),lambda who=str(self.sa_gl.itemAtPosition(idx,0).widget().text()): self.verif_champ(who))
        self.index_line += 1
        self.liste_line.append(self.index_line)
        
    def del_field(self):
        """ Delete field if countfield>1"""
        idx = self.index_line
        if idx > 1: 
            self.sa_gl.itemAtPosition(idx,0).widget().deleteLater()
            self.sa_gl.itemAtPosition(idx,1).widget().deleteLater()
            self.sa_gl.itemAtPosition(idx,2).widget().deleteLater()
            self.liste_line.remove(self.index_line)
            self.index_line += -1
            
    def field_structure_func(self):
        """ Function to Obtain field structure"""
        self.dict_structure = {}
        for i in self.liste_line:
            key = int(self.sa_gl.itemAtPosition(i,0).widget().text())
            name_field = self.sa_gl.itemAtPosition(i,1).widget().text()
            type_field = self.sa_gl.itemAtPosition(i,2).widget().currentText()
            self.dict_structure[key] = str(name_field)+"_|_"+str(type_field)
        self.sender.field_structure = self.dict_structure
        string = ""
        for k in self.dict_structure.keys():
            string += self.dict_structure[k].replace("_|_", " : ") + " | "
        self.sender.cb_format.addItem(string)
        self.sender.cb_format.setCurrentIndex(self.sender.cb_format.count()-1)
        self.sender.add_format_user()
        
    def verif_champ(self, value):
        """ Control and return a correct namefield with normal character"""
        idx = int(unicode(value))
        text = unicode(self.sa_gl.itemAtPosition(idx,1).widget().text())
        if text == "" :
            widget = QtGui.QWidget()
            QtGui.QMessageBox.critical(widget,"erreur","Nom de champ invalide : \n \"%s\" \n " % (text))
            self.sa_gl.itemAtPosition(idx,1).widget().setText("field_"+str(idx))
        else :
            result = re.sub(r"([^a-zA-Z0-9_])", r"_", unicode(self.sa_gl.itemAtPosition(idx,1).widget().text()) )
            self.sa_gl.itemAtPosition(idx,1).widget().setText(result.lower())
        
    def retranslateUi(self, Dialog):
        Dialog.setWindowTitle(_translate("Dialog", "Dialog", None))
        self.groupBox.setTitle(_translate("Dialog", "Format specific :", None))
        self.l_format.setText(_translate("Dialog", "Number ", None))
        self.l_value_format.setText(_translate("Dialog", "Name Field", None))
        self.l_value_type.setText(_translate("Dialog", "Type", None))
        self.b_add.setText(_translate("Dialog", "Add Field", None))
        self.b_del.setText(_translate("Dialog", "Delete Field", None))
        self.b_valide_modele.setText(_translate("Dialog", "Structure Validation", None))
        self.b_annule_modele.setText(_translate("Dialog", "Cancel", None))
        