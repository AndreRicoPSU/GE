from xml.etree import ElementTree
import csv

# PARSE XML
xml = ElementTree.parse("hmdb_proteins.xml")

# CREATE CSV FILE
csvfile = open("hmdb_proteins.csv",'w',encoding='utf-8')
csvfile_writer = csv.writer(csvfile)

# ADD THE HEADER TO CSV FILE
csvfile_writer.writerow(["name","role","age"])

# FOR EACH EMPLOYEE
for employee in xml.findall("employee"):
    
    if(employee):
       
       # EXTRACT EMPLOYEE DETAILS  
      name = employee.find("name")
      role = employee.find("role")
      age = employee.find("age")
      csv_line = [name.text, role.text, age.text]

      # ADD A NEW ROW TO CSV FILE
      csvfile_writer.writerow(csv_line)
csvfile.close() 