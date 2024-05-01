# IMPORTING REQUIRED LIBRARIES

from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
load_dotenv(find_dotenv())



# 1. ESTABLISHING A CONNECTION TO MONGODB

password = os.environ.get("MONGODB_PWD")
connection_string  = f"mongodb+srv://c0892997:{password}@bam1043.tyuqtgm.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_string)


dbs = client.list_database_names()      # (This code LISTS ALL THE DATABASES available in the MongoDB instance.)
print(dbs)


     
# 2. SETTING UP A DATABASE:

database = client["Big_Data_Fundamentals"]      # (A DATABASE named "Big_Data_Fundamentals" is CREATED.)



# 3. SETTING UP A COLLECTION:

collection = database["Student_Details"]        # (A COLLECTION named "Student_Details" is CREATED within the "Big_Data_Fundamentals" database.)



# 4. INSERTING A DOCUMENT INTO THE COLLECTION:

def create_docs():
    first_name = ["Harmeet", "Mihir", "Tanvi", "Damini", "Amol"]
    middle_name = ['' , "Bhagvanbhai", "Dhirajbhai", '', '']
    last_name = ["Singh", "Chaudhary", "Patel", "Damini", "Vashistha"]
    student_id =["c0892997", "c0893689","c0907214", "c0901598", "c0894995"]

    docs = []       # (Empty List)

    for first_name, middle_name, last_name, student_id in zip(first_name, middle_name, last_name, student_id):
        doc = {"First Name": first_name, "Middle Name": middle_name, "Last Name": last_name, "Student ID": student_id}
        docs.append(doc)

    collection.insert_many(docs)        # ( This query INSERTS the student details into the "Student_Details" collection.)
        
#create_docs()



# 5. READING DOCUMENTS FROM THE COLLECTION:

def find_all_students():
    students = collection.find()        # (This method retrieves ALL the documents from the "Student_Details" collection)

    for Student in students:
        print(Student)

#find_all_students()



# 6. FINDING A SPECIFIC DOCUMENT FROM THE COLLECTION:

def find_student():
     first_name = input("Enter the first name: ")
     last_name = input("Enter the last name: ")
     name = collection.find_one({"First Name": first_name, "Last Name": last_name})     # (This method retrieves the CORRESPONDING document from the "Student_Details" collection.)
     print(name)

#find_student()



# 7. COUNTING THE NUMBER OF DOCUMENTS:

def count_all_students():
    count = collection.count_documents(filter={})       # (This method COUNTS the total number of documents in the "Student_Details" collection.)
    print("Total number of students: ",count)

#count_all_students()



# 8. UPDATING DOCUMENTS IN THE COLLECTION:

def update_student_details(student_id):
    from bson.objectid import ObjectId

    _id = ObjectId(student_id)

    updates = {
        "$set":{"Occupation": "Student"},       # (This operator SETS the "Occupation" field to "Student".)
        "$rename":{"First Name": "first_name", "Last Name": "last_name"}        #(This operator RENAMES the "First Name" and "Last Name" fields to "first_name" and "last_name", respectively.)
    }
    collection.update_one({"_id":_id},updates)

#update_student_details("647a957ff54b52131bc8ab66")



# 9. UPDATE A DOCUMENT BY REMOVING A FIELD:

def update_student_details(student_id):
    from bson.objectid import ObjectId

    _id = ObjectId(student_id)

    collection.update_one({"_id": _id}, {"$unset": {"Middle Name": ""}})      # (This operator UNSET or REMOVE a specific field, i.e. 'Middle Name' in a student's details based on their student id.)

#update_student_details("647aa03ec81be2b14c9be823")



# 10. REPLACE A DOCUMENT WITH A NEW ONE:

def replace_student_details(student_id):
    from bson.objectid import ObjectId

    _id = ObjectId(student_id)

    replaced_doc = {
        "first_name": "Meet", "last_name": "Bamrah", "Occupation": "Business Analyst"
    }

    collection.replace_one({"_id": _id}, replaced_doc)      # (This method REPLACE a student's details with a new values based on their student id.)

#replace_student_details("647aa03ec81be2b14c9be823")



# 11. DELETING DOCUMENTS FROM THE COLLECTION:

def delete_student_details(student_id):
    from bson.objectid import ObjectId

    _id = ObjectId(student_id)

    collection.delete_one({"_id": _id})     # (This method DELETES the document specified by the 'student_id' from the "Student_Details" collection.)

#delete_student_details("647a957ff54b52131bc8ab69")



# 12. DELETING ALL DOCUMENTS FROM THE COLLECTION:

def delete_student_details():
   collection.delete_many({})       # (This method DELETES ALL the documents from the "Student_Details" collection.)

#delete_student_details()



# 13. INITIALISING A DOCUMENT RELATIONSHIP.

address = {     # (Address Dictionary)
    "Unit/Apartment": 7258,
    "Street": "Dime Crescent",
    "City": "Mississauga",
    "Province": "Ontario",
    "Postal Code": "L5W 1K6",
    "Country": "Canada"
}

def student_address(student_id, address):       # (This function establish a document RELATIONSHIP.)
    from bson.objectid import ObjectId

    _id = ObjectId(student_id)

    collection.update_one({
        "_id": _id}, {"$addToSet": {'Addresses':address}        # (This code adds the 'address' dictionary as a sub-document to the specified student document using the "$addToSet" operator.)
    })

#student_address("647aa03ec81be2b14c9be823",address)