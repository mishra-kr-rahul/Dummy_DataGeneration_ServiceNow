import csv
import random
import datetime
import re

resultFile = 'output_test_data.csv'

######Pulls String from List in Python for better printing in csv
def listToString(s):
    # initialize an empty string
    str1 = ""
    # traverse in the string
    for ele in s:
        str1 += ele
    # return string
    return str1

#####Generates Random Dates and Time Between the period mention in start and end
def random_date_create(start, end):
  return start + datetime.timedelta(
      seconds=random.randint(0, int((end - start).total_seconds())))

start = datetime.datetime(2020, 1, 1)
end = datetime.datetime(2023, 6, 1)

def add_time_random_date_create(date_input):
    num = random.randint(1, 300)
    dt_formatting = datetime.datetime.strptime(date_input, '%Y-%m-%d %H:%M:%S')
    result_resolve_time = dt_formatting + datetime.timedelta( hours = num )
    print(str(result_resolve_time),",",str(num))
    #return str(result_resolve_time),",",str(num)

#########Pulls Random Names for current code from the list mentioned in csv
def extract_random_value_from_csv(filename):
    data = []
    with open(filename, "r") as name_csv_file:
        csv_reader_name = csv.reader(name_csv_file)
        for row in csv_reader_name:
            data.append(row)
    db_length = len(data)
    rand_name = random.randint(0, db_length - 1)
    chosen_name = data[rand_name][0]
    return str(chosen_name)

#########Pulls random Error details of csv shared which are linked to create dataset
def extract_random_value_from_spark_csv(filename):
    data1 = []
    with open(filename, "r") as name_csv_file:
        csv_reader_name = csv.reader(name_csv_file)
        for row in csv_reader_name:
            data1.append(row)
    db_length1 = len(data1)
    rand_name1 = random.randint(0, db_length1 - 1)
    chosen_name1 = data1[rand_name1]
    final_output= [ "".join(chosen_name1[0]),"".join(chosen_name1[1]),"".join(chosen_name1[2]) ]
    return final_output

state_tkt = ['In-Progress','New','On-Hold','Closed','Resolved']
assignment_groups = [ 'HADOOP_ENGG' , 'HADOOP_SUPPORT_L1' , 'HADOOP_SUPPORT_L2' , 'HADOOP_SUPPORT_L3' ]
assignment_groups_list1 = [ 'SRE_TEAM' ]
priority_tkt = [ 'Critical' , 'High' , 'Moderate' , 'Low' ]
environment_list = ['DEV','UAT','SIT','PROD']

#######Categories of users groups and users are seperate for resolution of particular ticket
HADOOP_ENGG_grp_members =['Chloe Anderson','Landin Vaughn','Pamela Hancock','Yurem Montes','Seth Holder']
HADOOP_SUPPORT_L1_grp_members=['Jaquan Mcclure','Shirley Randolph','Savanah Ho','Keaton Forbes','Nyla Reid','Lacey Cole','Eliana Matthews','Edwin Brennan','Giovanna Henson','Marianna Stafford','Rosemary Bell','Esmeralda Maldonado','Alyvia Baker','Alonzo Berger','Kyra Velazquez']
HADOOP_SUPPORT_L2_grp_members=['Winston Schultz','Kiera Gallegos','Colton Robertson','Rashad Allen','Damari Mcgrath','Colt Chandler','Alana Hughes','Kenny Kennedy','Zayne Gonzales','Isabella Sheppard','Sullivan Preston','Edwin Ali']
HADOOP_SUPPORT_L3_grp_members=['Laci Acosta','Uriel Howell','Ariana Avila','Eva Underwood','Odin Potter','Sandra Myers','Guadalupe Mcneil','Kaydence Luna','Iyana Stein','Dale Oliver']
SRE_TEAM_grp_members=['Pranav Lucas','Kenna Porter','Adelaide Salinas','Mia Mueller','Braiden Cook']

def check_group_and_members(assiged_group):

    if assiged_group == 'HADOOP_ENGG':
        ENGG_TEAM_MEMBER = random.choice(HADOOP_ENGG_grp_members)
        return ENGG_TEAM_MEMBER
    elif assiged_group == "HADOOP_SUPPORT_L1":
        HADOOP_L1_TEAM_MEMBER=random.choice(HADOOP_SUPPORT_L1_grp_members)
        return HADOOP_L1_TEAM_MEMBER
    elif assiged_group == "HADOOP_SUPPORT_L2":
        HADOOP_L2_TEAM_MEMBER=random.choice(HADOOP_SUPPORT_L2_grp_members)
        return HADOOP_L2_TEAM_MEMBER
    elif assiged_group == "HADOOP_SUPPORT_L3":
        HADOOP_L3_TEAM_MEMBER=random.choice(HADOOP_SUPPORT_L3_grp_members)
        return HADOOP_L3_TEAM_MEMBER
    elif assiged_group == "SRE_TEAM":
        SRE_TEAM_MEMBER=random.choice(SRE_TEAM_grp_members)
        return SRE_TEAM_MEMBER
    else:
        print("Unknown Team entered. Please check the name of team")
#####Show SLA MET/NOT MET based on the calculation
####Mentioned below is the SLA Agreed with business team for each kind of ticket criticality in hours
SLA_LIMIT_1Critical='6'
SLA_LIMIT_2High='24'
SLA_LIMIT_3Moderate='120'
SLA_LIMIT_4Low='168'

#####The code generates the INC000 Sequence for the data generation.
numbers = range(10001, 50001)
for n in numbers:
    val_inc = (str("INC000")+str(n))
    random_date_val = random_date_create(start, end)
    random_date_val_1 = random_date_create(start, end)
    grp_name=random.choice(assignment_groups)
    print(str.strip(val_inc),",",str.strip(extract_random_value_from_csv('Name_List.csv')),",",str.strip(extract_random_value_from_spark_csv('Error_Desc.csv')),",",random_date_val,",",add_time_random_date_create(str(random_date_val)),",",random.choice(state_tkt),",",grp_name,",",random.choice(priority_tkt),",",random.choice(environment_list),",",check_group_and_members(grp_name))
    data = [ (str.strip(val_inc),str.strip(extract_random_value_from_csv('Name_List.csv')),extract_random_value_from_spark_csv('Error_Desc.csv')[0],extract_random_value_from_spark_csv('Error_Desc.csv')[1],extract_random_value_from_spark_csv('Error_Desc.csv')[2],random_date_val,add_time_random_date_create(str(random_date_val)),random.choice(state_tkt),grp_name,random.choice(priority_tkt),random.choice(environment_list),check_group_and_members(grp_name)) ]
    f = open(resultFile, "a")
    writer = csv.writer(f)
    writer.writerows(data)
    f.close()

#####Created this code as the final O/P above had blank line printed post the actual line , so removed the blank line in this and trying to add header if possible
out_fnam = 'Output_file_random_data_generator.csv'
with open( resultFile, newline='') as in_file:
    with open(out_fnam, 'w', newline='') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(['incidents', 'RequestedBy', 'Description', 'Category', 'Solution_Code', 'Start_Date',
                        'End_Date', 'Duration' ,'Status', 'Assignment_Group', 'Ticket_Criticality', 'Env', 'SolvedBy'])
        #Header_file
        for row in csv.reader(in_file):
            if row:
                writer.writerow(row)
