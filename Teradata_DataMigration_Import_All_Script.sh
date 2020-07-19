#!/bin/bash
(
SECONDS=0
start_time=$(date +%d-%b-%H_%M_%S)
echo "Job started at:" $start_time

#Installing dos2unix to remove whitespaces and extra characters from the script
sudo yum install dos2unix -y
#Removing whitespaces and extra characters from the script
dos2unix ./*.sh

#Parse SSM parameters for Teradata
aws ssm get-parameter --name "Teradata-cred" --with-decryption  --region 'us-east-1' > SSM_Teradata.json
jq --raw-output .Parameter.Value SSM_Teradata.json > SSM_Teradata_Parameter.json

#Parse SSM parameters for Redshift
aws ssm get-parameter --name "Redshift-cred" --with-decryption  --region 'us-east-1' > SSM_redshift.json
jq --raw-output .Parameter.Value SSM_redshift.json > SSM_Redshift_Parameter.json

DBSERVER=$(jq --raw-output .Host SSM_Teradata_Parameter.json)
DBUSER=$(jq --raw-output .UserName SSM_Teradata_Parameter.json)
DBPASSWORD=$(jq --raw-output .Password SSM_Teradata_Parameter.json)
HOST=$(jq --raw-output .ClusterEndpoint SSM_Redshift_Parameter.json)
REDSHIFTDB=$(jq --raw-output .DatabaseName SSM_Redshift_Parameter.json)
REDSHIFTUSER=$(jq --raw-output .UserName SSM_Redshift_Parameter.json)
REDSHIFTPWD=$(jq --raw-output .Password SSM_Redshift_Parameter.json)
HiveS3PATH=${1} #s3 folder location s3://folder_name/
JARLocation=${2} #s3 folder location s3://folder_name/
IAMRole=${3}
SgGroupId=${4}


#add private and public ip address of all task,core, and master ip dynamically in the security group
function dynamic_ip_add(){
#Fetch cluster id
cluster_id=$(sudo jq --raw-output .jobFlowId /mnt/var/lib/info/job-flow.json)
#copy instance metadata to json file
sudo aws emr list-instances --cluster-id $cluster_id --region us-east-1 > cluster_config.json
#get the public ip address of the instance
sudo jq --raw-output .Instances[].PrivateIpAddress cluster_config.json > ip_addr.txt
sudo jq --raw-output .Instances[].PublicIpAddress cluster_config.json >> ip_addr.txt
sed -i '/null/d' ip_addr.txt
awk '{ print $0 "/32" }' < ip_addr.txt > ipnew.txt
count=1
cat ./ipnew.txt|while read LINE
do
ip=${LINE}
#add Ip address to security group
sudo aws ec2 authorize-security-group-ingress --group-id ${SgGroupId} --ip-permissions IpProtocol=-1,FromPort=-1,ToPort=-1,IpRanges='[{CidrIp='"$ip"',Description="'EMR_Node$count'"}]' --region us-east-1
((count=count+1))
done
}

dynamic_ip_add

#copying all the jars of Teradata from s3 to sqoop directory
sudo aws s3 cp --recursive ${JARLocation} /usr/lib/sqoop/lib/;
#copying jar for accessing glue catalog
sudo cp /usr/lib/hive/auxlib/aws-glue-datacatalog-hive2-client.jar /usr/lib/sqoop/lib/;
export LIB_JARS=/usr/lib/sqoop/lib/;
#Suppresing accumulo warnings
sudo mkdir /var/lib/accumulo;ACCUMULO_HOME="/var/lib/accumulo";export ACCUMULO_HOME


# Fetch all the Databases from teradata and store the same databaselist.txt file
sqoop eval  \
--connect jdbc:teradata://${DBSERVER}/ \
--driver com.teradata.jdbc.TeraDriver \
--connection-manager org.apache.sqoop.manager.GenericJdbcManager \
--username ${DBUSER} \
--password ${DBPASSWORD} \
--query "SELECT DISTINCT DatabaseName FROM DBC.Databases WHERE OwnerName='"${DBUSER}"'" |tee ./databaselist.txt
sed 's/ * //g;s/||/|/g;s/|||/|/g' ./databaselist.txt|tee ./teradatadatabaselist.txt
sed -E 's/^.|.$//g' ./teradatadatabaselist.txt|tee ./teradatadatabaselist.txt
sed -i'' -e '1d' -e '$d' ./teradatadatabaselist.txt ||tee ./teradatadatabaselist.txt
cat ./teradatadatabaselist.txt| tail -n +3 |tee ./teradatadatabaselist.txt

#Function to iterate each datbase and import all the tables from database into hive table
function importAllTables() {
cat ./teradatadatabaselist.txt|while read LINE
do
        DBNAME=${LINE}
		#Drop hive database if already exists
		sudo hive - S -e "DROP DATABASE IF EXISTS ${DBNAME} CASCADE;"
		#Create hive database and store the same in s3
		sudo hive - S -e "CREATE DATABASE IF NOT EXISTS ${DBNAME} LOCATION '${HiveS3PATH}${DBNAME}';"
   
	   sudo sqoop import-all-tables \
		 -libjars $LIB_JARS \
		 -fetch-size 10000 \
		 --connect jdbc:teradata://${DBSERVER}/Database=${DBNAME} \
		 --driver com.teradata.jdbc.TeraDriver \
		 --fields-terminated-by ',' \
		 --connection-manager org.apache.sqoop.manager.GenericJdbcManager \
		 --direct \
		 --exclude-tables AlertRequest,MonitorRequest,Dyn_LockLog_xsd,Shredded_LockTbl,Shredded_TransTbl \
		 --hive-import \
		 --create-hive-table \
		 --hive-database ${DBNAME} \
		 --num-mappers 1 \
		 --compress \
		 --compression-codec snappy \
		 --username ${DBUSER} \
		 --password ${DBPASSWORD}
		 

# Get list of tables in from database
sqoop eval  \
--connect jdbc:teradata://${DBSERVER}/${DBNAME} \
--driver com.teradata.jdbc.TeraDriver \
--connection-manager org.apache.sqoop.manager.GenericJdbcManager \
--username ${DBUSER} \
--password ${DBPASSWORD} \
--query "SELECT DISTINCT TableName FROM dbc.tables WHERE tablekind = 'T' and databasename='"${DBNAME}"'" |tee ./listofteradatatable.txt
#cleaning of the file like removing header and footer
sed 's/ * //g;s/||/|/g;s/|||/|/g' ./listofteradatatable.txt |tee ./teradatatablelist.txt;sed -E 's/^.|.$//g' ./teradatatablelist.txt |tee ./teradatatablelist.txt;cat ./teradatatablelist.txt| tail -n +3 |tee ./teradatatablelist.txt;sed -i'' -e '1d' -e '$d' ./teradatatablelist.txt ||tee ./teradatatablelist.txt
cat ./teradatatablelist.txt|while read LINE
do
	DBTABLE=${LINE}
	sudo hdfs dfs -rm -r /user/root/${DBTABLE}
	sudo rm -rf  /home/ec2-user/${DBTABLE}.java
done


done
}

importAllTables
echo "Tables imported successfully to S3 bucket"

#Access Redshift cluster on Ec2
curl -O http://yum.postgresql.org/9.3/redhat/rhel-5-x86_64/pgdg-centos93-9.3-1.noarch.rpm
sudo yum -y install postgresql93 postgresql93-odbc


#Create external database by iterating all the hive databases
cat ./teradatadatabaselist.txt|while read LINE
do
        DBNAME=${LINE}
        #Create External database
        psql -t "host=${HOST} port=5439 dbname=${REDSHIFTDB} user=${REDSHIFTUSER} password=${REDSHIFTPWD}" -F  --no-align    -c  "CREATE EXTERNAL SCHEMA ${DBNAME}_ext FROM data catalog database '"${DBNAME}"' iam_role '${IAMRole}'  create external database if not exists;"
        psql -t "host=${HOST} port=5439 dbname=${REDSHIFTDB} user=${REDSHIFTUSER} password=${REDSHIFTPWD}" -F  --no-align    -c  "CREATE SCHEMA ${DBNAME}_mng;"
        #get all the tables from external schema
        psql -t "host=${HOST} port=5439 dbname=${REDSHIFTDB} user=${REDSHIFTUSER} password=${REDSHIFTPWD}" -F  --no-align    -c  "SELECT tablename FROM SVV_EXTERNAL_TABLES WHERE schemaname='"${DBNAME}_ext"'" -o ./tablelist.txt
        sed -r '/^\s*$/d' ./tablelist.txt|tee ./tablelist.txt;

        #Iterate to each tables of Exernal database and create managed table from it
        cat ./tablelist.txt|while read LINE
          do
                EXTDBTABLE=${LINE}
                psql -t "host=${HOST} port=5439 dbname=${REDSHIFTDB} user=${REDSHIFTUSER} password=${REDSHIFTPWD}" -F  --no-align    -c  "CREATE TABLE ${DBNAME}_mng.${EXTDBTABLE} AS SELECT * FROM ${DBNAME}_ext.${EXTDBTABLE}"
                psql -t "host=${HOST} port=5439 dbname=${REDSHIFTDB} user=${REDSHIFTUSER} password=${REDSHIFTPWD}" -F  --no-align    -c  "SELECT COUNT(*) FROM ${DBNAME}_mng.${EXTDBTABLE}"
          done
done
echo "Tables migrated successfully to Redshift"
#Delete all json file contains SSM parameters
rm SSM_Teradata.json
rm SSM_Teradata_Parameter.json
rm SSM_redshift.json
rm SSM_Redshift_Parameter.json

duration=$SECONDS
echo "TOTAL TIME TAKEN : $(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."

End_time=$(date +%d-%b-%H_%M_%S)
echo "Job completed at: $End_time"
) |sudo tee -a /var/lib/teradata_data_migration.log
sudo aws s3 cp /var/lib/teradata_data_migration.log s3://bucket_name/EMR_Teradata_Log/
