#!/bin/bash
(
SECONDS=0
start_time=$(date +%d-%b-%H_%M_%S)
echo "Job started at:" $start_time

#Installing dos2unix to remove whitespaces and extra characters from the script
sudo yum install dos2unix -y
#Removing whitespaces and extra characters from the script
dos2unix ./*.sh

#Parse SSM parameters for Netezza
aws ssm get-parameter --name "Netezza-cred" --with-decryption  --region 'us-east-1' > SSM_netezza.json
jq --raw-output .Parameter.Value SSM_netezza.json > SSM_Netezza_Parameter.json

#Parse SSM parameters for Redshift
aws ssm get-parameter --name "Redshift-cred" --with-decryption  --region 'us-east-1' > SSM_redshift.json
jq --raw-output .Parameter.Value SSM_redshift.json > SSM_Redshift_Parameter.json

#parameters
DBSERVER=$(jq --raw-output .Host SSM_Netezza_Parameter.json)
DBNAME=$(jq --raw-output .DatabaseName SSM_Netezza_Parameter.json)
DBUSER=$(jq --raw-output .UserName SSM_Netezza_Parameter.json)
DBPASSWORD=$(jq --raw-output .Password SSM_Netezza_Parameter.json)
HOST=$(jq --raw-output .ClusterEndpoint SSM_Redshift_Parameter.json)
REDSHIFTDB=$(jq --raw-output .DatabaseName SSM_Redshift_Parameter.json)
REDSHIFTUSER=$(jq --raw-output .UserName SSM_Redshift_Parameter.json)
REDSHIFTPWD=$(jq --raw-output .Password SSM_Redshift_Parameter.json)
JARLocation=${1} #s3 folder location s3://folder_name/
HiveDbLocation=${2} # s3 location
IAMRole=${3}


#copying all the jars of Netezza from s3 to sqoop directory
sudo aws s3 cp --recursive ${JARLocation} /usr/lib/sqoop/lib/;
#copying jar for accessing glue catalog
sudo cp /usr/lib/hive/auxlib/aws-glue-datacatalog-hive2-client.jar /usr/lib/sqoop/lib/;
export LIB_JARS=/usr/lib/sqoop/lib/;
#Suppresing accumulo warnings
sudo mkdir /var/lib/accumulo;ACCUMULO_HOME='/var/lib/accumulo';export ACCUMULO_HOME

#Drop and create hive database if already exists
sudo hive - S -e "DROP DATABASE IF EXISTS ${DBNAME} CASCADE;"
sudo hive - S -e "CREATE DATABASE IF NOT EXISTS ${DBNAME} LOCATION '${HiveDbLocation}${DBNAME}';"

#Function to iterate and import each table into hive table
function getCountForAllTables() {
sudo sqoop import-all-tables \
-libjars /usr/lib/sqoop/lib/ \
-fetch-size 10000 \
--connect jdbc:netezza://${DBSERVER}/${DBNAME} \
--driver org.netezza.Driver \
--connection-manager org.apache.sqoop.manager.GenericJdbcManager \
--direct \
--hive-import \
--create-hive-table \
--hive-database ${DBNAME} \
--num-mappers 1 \
--compress \
--compression-codec snappy \
--username ${DBUSER} \
--password ${DBPASSWORD}
}

# clear hdfs root directory
function hdfs_clear(){
sqoop eval  \
--connect jdbc:netezza://${DBSERVER}/${DBNAME}  \
--driver org.netezza.Driver \
--connection-manager org.apache.sqoop.manager.GenericJdbcManager \
--username admin \
--password password \
--query "select tablename from _v_table where database ='"${DBNAME^^}"'" |tee ./listofnetezzatable.txt
#cleaning of the file like removing header and footer
sed 's/ * //g;s/||/|/g;s/|||/|/g' ./listofnetezzatable.txt |tee ./Netezzatablelist.txt;sed -E 's/^.|.$//g' ./Netezzatablelist.txt |tee ./Netezzatablelist.txt;cat ./Netezzatablelist.txt| tail -n +3 |tee ./Netezzatablelist.txt;sed -i'' -e '1d' -e '$d' ./Netezzatablelist.txt ||tee ./Netezzatablelist.txt
cat ./Netezzatablelist.txt|while read LINE
do
        DBTABLE=${LINE}
        sudo hdfs dfs -rm -r /user/root/${DBTABLE}
        sudo rm -rf  /home/ec2-user/${DBTABLE}.java
done
}

#calling of functions
getCountForAllTables
echo "Tables imported successfully in S3 bucket"
hdfs_clear
echo "HDFS directory cleared successfully"


#########Access Redshift cluster on Ec2
#curl -O http://yum.postgresql.org/9.3/redhat/rhel-5-x86_64/pgdg-centos93-9.3-1.noarch.rpm
sudo aws s3 cp --recursive s3://aws-qinetic-emr/Redshift connector/pgdg-centos93-9.3-1.noarch.rpm
sudo yum -y install postgresql93 postgresql93-odbc

        DBNAME=${DBNAME}
        #########Create External database
        psql -t "host=${HOST} port=5439 dbname=${REDSHIFTDB} user=${REDSHIFTUSER} password=${REDSHIFTPWD}" -F  --no-align    -c  "CREATE EXTERNAL SCHEMA ${DBNAME}_rk FROM data catalog database '"${DBNAME}"' iam_role '${IAMRole}'  create external database if not exists;"
                psql -t "host=${HOST} port=5439 dbname=${REDSHIFTDB} user=${REDSHIFTUSER} password=${REDSHIFTPWD}" -F  --no-align    -c  "CREATE SCHEMA ${DBNAME}_emr;"
        ######### Get all the tables from external schema
        psql -t "host=${HOST} port=5439 dbname=${REDSHIFTDB} user=${REDSHIFTUSER} password=${REDSHIFTPWD}" -F  --no-align    -c  "SELECT tablename FROM SVV_EXTERNAL_TABLES WHERE schemaname='"${DBNAME}_rk"'" -o ./tablelist.txt
        sed -r '/^\s*$/d' ./tablelist.txt|tee ./tablelist.txt;

        #Iterate to each tables of Exernal database and create managed table from it
        cat ./tablelist.txt|while read LINE
          do
                EXTDBTABLE=${LINE}
                psql -t "host=${HOST} port=5439 dbname=${REDSHIFTDB} user=${REDSHIFTUSER} password=${REDSHIFTPWD}" -F  --no-align    -c  "CREATE TABLE ${DBNAME}_emr.${EXTDBTABLE} AS SELECT * FROM ${DBNAME}_rk.${EXTDBTABLE}"
                psql -t "host=${HOST} port=5439 dbname=${REDSHIFTDB} user=${REDSHIFTUSER} password=${REDSHIFTPWD}" -F  --no-align    -c  "SELECT COUNT(*) FROM ${DBNAME}_emr.${EXTDBTABLE}"
          done
          
          echo "Tables migrated successfully to Redshift"
#Delete all json file contains SSM parameters
rm SSM_netezza.json
rm SSM_Netezza_Parameter.json
rm SSM_redshift.json
rm SSM_Redshift_Parameter.json

duration=$SECONDS
echo "TOTAL TIME TAKEN : $(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
End_time=$(date +%d-%b-%H_%M_%S)
echo "Job completed at: $End_time"
) |sudo tee -a /var/lib/Netezza_to_Redshift_Import_All_Script.sh.log
sudo aws s3 cp /var/lib/Netezza_to_Redshift_Import_All_Script.sh.log s3://bucket_name/EMR_Netezza_Log/
