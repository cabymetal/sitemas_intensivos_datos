# Carlos Murillo
# script to load data to S3

#one step recommended to small files
curl -s https://www.datos.gov.co/api/views/8835-5baf/rows.csv | aws s3 cp - s3://rawcmurill5/covid_colombia/vacunacion/pruebas.csv

echo "loaded pruebas to s3"

# two step for larger files I dont use the expected size parameter to s3 cp because the file is not larger than 4GB
curl -O https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv
mv rows.csv casos_covid.csv
aws s3 cp casos_covid.csv s3://rawcmurill5/covid_colombia/casos/casos_covid.csv

echo "loaded casos to s3"
echo "presiona tecla para continuar"
PAUSE

rm -f casos_covid.csv
echo "deleted files"
echo "script successful"
PAUSE

# create cluster with termination on idle time
aws emr create-cluster --applications Name=Hadoop Name=Hive Name=Hue Name=JupyterHub Name=JupyterEnterpriseGateway Name=Zeppelin Name=Tez Name=Spark Name=Livy Name=HCatalog --ec2-attributes '{"KeyName":"pckeypair","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-057ea1b814b9e1482","EmrManagedSlaveSecurityGroup":"sg-0cebe96bbb159af9d","EmrManagedMasterSecurityGroup":"sg-0e1111570aa5f7fa9"}' --release-label emr-6.4.0 --log-uri 's3n://aws-logs-027146100852-us-east-1/elasticmapreduce/' --instance-groups '[{"InstanceCount":1,"BidPrice":"OnDemandPrice","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m4.xlarge","Name":"Master - 1"},{"InstanceCount":2,"BidPrice":"OnDemandPrice","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m4.xlarge","Name":"Core - 2"}]' --configurations '[{"Classification":"jupyter-s3-conf","Properties":{"s3.persistence.bucket":"cmurill5notebooks","s3.persistence.enabled":"true"}},{"Classification":"hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}},{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' --auto-scaling-role EMR_AutoScaling_DefaultRole --ebs-root-volume-size 10 --service-role EMR_DefaultRole --enable-debugging --auto-termination-policy '{"IdleTimeout":3600}' --name 'cmurill5clustercli' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1
