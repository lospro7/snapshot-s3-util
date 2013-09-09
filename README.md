# Building
```
mvn clean package
```

# Running

##To create a snapshot and immediately export to S3
```
hadoop jar target/snapshot-s3-util-1.0.0.jar com.imgur.backup.SnapshotS3Util --createExport --table <tableName> --bucketName <bucketName> --mappers <numMaps> --awsAccessKey <accessKey> --awsAccessSecret <accessSecret>
```

# Options
````
usage: BackupUtil -b <arg> -c | -e | -i | -x [-d <arg>]   -k <arg> [-m
       <arg>] [-n <arg>] [-p <arg>] -s <arg> [-t <arg>]
Backup utility for creating snapshots and exporting/importing to and from
S3
 -b,--bucketName <arg>        The S3 bucket name where snapshots are
                              stored
 -c,--create                  Create HBase snapshot
 -d,--hdfsPath <arg>          The snapshot directory in HDFS. Default is
                              '/hbase'
 -e,--export                  Export HBase snapshot to S3
 -i,--import                  Import HBase snapshot from S3. May need to
                              run as hbase user if importing into HBase
 -k,--awsAccessKey <arg>      The AWS access key
 -m,--mappers <arg>           The number of parallel copiers if copying
                              to/from S3. Default: 1
 -n,--snapshot <arg>          The snapshot name. Required for importing
                              from S3
 -p,--s3Path <arg>            The snapshot directory in S3. Default is
                              '/hbase'
 -s,--awsAccessSecret <arg>   The AWS access secret string
 -t,--table <arg>             The table name to create a snapshot from.
                              Required for creating a snapshot
 -x,--createExport            Create HBase snapshot AND export to S3
```
