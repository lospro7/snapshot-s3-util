package com.imgur.backup;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create snapshots of tables and export/import to/from S3
 */
public class SnapshotS3Util extends Configured implements Tool
{
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotS3Util.class);
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmmss");

    private boolean createSnapshot = false;
    private boolean exportSnapshot = false;
    private boolean importSnapshot = false;

    private String snapshotName = null;
    private String tableName    = null;
    private String hdfsPath     = "/hbase";
    private long mappers        = 1;
    
    // S3 options
    private String accessKey    = null;
    private String accessSecret = null;
    private String bucketName   = null;
    private String s3Path       = "/hbase";

    /**
     * Init and run job
     * @param args command line options
     */
    @Override
    public int run(String[] args) throws Exception {
        try {
            getAndCheckArgs(args);
        } catch (Exception e) {
            System.err.println("Error parsing command line arguments: " + e.getMessage());
            printOptions();
            System.exit(-1);
        }
        
        String effectiveSnapshotName = getEffectiveSnapshotName();
        printInfo(effectiveSnapshotName);
        
        if (createSnapshot) {
            LOG.info("Creating snapshot...");
            
            if (createSnapshot(effectiveSnapshotName)) {
                LOG.info("Successfully created snapshot '{}'", effectiveSnapshotName);
            } else {
                LOG.error("Snapshot creation failed");
                return -1;
            }
        }
        
        if (exportSnapshot) {
            LOG.info("Exporting snapshot '{}' to S3...", effectiveSnapshotName);
            
            if (exportToS3(effectiveSnapshotName)) {
                LOG.info("Successfully exported snapshot '{}' to S3", effectiveSnapshotName);
            } else {
                LOG.error("Failed to export snapshot '{}' to S3", effectiveSnapshotName);
                return -1;
            }
        } else if (importSnapshot) {
        	LOG.info("Importing snapshot '{}' from S3...", effectiveSnapshotName);
        	
        	if (importFromS3(effectiveSnapshotName)) {
                LOG.info("Successfully imported snapshot '{}' from S3", effectiveSnapshotName);
            } else {
                LOG.error("Failed to import snapshot '{}' from S3", effectiveSnapshotName);
                return -1;
            }
        }

        LOG.info("Complete");
        return 0;
    }
    
    /**
     * Print info to log
     * @param effectiveSnapshotName
     */
    private void printInfo(String effectiveSnapshotName) {
    	LOG.info("HBase Snapshot S3 Util");
        LOG.info("--------------------------------------------------");
        LOG.info("Create snapshot : {}", createSnapshot);
        LOG.info("Export snapshot : {}", exportSnapshot);
        LOG.info("Import snapshot : {}", importSnapshot);
        LOG.info("Table name      : {}", tableName);
        LOG.info("Snapshot name   : {}", effectiveSnapshotName);
        LOG.info("Bucket name     : {}", bucketName);
        LOG.info("S3 Path         : {}", s3Path);
        LOG.info("HDFS Path       : {}", hdfsPath);
        LOG.info("Mappers         : {}", mappers);
        LOG.info("--------------------------------------------------");
	}

	/**
     * Synchronously create a snapshot of given table
     * @param tableName
     * @param snapshotName
     * @return
     */
    private boolean createSnapshot(String snapshotName) {
        boolean ret = false;
        HBaseAdmin admin = null;

        try {
            admin = new HBaseAdmin(getConf());
            admin.snapshot(snapshotName, tableName);
            ret = true;
        } catch (SnapshotCreationException e) {
            LOG.error("Failed to create snapshot", e);
        } catch (IllegalArgumentException e) {
            LOG.error("Snapshot request is invalid. Snapshot name: %s", snapshotName, e);
        } catch (Exception e) {
            LOG.error("An error occurred while attempting to create snapshot", e);
        }
        
        try {
            admin.close();
        } catch (Exception e) {}

        return ret;
    }
    
    /**
     * Export to S3
     * @param snapshotName
     * @return
     */
    private boolean exportToS3(String snapshotName) {
        int ret = -1;
        String url = getS3Url(true);
        String[] args = {
            "-snapshot",
            snapshotName,
            "-copy-to",
            url,
            "-mappers",
            Long.toString(mappers)
        };

        try {
            LOG.info("Destination: {}", url);
            ret = ToolRunner.run(getConf(), new ExportSnapshot(), args);
        } catch (Exception e) {
            LOG.error("Exception ocurred while exporting to S3", e);
        }

        return (ret == 0);
    }
    
    /**
     * Import snapshot from S3. Does some config tweaks to actually do an export from S3 to HDFS.
     * @param snapshotName
     * @return
     */
    private boolean importFromS3(String snapshotName) {
        int ret = -1;
        
        try {
	        Configuration config = HBaseConfiguration.create();
	        String s3Url = getS3Url(false);        
	        String hdfsUrl = config.get("fs.defaultFS");
	        
	        if (hdfsUrl == null) {
	        	hdfsUrl = config.get("fs.default.name");
	        	
	        	if (hdfsUrl == null) {
	        		throw new Exception("Could not determine current fs.defaultFS or fs.default.name");
	        	}
	        }
	        
	        hdfsUrl = hdfsUrl + hdfsPath;
	        String[] args = {
	            "-snapshot",
	            snapshotName,
	            "-copy-to",
	            hdfsUrl,
	            "-mappers",
	            Long.toString(mappers)
	        };

	        // Override dfs configuration to point to S3
            config.set("fs.default.name", "s3://" + bucketName);
            config.set("fs.defaultFS", "s3://" + bucketName);
            config.set("fs.s3.awsAccessKeyId", accessKey);
            config.set("fs.s3.awsSecretAccessKey", accessSecret);
            config.set("hbase.rootdir", s3Url);
            
            LOG.info("Copying from: {} to {}", s3Url, hdfsUrl);
            ret = ToolRunner.run(config, new ExportSnapshot(), args);
        } catch (Exception e) {
            LOG.error("Exception ocurred while exporting to S3", e);
        }

        return (ret == 0);
    }
    
    /**
     * Get the snapshot directory path in S3
     * @return the URL "s3://..."
     */
    private String getS3Url(boolean withCredentials) {
    	if (withCredentials) {
    		return "s3://" + accessKey + ":" + accessSecret + "@" + bucketName + s3Path;
    	}
    	
    	return "s3://" + bucketName + s3Path;
    }

    /**
     * Get the effective snapshot name
     * @return The effective snapshot name
     * @throws IllegalArgumentException
     */
    private String getEffectiveSnapshotName() throws IllegalArgumentException {
    	if (snapshotName != null) {
    		return snapshotName;
    	}
    	
        return tableName + "-snapshot-" + DATE_FORMAT.format(new Date());
    }

    /**
     * Get args 
     * @param args
     * @throws Exception
     */
    private void getAndCheckArgs(String[] args) throws Exception {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        cmd = parser.parse(getOptions(), args);
        
        for (Option option : cmd.getOptions()) {
            switch (option.getId()) {
            case 'c':
                createSnapshot = true;
                break;
            case 'x':
                createSnapshot = true;
                exportSnapshot = true;
                break;
            case 'e':
                exportSnapshot = true;
                break;
            case 'i':
                importSnapshot = true;
                break;
            case 't':
                tableName = option.getValue();
                break;
            case 'n':
                snapshotName = option.getValue();
                break;
            case 'k':
                accessKey = option.getValue();
                break;
            case 's':
                accessSecret = option.getValue();
                break;
            case 'b':
                bucketName = option.getValue();
                break;
            case 'p':
            	s3Path = option.getValue();
            	break;
            case 'd':
            	hdfsPath = option.getValue();
            	break;
            case 'm':
            	mappers = Long.parseLong(option.getValue());
            	break;
            default:
                throw new IllegalArgumentException("unexpected option " + option);
            }
        }
        
        if (createSnapshot && StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Need a table name");
        }
        
        if ((importSnapshot || (exportSnapshot && !createSnapshot)) && StringUtils.isEmpty(snapshotName)) {
            throw new IllegalArgumentException("Need a snapshot name");
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        int rc = ToolRunner.run(config, new SnapshotS3Util(), args);
        System.exit(rc);
    }
    
    /**
     * Returns the command-line options supported.
     * @return the command-line options
     */
    private static Options getOptions() {
        Option tableName = new Option("t", "table", true,
    		"The table name to create a snapshot from. Required for creating a snapshot");
        Option snapshotName = new Option("n", "snapshot", true,
    		"The snapshot name. Required for importing from S3");
        Option accessId  = new Option("k", "awsAccessKey", true,
    		"The AWS access key");
        Option accessSecret = new Option("s", "awsAccessSecret", true,
    		"The AWS access secret string");
        Option bucketName = new Option("b", "bucketName", true,
    		"The S3 bucket name where snapshots are stored");
        Option s3Path = new Option("p", "s3Path", true,
    		"The snapshot directory in S3. Default is '/hbase'");
        Option hdfsPath = new Option("d", "hdfsPath", true,
    		"The snapshot directory in HDFS. Default is '/hbase'");
        Option mappers = new Option("m", "mappers", true,
    		"The number of parallel copiers if copying to/from S3. Default: 1");
        
        tableName.setRequired(false);
        snapshotName.setRequired(false);
        accessId.setRequired(true);
        accessSecret.setRequired(true);
        bucketName.setRequired(true);
        s3Path.setRequired(false);
        hdfsPath.setRequired(false);
        mappers.setRequired(false);
        
        Option createSnapshot = new Option("c", "create", false,
    		"Create HBase snapshot");
        Option createExportSnapshot = new Option("x", "createExport", false,
    		"Create HBase snapshot AND export to S3");
        Option exportSnapshot = new Option("e", "export", false,
    		"Export HBase snapshot to S3");
        Option importSnapshot = new Option("i", "import", false,
    		"Import HBase snapshot from S3. May need to run as hbase user if importing into HBase");

        OptionGroup actions = new OptionGroup();
        actions.setRequired(true);
        actions.addOption(createSnapshot);
        actions.addOption(createExportSnapshot);
        actions.addOption(exportSnapshot);
        actions.addOption(importSnapshot);

        Options options = new Options();
        options.addOptionGroup(actions);
        options.addOption(tableName);
        options.addOption(snapshotName);
        options.addOption(accessId);
        options.addOption(accessSecret);
        options.addOption(bucketName);
        options.addOption(s3Path);
        options.addOption(hdfsPath);
        options.addOption(mappers);

        return options;
    }
    
    /**
     * Print the available options to the display.
     */
    private static void printOptions() {
        HelpFormatter formatter = new HelpFormatter();
        String header = "Backup utility for creating snapshots and exporting/importing to and from S3";
        formatter.printHelp("BackupUtil", header, getOptions(), "", true);
    }
}
