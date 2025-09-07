import { S3Client, ListBucketsCommand, ListObjectsV2Command, HeadObjectCommand, RestoreObjectCommand } from '@aws-sdk/client-s3';
import { readFileSync } from 'fs';
import { join } from 'path';

interface AWSConfig {
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
}

interface S3Bucket {
  name: string;
  creationDate: string;
  size: number;
  sizeFormatted: string;
}

interface S3Object {
  key: string;
  size: number;
  storageClass: string;
  lastModified: string;
}

interface RestoreOperation {
  objects: S3Object[];
  totalSize: number;
  totalSizeFormatted: string;
}

interface RestoreStatus {
  key: string;
  storageClass: string;
  restoreStatus: 'in-progress' | 'completed' | 'not-requested';
  restoreExpiryDate?: string;
  size: number;
}

interface StatusSummary {
  bucketName: string;
  glacierObjects: S3Object[];
  inProgress: RestoreStatus[];
  completed: RestoreStatus[];
  notRequested: RestoreStatus[];
  totalSizeInProgress: number;
  totalSizeCompleted: number;
}

class AWSGlacierTool {
  private s3Client: S3Client;

  constructor() {
    // Load AWS config from ~/.aws/credentials
    try {
      const credentialsFile = join(process.env.HOME || '', '.aws', 'credentials');
      const credentialsData = readFileSync(credentialsFile, 'utf-8');

      const config: AWSConfig = {
        accessKeyId: this.getConfigValue(credentialsData, 'aws_access_key_id'),
        secretAccessKey: this.getConfigValue(credentialsData, 'aws_secret_access_key'),
        region: 'us-east-1' // Default region, can be overridden by AWS_REGION env var
      };

      // Check for region in ~/.aws/config
      try {
        const configFile = join(process.env.HOME || '', '.aws', 'config');
        const configData = readFileSync(configFile, 'utf-8');
        const regionFromConfig = this.getConfigValue(configData, 'region');
        if (regionFromConfig) {
          config.region = regionFromConfig;
        }
      } catch {
        // Config file doesn't exist, use default region
      }

      // Allow environment variable to override region
      if (process.env.AWS_REGION) {
        config.region = process.env.AWS_REGION;
      }

      if (!config.accessKeyId || !config.secretAccessKey) {
        throw new Error('Missing AWS credentials in ~/.aws/credentials file');
      }

      // Initialize S3 client
      this.s3Client = new S3Client({
        region: config.region,
        credentials: {
          accessKeyId: config.accessKeyId,
          secretAccessKey: config.secretAccessKey
        }
      });
    } catch (error) {
      throw new Error(`Could not load AWS credentials: ${error instanceof Error ? error.message : error}`);
    }
  }

  /**
   * Format bytes to human readable format
   */
  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';

    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));

    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  /**
   * Get the size of a specific S3 bucket using AWS SDK
   */
  private async getBucketSize(bucketName: string): Promise<number> {
    try {
      let totalSize = 0;
      let continuationToken: string | undefined;

      do {
        const command = new ListObjectsV2Command({
          Bucket: bucketName,
          ContinuationToken: continuationToken
        });

        const response = await this.s3Client.send(command);

        if (response.Contents) {
          for (const obj of response.Contents) {
            totalSize += obj.Size || 0;
          }
        }

        continuationToken = response.NextContinuationToken;
      } while (continuationToken);

      return totalSize;
    } catch (error) {
      console.warn(`Warning: Could not get size for bucket ${bucketName}: ${error}`);
      return 0;
    }
  }

  /**
   * Get all objects in a bucket with their storage class information using AWS SDK
   */
  private async getBucketObjects(bucketName: string): Promise<S3Object[]> {
    console.log(`Scanning bucket "${bucketName}" for objects...`);

    const objects: S3Object[] = [];
    let continuationToken: string | undefined;

    do {
      try {
        const command = new ListObjectsV2Command({
          Bucket: bucketName,
          ContinuationToken: continuationToken
        });

        const response = await this.s3Client.send(command);

        if (response.Contents) {
          for (const obj of response.Contents) {
            objects.push({
              key: obj.Key || '',
              size: obj.Size || 0,
              storageClass: obj.StorageClass || 'STANDARD',
              lastModified: obj.LastModified?.toISOString() || ''
            });
          }
        }

        continuationToken = response.NextContinuationToken;
      } catch (error) {
        throw new Error(`Failed to list objects in bucket ${bucketName}: ${error}`);
      }
    } while (continuationToken);

    console.log(`Found ${objects.length} objects in bucket "${bucketName}"`);
    return objects;
  }

  /**
   * Filter objects that need to be restored from Glacier storage classes
   */
  private filterGlacierObjects(objects: S3Object[]): S3Object[] {
    const glacierStorageClasses = [
      'GLACIER',
      'DEEP_ARCHIVE',
      'GLACIER_IR' // Glacier Instant Retrieval
    ];

    return objects.filter(obj =>
      glacierStorageClasses.includes(obj.storageClass)
    );
  }

  /**
   * Perform dry run analysis of what would be restored
   */
  public async dryRunRestore(bucketName: string): Promise<RestoreOperation> {
    console.log(`\nPerforming dry-run analysis for bucket: ${bucketName}`);
    console.log('=' .repeat(60));

    // Get all objects in the bucket
    const allObjects = await this.getBucketObjects(bucketName);

    if (allObjects.length === 0) {
      console.log(`No objects found in bucket "${bucketName}"`);
      return {
        objects: [],
        totalSize: 0,
        totalSizeFormatted: '0 B'
      };
    }

    // Filter objects that are in Glacier storage classes
    const glacierObjects = this.filterGlacierObjects(allObjects);

    // Calculate total size
    const totalSize = glacierObjects.reduce((sum, obj) => sum + obj.size, 0);

    console.log(`\nStorage class distribution:`);
    const storageClassCounts = new Map<string, number>();
    const storageClassSizes = new Map<string, number>();

    allObjects.forEach(obj => {
      const storageClass = obj.storageClass;
      storageClassCounts.set(storageClass, (storageClassCounts.get(storageClass) || 0) + 1);
      storageClassSizes.set(storageClass, (storageClassSizes.get(storageClass) || 0) + obj.size);
    });

    for (const [storageClass, count] of storageClassCounts.entries()) {
      const size = storageClassSizes.get(storageClass) || 0;
      console.log(`  ${storageClass}: ${count} objects (${this.formatBytes(size)})`);
    }

    if (glacierObjects.length === 0) {
      console.log('No objects found in Glacier storage classes.');
    } else {
      // Sort by size (largest first) for better visibility
      glacierObjects.sort((a, b) => b.size - a.size);

      const longestKeyLength = 2 + glacierObjects.reduce((max, obj) => Math.max(max, obj.key.length), 0);
      const horizontalBarLength = longestKeyLength + 30;

      console.log(`\nObjects that would be restored from Glacier storage classes:`);
      console.log('-'.repeat(horizontalBarLength));

      console.log('Key'.padEnd(longestKeyLength) + 'Storage Class'.padEnd(15) + 'Size');
      console.log('-'.repeat(horizontalBarLength));

        glacierObjects.forEach(obj => {
        console.log(
          obj.key.padEnd(longestKeyLength) +
          obj.storageClass.padEnd(15) +
          this.formatBytes(obj.size)
        );
      });
    }

    return {
      objects: glacierObjects,
      totalSize: totalSize,
      totalSizeFormatted: this.formatBytes(totalSize)
    };
  }

  /**
   * Actually restore objects from Glacier (bulk mode, 2 days) using AWS SDK
   */
  public async restoreObjects(bucketName: string, dryRun: boolean = false): Promise<RestoreOperation> {
    const operation = await this.dryRunRestore(bucketName);

    if (dryRun || operation.objects.length === 0) {
      return operation;
    }

    console.log(`\nStarting restoration of ${operation.objects.length} objects...`);
    console.log('Restoration mode: Bulk (2 days retention)');
    console.log('=' .repeat(60));

    let successCount = 0;
    let errorCount = 0;

    for (const obj of operation.objects) {
      try {
        const command = new RestoreObjectCommand({
          Bucket: bucketName,
          Key: obj.key,
          RestoreRequest: {
            Days: 2,
            GlacierJobParameters: {
              Tier: 'Bulk'
            }
          }
        });

        await this.s3Client.send(command);
        successCount++;

        if (successCount % 10 === 0) {
          console.log(`Processed ${successCount}/${operation.objects.length} objects...`);
        }

      } catch (error) {
        errorCount++;
        console.warn(`Failed to restore ${obj.key}: ${error}`);
      }
    }

    console.log(`\nRestoration request completed:`);
    console.log(`  Successfully requested: ${successCount} objects`);
    console.log(`  Failed: ${errorCount} objects`);
    console.log(`  Total size to be restored: ${operation.totalSizeFormatted}`);

    return operation;
  }

  /**
   * List all S3 buckets and their sizes using AWS SDK
   */
  public async listBucketsWithSizes(): Promise<S3Bucket[]> {
    console.log('Fetching S3 buckets...');

    // Get list of all buckets using AWS SDK
    const command = new ListBucketsCommand({});
    const response = await this.s3Client.send(command);

    if (!response.Buckets) {
      return [];
    }

    console.log(`Found ${response.Buckets.length} buckets. Calculating sizes...`);

    const buckets: S3Bucket[] = [];

    for (const bucket of response.Buckets) {
      if (bucket.Name) {
        console.log(`Getting size for bucket: ${bucket.Name}`);
        const size = await this.getBucketSize(bucket.Name);

        buckets.push({
          name: bucket.Name,
          creationDate: bucket.CreationDate?.toISOString() || '',
          size: size,
          sizeFormatted: this.formatBytes(size)
        });
      }
    }

    // Sort buckets by size (largest first)
    buckets.sort((a, b) => b.size - a.size);

    return buckets;
  }

  /**
   * Display buckets in a formatted table
   */
  public displayBuckets(buckets: S3Bucket[]): void {
    console.log('\n=== S3 Buckets and Sizes ===');
    console.log('┌─────────────────────────────────────────────────────────────┬─────────────────────┬──────────────────┐');
    console.log('│ Bucket Name                                                 │ Size                │ Creation Date    │');
    console.log('├─────────────────────────────────────────────────────────────┼─────────────────────┼──────────────────┤');

    buckets.forEach(bucket => {
      const name = bucket.name.padEnd(59);
      const size = bucket.sizeFormatted.padEnd(19);
      const date = new Date(bucket.creationDate).toLocaleDateString().padEnd(16);
      console.log(`│ ${name} │ ${size} │ ${date} │`);
    });

    console.log('└─────────────────────────────────────────────────────────────┴─────────────────────┴──────────────────┘');

    const totalSize = buckets.reduce((sum, bucket) => sum + bucket.size, 0);
    console.log(`\nTotal storage across all buckets: ${this.formatBytes(totalSize)}`);
    console.log(`Number of buckets: ${buckets.length}`);
  }

  /**
   * Get the restore status of objects in a bucket using AWS SDK
   */
  private async getObjectRestoreStatus(bucketName: string, objectKey: string): Promise<RestoreStatus> {
    try {
      const command = new HeadObjectCommand({
        Bucket: bucketName,
        Key: objectKey
      });

      const response = await this.s3Client.send(command);

      let restoreStatus: 'in-progress' | 'completed' | 'not-requested' = 'not-requested';
      let restoreExpiryDate: string | undefined;

      if (response.Restore) {
        const restoreInfo = response.Restore;
        if (restoreInfo.includes('ongoing-request="true"')) {
          restoreStatus = 'in-progress';
        } else if (restoreInfo.includes('ongoing-request="false"')) {
          restoreStatus = 'completed';
          // Extract expiry date if available
          const expiryMatch = restoreInfo.match(/expiry-date="([^"]+)"/);
          if (expiryMatch) {
            restoreExpiryDate = expiryMatch[1];
          }
        }
      }

      return {
        key: objectKey,
        storageClass: response.StorageClass || 'STANDARD',
        restoreStatus: restoreStatus,
        restoreExpiryDate: restoreExpiryDate,
        size: response.ContentLength || 0
      };

    } catch (error) {
      // If we can't get the object info, assume it's not requested
      return {
        key: objectKey,
        storageClass: 'UNKNOWN',
        restoreStatus: 'not-requested',
        size: 0
      };
    }
  }

  /**
   * Check the status of Glacier retrievals for a bucket
   */
  public async checkRestoreStatus(bucketName: string): Promise<StatusSummary> {
    console.log(`\nChecking restore status for bucket: ${bucketName}`);
    console.log('=' .repeat(60));

    // Get all objects in Glacier storage classes
    const allObjects = await this.getBucketObjects(bucketName);
    const glacierObjects = this.filterGlacierObjects(allObjects);

    if (glacierObjects.length === 0) {
      console.log('No objects found in Glacier storage classes.');
      return {
        bucketName: bucketName,
        glacierObjects: [],
        inProgress: [],
        completed: [],
        notRequested: [],
        totalSizeInProgress: 0,
        totalSizeCompleted: 0
      };
    }

    console.log(`Found ${glacierObjects.length} objects in Glacier storage classes.`);
    console.log('Checking restore status for each object...\n');

    const statusResults: RestoreStatus[] = [];
    let processed = 0;

    for (const obj of glacierObjects) {
      const status = await this.getObjectRestoreStatus(bucketName, obj.key);
      statusResults.push(status);
      processed++;

      if (processed % 10 === 0) {
        console.log(`Processed ${processed}/${glacierObjects.length} objects...`);
      }
    }

    // Categorize results
    const inProgress = statusResults.filter(s => s.restoreStatus === 'in-progress');
    const completed = statusResults.filter(s => s.restoreStatus === 'completed');
    const notRequested = statusResults.filter(s => s.restoreStatus === 'not-requested');

    const totalSizeInProgress = inProgress.reduce((sum, obj) => sum + obj.size, 0);
    const totalSizeCompleted = completed.reduce((sum, obj) => sum + obj.size, 0);

    return {
      bucketName: bucketName,
      glacierObjects: glacierObjects,
      inProgress: inProgress,
      completed: completed,
      notRequested: notRequested,
      totalSizeInProgress: totalSizeInProgress,
      totalSizeCompleted: totalSizeCompleted
    };
  }

  /**
   * Display detailed restore status information
   */
  public async displayRestoreStatus(bucketName: string): Promise<void> {
    const summary = await this.checkRestoreStatus(bucketName);

    console.log('\n' + '='.repeat(60));
    console.log('RESTORE STATUS SUMMARY');
    console.log('='.repeat(60));
    console.log(`Bucket: ${summary.bucketName}`);
    console.log(`Total Glacier objects: ${summary.glacierObjects.length}`);
    console.log();
    console.log(`📥 In Progress: ${summary.inProgress.length} objects (${this.formatBytes(summary.totalSizeInProgress)})`);
    console.log(`✅ Completed: ${summary.completed.length} objects (${this.formatBytes(summary.totalSizeCompleted)})`);
    console.log(`⏸️  Not Requested: ${summary.notRequested.length} objects`);

    if (summary.inProgress.length > 0) {
      console.log('\n' + '-'.repeat(60));
      console.log('OBJECTS WITH ONGOING RESTORE REQUESTS:');
      console.log('-'.repeat(60));

      for (const status of summary.inProgress) {
          const key = status.key.length > 50 ? '...' + status.key.slice(-47) : status.key;
          console.log(`${key.padEnd(50)} ${status.storageClass.padEnd(15)} ${this.formatBytes(status.size)}`);
      }
    }

    if (summary.completed.length > 0) {
      console.log('\n' + '-'.repeat(60));
      console.log('RECENTLY COMPLETED RESTORES:');
      console.log('-'.repeat(60));

      for (const status of summary.completed) {
          const key = status.key.length > 40 ? '...' + status.key.slice(-37) : status.key;
          const expiry = status.restoreExpiryDate ?
              new Date(status.restoreExpiryDate).toLocaleDateString() : 'Unknown';
          console.log(`${key.padEnd(40)} ${status.storageClass.padEnd(15)} ${this.formatBytes(status.size).padEnd(10)} Expires: ${expiry}`);
      }
    }

    console.log('\n' + '='.repeat(60));
    if (summary.inProgress.length > 0) {
      console.log('⏳ Some restore requests are still in progress.');
      console.log('   Files will be available for download once restoration completes.');
      console.log('   Bulk requests typically take 5-12 hours to complete.');
    }
    if (summary.completed.length > 0) {
      console.log('✅ Some files are ready for download!');
      console.log('   Note: Restored files are temporarily available and will expire.');
    }
    if (summary.notRequested.length > 0) {
      console.log(`🔄 ${summary.notRequested.length} objects have not been restored yet.`);
      console.log(`   Run 'dry-run ${bucketName}' to see what would be restored.`);
    }
  }

  /**
   * Show available commands
   */
  public showHelp(): void {
    console.log('AWS S3 Glacier Management Tool');
    console.log('==============================\n');
    console.log('Configuration:');
    console.log('  Uses standard AWS credentials from ~/.aws/credentials');
    console.log('  Region from ~/.aws/config or AWS_REGION environment variable');
    console.log('  Default region: us-east-1');
    console.log('\nAvailable commands:');
    console.log('  list                    - List all S3 buckets and their sizes');
    console.log('  dry-run <bucket>        - Simulate restoration without actually restoring files');
    console.log('  restore <bucket>        - Restore files from Deep Glacier (bulk mode, 2 days)');
    console.log('  status <bucket>         - Check status of ongoing Glacier retrievals');
    console.log('\nUsage:');
    console.log('  node dist/index.js list');
    console.log('  node dist/index.js dry-run my-bucket-name');
    console.log('  node dist/index.js restore my-bucket-name');
    console.log('  node dist/index.js status my-bucket-name');
  }

  /**
   * Get AWS config value from the config file content
   */
  private getConfigValue(configData: string, key: string): string {
    const regex = new RegExp(`^${key}\\s*=\\s*(.*)$`, 'm');
    const match = configData.match(regex);
    return match ? match[1] : '';
  }
}

// Main execution
async function main() {
  try {
    const tool = new AWSGlacierTool();
    const args = process.argv.slice(2);
    const command = args[0];

    if (!command) {
      tool.showHelp();
      return;
    }

    switch (command.toLowerCase()) {
      case 'list':
        console.log('AWS S3 Glacier Management Tool');
        console.log('==============================\n');

        // Feature 1: List all buckets and their sizes
        const buckets = await tool.listBucketsWithSizes();
        tool.displayBuckets(buckets);
        break;

      case 'dry-run':
        const dryRunBucketName = args[1];
        if (!dryRunBucketName) {
          console.error('Error: Please specify a bucket name');
          console.log('Usage: node dist/index.js dry-run <bucket-name>');
          process.exit(1);
        }

        const dryRunResult = await tool.dryRunRestore(dryRunBucketName);

        console.log(`\n${'='.repeat(60)}`);
        console.log('DRY RUN SUMMARY');
        console.log(`${'='.repeat(60)}`);
        console.log(`Bucket: ${dryRunBucketName}`);
        console.log(`Objects to restore: ${dryRunResult.objects.length}`);
        console.log(`Total size to restore: ${dryRunResult.totalSizeFormatted}`);
        console.log(`\nNote: This was a dry run. No files were actually restored.`);
        console.log(`Run 'restore ${dryRunBucketName}' to perform the actual restoration.`);
        break;

      case 'restore':
        const bucketName = args[1];
        if (!bucketName) {
          console.error('Error: Please specify a bucket name');
          console.log('Usage: node dist/index.js restore <bucket-name>');
          process.exit(1);
        }

        const restoreResult = await tool.restoreObjects(bucketName, false);

        console.log(`\n${'='.repeat(60)}`);
        console.log('RESTORATION SUMMARY');
        console.log(`${'='.repeat(60)}`);
        console.log(`Bucket: ${bucketName}`);
        console.log(`Objects processed: ${restoreResult.objects.length}`);
        console.log(`Total size: ${restoreResult.totalSizeFormatted}`);
        console.log(`\nRestoration requests have been submitted.`);
        console.log(`Files will be available for download in a few hours (bulk mode).`);
        console.log(`Use 'status ${bucketName}' to check restoration progress.`);
        break;

      case 'status':
        const statusBucketName = args[1];
        if (!statusBucketName) {
          console.error('Error: Please specify a bucket name');
          console.log('Usage: node dist/index.js status <bucket-name>');
          process.exit(1);
        }

        console.log('AWS S3 Glacier Management Tool');
        console.log('==============================');

        await tool.displayRestoreStatus(statusBucketName);
        break;

      default:
        console.error(`Error: Unknown command "${command}"`);
        console.log('');
        tool.showHelp();
        process.exit(1);
    }

  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

// Run the tool if this file is executed directly
if (require.main === module) {
  main();
}

export { AWSGlacierTool, S3Bucket };
