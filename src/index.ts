import { execSync } from 'child_process';

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

class AWSGlacierTool {
  /**
   * Execute AWS CLI command and return the output
   */
  private executeAwsCommand(command: string): string {
    try {
      const output = execSync(command, { encoding: 'utf-8' });
      return output.trim();
    } catch (error) {
      throw new Error(`AWS CLI command failed: ${error}`);
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
   * Get the size of a specific S3 bucket
   */
  private getBucketSize(bucketName: string): number {
    try {
      const command = `aws s3api list-objects-v2 --bucket "${bucketName}" --query "sum(Contents[].Size)" --output text`;
      const output = this.executeAwsCommand(command);

      // AWS CLI returns "None" if bucket is empty or has no objects
      if (output === 'None' || output === '') {
        return 0;
      }

      return parseInt(output, 10) || 0;
    } catch (error) {
      console.warn(`Warning: Could not get size for bucket ${bucketName}: ${error}`);
      return 0;
    }
  }

  /**
   * Get all objects in a bucket with their storage class information
   */
  private async getBucketObjects(bucketName: string): Promise<S3Object[]> {
    console.log(`Scanning bucket "${bucketName}" for objects...`);

    const objects: S3Object[] = [];
    let continuationToken = '';

    do {
      const tokenParam = continuationToken ? `--continuation-token "${continuationToken}"` : '';
      const command = `aws s3api list-objects-v2 --bucket "${bucketName}" ${tokenParam} --query "Contents[].{Key:Key,Size:Size,StorageClass:StorageClass,LastModified:LastModified}" --output json`;

      try {
        const output = this.executeAwsCommand(command);
        const objectsData = JSON.parse(output);

        if (objectsData && Array.isArray(objectsData)) {
          for (const obj of objectsData) {
            objects.push({
              key: obj.Key,
              size: obj.Size || 0,
              storageClass: obj.StorageClass || 'STANDARD',
              lastModified: obj.LastModified
            });
          }
        }

        // Check if there are more objects to fetch
        const nextTokenCommand = `aws s3api list-objects-v2 --bucket "${bucketName}" ${tokenParam} --query "NextContinuationToken" --output text`;
        const nextToken = this.executeAwsCommand(nextTokenCommand);
        continuationToken = (nextToken && nextToken !== 'None') ? nextToken : '';

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

    console.log(`\nObjects that would be restored from Glacier storage classes:`);
    console.log('-'.repeat(60));

    if (glacierObjects.length === 0) {
      console.log('No objects found in Glacier storage classes.');
    } else {
      // Sort by size (largest first) for better visibility
      glacierObjects.sort((a, b) => b.size - a.size);

      // Display first 20 objects to avoid overwhelming output
      const displayObjects = glacierObjects.slice(0, 20);

      console.log('Key'.padEnd(50) + 'Storage Class'.padEnd(15) + 'Size');
      console.log('-'.repeat(80));

      displayObjects.forEach(obj => {
        const key = obj.key.length > 45 ? '...' + obj.key.slice(-42) : obj.key;
        console.log(
          key.padEnd(50) +
          obj.storageClass.padEnd(15) +
          this.formatBytes(obj.size)
        );
      });

      if (glacierObjects.length > 20) {
        console.log(`... and ${glacierObjects.length - 20} more objects`);
      }
    }

    return {
      objects: glacierObjects,
      totalSize: totalSize,
      totalSizeFormatted: this.formatBytes(totalSize)
    };
  }

  /**
   * Actually restore objects from Glacier (bulk mode, 2 days)
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
        const restoreRequest = {
          Days: 2,
          GlacierJobParameters: {
            Tier: 'Bulk'
          }
        };

        const command = `aws s3api restore-object --bucket "${bucketName}" --key "${obj.key}" --restore-request '${JSON.stringify(restoreRequest)}'`;

        this.executeAwsCommand(command);
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
   * List all S3 buckets and their sizes
   */
  public async listBucketsWithSizes(): Promise<S3Bucket[]> {
    console.log('Fetching S3 buckets...');

    // Get list of all buckets
    const bucketsOutput = this.executeAwsCommand('aws s3api list-buckets --query "Buckets[].{Name:Name,CreationDate:CreationDate}" --output json');
    const bucketsData = JSON.parse(bucketsOutput);

    console.log(`Found ${bucketsData.length} buckets. Calculating sizes...`);

    const buckets: S3Bucket[] = [];

    for (const bucket of bucketsData) {
      console.log(`Getting size for bucket: ${bucket.Name}`);
      const size = this.getBucketSize(bucket.Name);

      buckets.push({
        name: bucket.Name,
        creationDate: bucket.CreationDate,
        size: size,
        sizeFormatted: this.formatBytes(size)
      });
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
   * Show available commands
   */
  public showHelp(): void {
    console.log('AWS S3 Glacier Management Tool');
    console.log('==============================\n');
    console.log('Available commands:');
    console.log('  list                    - List all S3 buckets and their sizes');
    console.log('  dry-run <bucket>        - Simulate restoration without actually restoring files');
    console.log('  restore <bucket>        - Restore files from Deep Glacier (bulk mode, 2 days)');
    console.log('  status <bucket>         - Check status of ongoing Glacier retrievals (coming soon)');
    console.log('\nUsage:');
    console.log('  node dist/index.js list');
    console.log('  node dist/index.js dry-run my-bucket-name');
    console.log('  node dist/index.js restore my-bucket-name');
    console.log('  node dist/index.js status my-bucket-name');
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
        console.log(`Status feature for bucket "${statusBucketName}" - Coming soon!`);
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
