import { execSync } from 'child_process';

interface S3Bucket {
  name: string;
  creationDate: string;
  size: number;
  sizeFormatted: string;
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
    console.log('  restore <bucket>        - Check and restore files from Deep Glacier (coming soon)');
    console.log('  status <bucket>         - Check status of ongoing Glacier retrievals (coming soon)');
    console.log('\nUsage:');
    console.log('  node dist/index.js list');
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

      case 'restore':
        const bucketName = args[1];
        if (!bucketName) {
          console.error('Error: Please specify a bucket name');
          console.log('Usage: node dist/index.js restore <bucket-name>');
          process.exit(1);
        }
        console.log(`Restore feature for bucket "${bucketName}" - Coming soon!`);
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
