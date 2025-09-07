
# Glacier Retrieve

> [!Warning]
> 
> This was mostly vibe-coded in a couple of hours. While it might be useful depending on your use case, it is not production-ready. Use at your own risk.

A simple tool to help retrieve files from Amazon Glacier.

Set your AWS credentials in `~/.aws/credentials` as you would for running the AWS CLI tool. If you need to specify the region, set it in `~/.aws/config`.

Beware that the access key used must have S3 permissions, including `s3:RestoreObject` needed to restore objects from Glacier.

To set up:

```
nvm install
npm install
npm run build
```

To run:

```
node dist/index.js
```
