
# Glacier Retrieve

A simple tool to help retrieve files from Amazon Glacier.

You must have the `aws` CLI installed and configured with your credentials. It must have S3 permissions, including `s3:RestoreObject` needed to restore objects from Glacier.

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
