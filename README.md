
# Glacier Retrieve

> [!Warning]
> 
> This was mostly vibe-coded in a couple of hours. While it might be useful depending on your use case, it is not production-ready. Use at your own risk.

A simple tool to help retrieve files from Amazon Glacier.

It basically lets you start the restore process for all objects in a given S3 bucket and then check the status of the restore process. Downloading the files is left to you.

Set your AWS credentials in `~/.aws/credentials` as you would for running the AWS CLI tool. If you need to specify the region, set it in `~/.aws/config`.

Beware that the access key used must have S3 permissions, including `s3:RestoreObject` needed to restore objects from Glacier.

## Setting up

This will install the dependencies and build the project for running.

```
nvm install
npm install
npm run build
```

## Running

If you just want to run it without installing globally, you can do:

```
npm start
```

## Installing globally

```
npm install -g .
```

Then you can run it as:

```
glacier-retrieve
```
