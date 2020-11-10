# Soto S3 Transfer

Make uploading and downloading of files to AWS S3 easy.

## Setup

Soto S3 Transfer uses the Soto Swift SDK for AWS. You need to create a Soto service object before you can use the S3 transfer manager. See Soto [documentation](https://github.com/soto-project/soto) for more guidance. You also need to supply the `threadPoolProvider` parameter which indicates where Soto S3 Transfer will get threads from to run the file loading and saving. 

```swift
let client = AWSClient(httpClientProvider: .createNew)
let s3 = S3(client: client, region: .euwest1)
let s3Transfer = S3TransferManager(s3: s3, threadPoolProvider: .createNew)
```

## Upload to S3

Uploading files to S3 is done with one call. The call returns a [NIO](https://github.com/apple/swift-nio) EventLoopFuture which will be fulfilled with the response when the operation is finished.
```swift
let uploadFuture = s3Transfer.copy(
    from: "/Users/me/images/test.jpg", 
    to: S3File(url: "s3://my-bucket/test.jpg")!
)
```
You can also upload a folder as follows
```swift
let uploadFuture = s3Transfer.copy(
    from: "/Users/me/images/", 
    to: S3Folder(url: "s3://my-bucket/images/")!
)
```

## Download from S3

Download is as simple as upload just swap the parameters around
```swift
let downloadFuture = s3Transfer.copy(
    from: S3File(url: "s3://my-bucket/test.jpg")!, 
    to: "/Users/me/images/test.jpg"
)
let downloadFolderFuture = s3Transfer.copy(
    from: S3Folder(url: "s3://my-bucket/images/")!, 
    to: "/Users/me/downloads/images/"
)
```

## Copy from one S3 bucket to another

You can also copy from one S3 bucket to another by supplying two `S3Files` or two `S3Folders`
```swift
let copyFuture = s3Transfer.copy(
    from: S3File(url: "s3://my-bucket/test2.jpg")!, 
    to: S3File(url: "s3://my-bucket/test.jpg")!
)
let copyFolderFuture = s3Transfer.copy(
    from: S3Folder(url: "s3://my-bucket/images/")!, 
    to: S3Folder(url: "s3://my-other-bucket/images/")!)
)
```

## Sync operations

There are `sync` versions of these operations as well. This will only copy files across if they are newer than the existing files. You can also have it delete files  in the target folder if they don't exist in the source folder.

```swift
let uploadSyncFuture = s3Transfer.sync(
    from: "/Users/me/images/", 
    to: S3Folder(url: "s3://my-bucket/images")!,
    delete: true
)
let downloadFuture = s3Transfer.copy(
    from: S3Folder(url: "s3://my-bucket/images")!, 
    to: "/Users/me/downloads/images/",
    delete: false
)
```

## Multipart upload

If uploads are above a certain size then the transfer manager will use multipart upload to upload the file to S3. You can control what this threshold is and the multipart part sizes by supplying a configuration at initialization of the manager. If you don't supply a configuration both of these values are set to 8MB.
```swift
let s3Transfer = S3TransferManager(
    s3: s3, 
    threadPoolProvider: .createNew,
    configuration: .init(multipartThreshold: 16*1024*1024, multipartPartSize: 16*1024*1024)
)
```
