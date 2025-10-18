//===----------------------------------------------------------------------===//
//
// This source file is part of the Soto for AWS open source project
//
// Copyright (c) 2020-2021 the Soto project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Soto project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Foundation
import SotoS3

extension Date {
    fileprivate func _httpDateString() -> String {
        let formatter = DateFormatter()
        formatter.dateFormat = "EEE, dd MMM yyyy HH:mm:ss z"
        formatter.timeZone = TimeZone(abbreviation: "GMT")
        formatter.locale = Locale(identifier: "en_US_POSIX")
        return formatter.string(from: self)
    }
}

extension S3FileTransferManager {
    public struct PutOptions: Sendable {
        /// The canned ACL to apply to the object. For more information, see Canned ACL. This action is not supported by Amazon S3 on Outposts.
        public let acl: S3.ObjectCannedACL?
        ///  Can be used to specify caching behavior along the request/reply chain. For more information, see http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9.
        public let cacheControl: String?
        /// Specifies presentational information for the object. For more information, see http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1.
        public let contentDisposition: String?
        /// Specifies what content encodings have been applied to the object and thus what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field. For more information, see http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11.
        public let contentEncoding: String?
        /// The language the content is in.
        public let contentLanguage: String?
        /// A standard MIME type describing the format of the contents. For more information, see http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17.
        public let contentType: String?
        /// The account id of the expected bucket owner. If the bucket is owned by a different account, the request will fail with an HTTP 403 (Access Denied) error.
        public let expectedBucketOwner: String?
        /// The date and time at which the object is no longer cacheable. For more information, see http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21.
        public let expires: Date?
        /// Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object. This action is not supported by Amazon S3 on Outposts.
        public let grantFullControl: String?
        /// Allows grantee to read the object data and its metadata. This action is not supported by Amazon S3 on Outposts.
        public let grantRead: String?
        /// Allows grantee to read the object ACL. This action is not supported by Amazon S3 on Outposts.
        public let grantReadACP: String?
        /// Allows grantee to write the ACL for the applicable object. This action is not supported by Amazon S3 on Outposts.
        public let grantWriteACP: String?
        /// A map of metadata to store with the object in S3.
        public let metadata: [String: String]?
        /// Specifies whether a legal hold will be applied to this object. For more information about S3 Object Lock, see Object Lock.
        public let objectLockLegalHoldStatus: S3.ObjectLockLegalHoldStatus?
        /// The Object Lock mode that you want to apply to this object.
        public let objectLockMode: S3.ObjectLockMode?
        /// The date and time when you want this object's Object Lock to expire.
        public let objectLockRetainUntilDate: Date?
        public let requestPayer: S3.RequestPayer?
        /// The server-side encryption algorithm used when storing this object in Amazon S3 (for example, AES256, aws:kms).
        public let serverSideEncryption: S3.ServerSideEncryption?
        /// Specifies the algorithm to use to when encrypting the object (for example, AES256).
        public let sseCustomerAlgorithm: String?
        /// Specifies the customer-provided encryption key for Amazon S3 to use in encrypting data. This value is used to store the object and then it is discarded; Amazon S3 does not store the encryption key. The key must be appropriate for use with the algorithm specified in the x-amz-server-side-encryption-customer-algorithm header.
        public let sseCustomerKey: String?
        /// Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321. Amazon S3 uses this header for a message integrity check to ensure that the encryption key was transmitted without error.
        public let sseCustomerKeyMD5: String?
        /// Specifies the AWS KMS Encryption Context to use for object encryption. The value of this header is a base64-encoded UTF-8 string holding JSON with the encryption context key-value pairs.
        public let ssekmsEncryptionContext: String?
        /// If x-amz-server-side-encryption is present and has the value of aws:kms, this header specifies the ID of the AWS Key Management Service (AWS KMS) symmetrical customer managed customer master key (CMK) that was used for the object.  If the value of x-amz-server-side-encryption is aws:kms, this header specifies the ID of the symmetric customer managed AWS KMS CMK that will be used for the object. If you specify x-amz-server-side-encryption:aws:kms, but do not provide x-amz-server-side-encryption-aws-kms-key-id, Amazon S3 uses the AWS managed CMK in AWS to protect the data.
        public let ssekmsKeyId: String?
        /// By default, Amazon S3 uses the STANDARD Storage Class to store newly created objects. The STANDARD storage class provides high durability and high availability. Depending on performance needs, you can specify a different Storage Class. Amazon S3 on Outposts only uses the OUTPOSTS Storage Class. For more information, see Storage Classes in the Amazon S3 Service Developer Guide.
        public let storageClass: S3.StorageClass?
        /// The tag-set for the object. The tag-set must be encoded as URL Query parameters. (For example, "Key1=Value1")
        public let tagging: String?
        /// If the bucket is configured as a website, redirects requests for this object to another object in the same bucket or to an external URL. Amazon S3 stores the value of this header in the object metadata. For information about object metadata, see Object Key and Metadata. In the following example, the request header sets the redirect to an object (anotherPage.html) in the same bucket:  x-amz-website-redirect-location: /anotherPage.html  In the following example, the request header sets the object redirect to another website:  x-amz-website-redirect-location: http://www.example.com/  For more information about website hosting in Amazon S3, see Hosting Websites on Amazon S3 and How to Configure Website Page Redirects.
        public let websiteRedirectLocation: String?

        public init(
            acl: S3.ObjectCannedACL? = nil,
            cacheControl: String? = nil,
            contentDisposition: String? = nil,
            contentEncoding: String? = nil,
            contentLanguage: String? = nil,
            contentType: String? = nil,
            expectedBucketOwner: String? = nil,
            expires: Date? = nil,
            grantFullControl: String? = nil,
            grantRead: String? = nil,
            grantReadACP: String? = nil,
            grantWriteACP: String? = nil,
            metadata: [String: String]? = nil,
            objectLockLegalHoldStatus: S3.ObjectLockLegalHoldStatus? = nil,
            objectLockMode: S3.ObjectLockMode? = nil,
            objectLockRetainUntilDate: Date? = nil,
            requestPayer: S3.RequestPayer? = nil,
            serverSideEncryption: S3.ServerSideEncryption? = nil,
            sseCustomerAlgorithm: String? = nil,
            sseCustomerKey: String? = nil,
            sseCustomerKeyMD5: String? = nil,
            ssekmsEncryptionContext: String? = nil,
            ssekmsKeyId: String? = nil,
            storageClass: S3.StorageClass? = nil,
            tagging: String? = nil,
            websiteRedirectLocation: String? = nil
        ) {
            self.acl = acl
            self.cacheControl = cacheControl
            self.contentDisposition = contentDisposition
            self.contentEncoding = contentEncoding
            self.contentLanguage = contentLanguage
            self.contentType = contentType
            self.expectedBucketOwner = expectedBucketOwner
            self.expires = expires
            self.grantFullControl = grantFullControl
            self.grantRead = grantRead
            self.grantReadACP = grantReadACP
            self.grantWriteACP = grantWriteACP
            self.metadata = metadata
            self.objectLockLegalHoldStatus = objectLockLegalHoldStatus
            self.objectLockMode = objectLockMode
            self.objectLockRetainUntilDate = objectLockRetainUntilDate
            self.requestPayer = requestPayer
            self.serverSideEncryption = serverSideEncryption
            self.sseCustomerAlgorithm = sseCustomerAlgorithm
            self.sseCustomerKey = sseCustomerKey
            self.sseCustomerKeyMD5 = sseCustomerKeyMD5
            self.ssekmsEncryptionContext = ssekmsEncryptionContext
            self.ssekmsKeyId = ssekmsKeyId
            self.storageClass = storageClass
            self.tagging = tagging
            self.websiteRedirectLocation = websiteRedirectLocation
        }
    }

    public struct GetOptions: AWSEncodableShape {
        /// The account id of the expected bucket owner. If the bucket is owned by a different account, the request will fail with an HTTP 403 (Access Denied) error.
        public let expectedBucketOwner: String?
        /// Return the object only if its entity tag (ETag) is the same as the one specified, otherwise return a 412 (precondition failed).
        public let ifMatch: String?
        /// Return the object only if it has been modified since the specified time, otherwise return a 304 (not modified).
        public let ifModifiedSince: Date?
        /// Return the object only if its entity tag (ETag) is different from the one specified, otherwise return a 304 (not modified).
        public let ifNoneMatch: String?
        /// Return the object only if it has not been modified since the specified time, otherwise return a 412 (precondition failed).
        public let ifUnmodifiedSince: Date?
        public let requestPayer: S3.RequestPayer?
        /// Specifies the algorithm to use to when encrypting the object (for example, AES256).
        public let sseCustomerAlgorithm: String?
        /// Specifies the customer-provided encryption key for Amazon S3 to use in encrypting data. This value is used to store the object and then it is discarded; Amazon S3 does not store the encryption key. The key must be appropriate for use with the algorithm specified in the x-amz-server-side-encryption-customer-algorithm header.
        public let sseCustomerKey: String?
        /// Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321. Amazon S3 uses this header for a message integrity check to ensure that the encryption key was transmitted without error.
        public let sseCustomerKeyMD5: String?
        /// VersionId used to reference a specific version of the object.
        public let versionId: String?

        /// Ignore file folder clashes when downloading folders. When set to true the file is not downloaded
        public let ignoreFileFolderClashes: Bool

        public init(
            expectedBucketOwner: String? = nil,
            ifMatch: String? = nil,
            ifModifiedSince: Date? = nil,
            ifNoneMatch: String? = nil,
            ifUnmodifiedSince: Date? = nil,
            requestPayer: S3.RequestPayer? = nil,
            sseCustomerAlgorithm: String? = nil,
            sseCustomerKey: String? = nil,
            sseCustomerKeyMD5: String? = nil,
            versionId: String? = nil,
            ignoreFileFolderClashes: Bool = false
        ) {
            self.expectedBucketOwner = expectedBucketOwner
            self.ifMatch = ifMatch
            self.ifModifiedSince = ifModifiedSince
            self.ifNoneMatch = ifNoneMatch
            self.ifUnmodifiedSince = ifUnmodifiedSince
            self.requestPayer = requestPayer
            self.sseCustomerAlgorithm = sseCustomerAlgorithm
            self.sseCustomerKey = sseCustomerKey
            self.sseCustomerKeyMD5 = sseCustomerKeyMD5
            self.versionId = versionId

            self.ignoreFileFolderClashes = ignoreFileFolderClashes
        }
    }

    public typealias CopyOptions = PutOptions
}

extension S3.PutObjectRequest {
    init(body: AWSHTTPBody, bucket: String, key: String, options: S3FileTransferManager.PutOptions) {
        self.init(
            acl: options.acl,
            body: body,
            bucket: bucket,
            cacheControl: options.cacheControl,
            contentDisposition: options.contentDisposition,
            contentEncoding: options.contentEncoding,
            contentLanguage: options.contentLanguage,
            contentType: options.contentType,
            expectedBucketOwner: options.expectedBucketOwner,
            expires: options.expires?._httpDateString(),
            grantFullControl: options.grantFullControl,
            grantRead: options.grantRead,
            grantReadACP: options.grantReadACP,
            grantWriteACP: options.grantWriteACP,
            key: key,
            metadata: options.metadata,
            objectLockLegalHoldStatus: options.objectLockLegalHoldStatus,
            objectLockMode: options.objectLockMode,
            objectLockRetainUntilDate: options.objectLockRetainUntilDate,
            requestPayer: options.requestPayer,
            serverSideEncryption: options.serverSideEncryption,
            sseCustomerAlgorithm: options.sseCustomerAlgorithm,
            sseCustomerKey: options.sseCustomerKey,
            sseCustomerKeyMD5: options.sseCustomerKeyMD5,
            ssekmsEncryptionContext: options.ssekmsEncryptionContext,
            ssekmsKeyId: options.ssekmsKeyId,
            storageClass: options.storageClass,
            tagging: options.tagging,
            websiteRedirectLocation: options.websiteRedirectLocation
        )
    }
}

extension S3.CreateMultipartUploadRequest {
    init(bucket: String, key: String, options: S3FileTransferManager.PutOptions) {
        self.init(
            acl: options.acl,
            bucket: bucket,
            cacheControl: options.cacheControl,
            contentDisposition: options.contentDisposition,
            contentEncoding: options.contentEncoding,
            contentLanguage: options.contentLanguage,
            contentType: options.contentType,
            expectedBucketOwner: options.expectedBucketOwner,
            expires: options.expires?._httpDateString(),
            grantFullControl: options.grantFullControl,
            grantRead: options.grantRead,
            grantReadACP: options.grantReadACP,
            grantWriteACP: options.grantWriteACP,
            key: key,
            metadata: options.metadata,
            objectLockLegalHoldStatus: options.objectLockLegalHoldStatus,
            objectLockMode: options.objectLockMode,
            objectLockRetainUntilDate: options.objectLockRetainUntilDate,
            requestPayer: options.requestPayer,
            serverSideEncryption: options.serverSideEncryption,
            sseCustomerAlgorithm: options.sseCustomerAlgorithm,
            sseCustomerKey: options.sseCustomerKey,
            sseCustomerKeyMD5: options.sseCustomerKeyMD5,
            ssekmsEncryptionContext: options.ssekmsEncryptionContext,
            ssekmsKeyId: options.ssekmsKeyId,
            storageClass: options.storageClass,
            tagging: options.tagging,
            websiteRedirectLocation: options.websiteRedirectLocation
        )
    }
}

extension S3.CopyObjectRequest {
    init(bucket: String, copySource: String, key: String, options: S3FileTransferManager.CopyOptions) {
        self.init(
            acl: options.acl,
            bucket: bucket,
            cacheControl: options.cacheControl,
            contentDisposition: options.contentDisposition,
            contentEncoding: options.contentEncoding,
            contentLanguage: options.contentLanguage,
            contentType: options.contentType,
            copySource: copySource,
            expectedBucketOwner: options.expectedBucketOwner,
            expires: options.expires?._httpDateString(),
            grantFullControl: options.grantFullControl,
            grantRead: options.grantRead,
            grantReadACP: options.grantReadACP,
            grantWriteACP: options.grantWriteACP,
            key: key,
            metadata: options.metadata,
            objectLockLegalHoldStatus: options.objectLockLegalHoldStatus,
            objectLockMode: options.objectLockMode,
            objectLockRetainUntilDate: options.objectLockRetainUntilDate,
            requestPayer: options.requestPayer,
            serverSideEncryption: options.serverSideEncryption,
            sseCustomerAlgorithm: options.sseCustomerAlgorithm,
            sseCustomerKey: options.sseCustomerKey,
            sseCustomerKeyMD5: options.sseCustomerKeyMD5,
            ssekmsEncryptionContext: options.ssekmsEncryptionContext,
            ssekmsKeyId: options.ssekmsKeyId,
            storageClass: options.storageClass,
            tagging: options.tagging,
            websiteRedirectLocation: options.websiteRedirectLocation
        )
    }
}

extension S3.GetObjectRequest {
    init(bucket: String, key: String, options: S3FileTransferManager.GetOptions) {
        self.init(
            bucket: bucket,
            expectedBucketOwner: options.expectedBucketOwner,
            ifMatch: options.ifMatch,
            ifModifiedSince: options.ifModifiedSince,
            ifNoneMatch: options.ifNoneMatch,
            ifUnmodifiedSince: options.ifUnmodifiedSince,
            key: key,
            requestPayer: options.requestPayer,
            sseCustomerAlgorithm: options.sseCustomerAlgorithm,
            sseCustomerKey: options.sseCustomerKey,
            sseCustomerKeyMD5: options.sseCustomerKeyMD5,
            versionId: options.versionId
        )
    }
}
