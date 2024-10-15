package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Constants for default configurations.
const (
	BatchSize              = 1000
	DefaultDurationLimit   = 30 * time.Minute
	defaultRegion          = "us-east-1"
	defaultMaxRetries      = 3
	defaultTimeout         = 28 * time.Second
	contentTypeOctetStream = "application/octet-stream"
)

// Client is a wrapper around AWS S3 client providing additional utility methods.
type Client struct {
	region    string
	awsSess   *session.Session
	awsClient *s3.S3
}

// ObjectInfo contains metadata about an S3 object.
type ObjectInfo struct {
	Key        string    `json:"key"`
	Size       int64     `json:"size"`
	UpdateTime time.Time `json:"updateTime"`
}

// SyncWriterAt wraps an io.Writer to implement io.WriterAt interface.
type SyncWriterAt struct {
	w io.Writer
}

// WriteAt writes len(p) bytes from p to the underlying data stream at offset.
// It returns the number of bytes written and any error encountered that caused the write to stop early.
func (a SyncWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	return a.w.Write(p)
}

// Constructors

// NewClient creates a new S3 client with the specified AWS region.
// If no region is provided, it defaults to "us-east-1".
func NewClient(region string) *Client {
	if region == "" {
		region = defaultRegion
	}

	s3Config := aws.Config{
		CredentialsChainVerboseErrors: aws.Bool(true),
		Region:                        aws.String(region),
	}

	awsSess := session.Must(session.NewSession(&s3Config))
	return &Client{
		region:    region,
		awsSess:   awsSess,
		awsClient: s3.New(awsSess),
	}
}

// NewClientFromEnv creates a new S3 client using AWS SDK configuration from environment variables.
func NewClientFromEnv() *Client {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "1")

	awsSess := session.Must(session.NewSession(&aws.Config{}))
	return &Client{
		region:    aws.StringValue(awsSess.Config.Region),
		awsSess:   awsSess,
		awsClient: s3.New(awsSess),
	}
}

// NewClientWithConfig creates a new S3 client with a custom AWS configuration.
func NewClientWithConfig(cfg *aws.Config) *Client {
	awsSess := session.Must(session.NewSession(cfg))
	return &Client{
		region:    aws.StringValue(cfg.Region),
		awsSess:   awsSess,
		awsClient: s3.New(awsSess),
	}
}

// Getters

// Region returns the AWS region of the client.
func (c *Client) Region() string {
	return c.region
}

// AWSSession returns the underlying AWS session.
func (c *Client) AWSSession() *session.Session {
	return c.awsSess
}

// AWSClient returns the underlying AWS S3 client.
func (c *Client) AWSClient() *s3.S3 {
	return c.awsClient
}

// Bucket Operations

// CheckBucket verifies if the specified bucket exists and is accessible.
func (c *Client) CheckBucket(ctx context.Context, bucket string) error {
	_, err := c.awsClient.HeadBucketWithContext(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	return err
}

// CreateBucket creates a new S3 bucket. If the bucket already exists, it does not return an error.
func (c *Client) CreateBucket(ctx context.Context, bucket string) error {
	_, err := c.awsClient.CreateBucketWithContext(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil && !IsBucketExistsErr(err) {
		return err
	}
	return nil
}

// DeleteBucket deletes the specified S3 bucket after emptying its contents.
func (c *Client) DeleteBucket(ctx context.Context, bucket string) error {
	if err := c.EmptyBucket(ctx, bucket); err != nil {
		return err
	}

	_, err := c.awsClient.DeleteBucketWithContext(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	return err
}

// EmptyBucket removes all objects from the specified S3 bucket.
func (c *Client) EmptyBucket(ctx context.Context, bucket string) error {
	listParams := &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}

	err := c.awsClient.ListObjectsV2PagesWithContext(ctx, listParams, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		deleteObjects := make([]*s3.ObjectIdentifier, 0, len(page.Contents))
		for _, obj := range page.Contents {
			deleteObjects = append(deleteObjects, &s3.ObjectIdentifier{Key: obj.Key})
		}

		if len(deleteObjects) > 0 {
			for i := 0; i < len(deleteObjects); i += BatchSize {
				end := i + BatchSize
				if end > len(deleteObjects) {
					end = len(deleteObjects)
				}

				batch := deleteObjects[i:end]
				deleteInput := &s3.DeleteObjectsInput{
					Bucket: aws.String(bucket),
					Delete: &s3.Delete{
						Objects: batch,
						Quiet:   aws.Bool(false),
					},
				}

				if _, err := c.awsClient.DeleteObjects(deleteInput); err != nil {
					return false
				}
			}
		}

		return !lastPage
	})

	return err
}

// ListBuckets retrieves all S3 buckets accessible by the client.
func (c *Client) ListBuckets(ctx context.Context) ([]string, error) {
	buckets, err := c.awsClient.ListBucketsWithContext(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("error listing buckets: %w", err)
	}

	ret := make([]string, len(buckets.Buckets))
	for i, bucket := range buckets.Buckets {
		ret[i] = *bucket.Name
	}
	return ret, nil
}

// Object Operations

// GetObject retrieves the object data from the specified bucket and key.
// It returns the data, a boolean indicating if the object was found, and an error if any.
func (c *Client) GetObject(bucket, key string) ([]byte, bool, error) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(defaultTimeout))
	defer cancel()
	return c.GetObjectWithContext(ctx, bucket, key)
}

// GetObjectWithContext retrieves the object data with a provided context.
// It returns the data, a boolean indicating if the object was found, and an error if any.
func (c *Client) GetObjectWithContext(ctx context.Context, bucket, key string) ([]byte, bool, error) {
	buf := aws.NewWriteAtBuffer([]byte{})
	found, err := c.ReadObject(ctx, bucket, key, buf)
	return buf.Bytes(), found, err
}

// GetObjectToStruct retrieves the object data and unmarshals it into the provided struct.
// It returns a boolean indicating if the object was found and an error if any.
func (c *Client) GetObjectToStruct(ctx context.Context, bucket, key string, dataStruct any) (bool, error) {
	obj, found, err := c.GetObjectWithContext(ctx, bucket, key)
	if err != nil || !found {
		return found, err
	}

	err = json.Unmarshal(obj, dataStruct)
	if err != nil {
		return found, fmt.Errorf("unmarshaling object data into struct failed: %w", err)
	}

	return found, nil
}

// PutObject uploads an object to the specified bucket and key with optional metadata.
func (c *Client) PutObject(ctx context.Context, bucket, key string, body io.Reader, metadata map[string]*string) error {
	_, err := c.awsClient.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		Body:     aws.ReadSeekCloser(body),
		Metadata: metadata,
	})
	return err
}

// SaveObjectData uploads raw byte data to the specified bucket and key.
func (c *Client) SaveObjectData(ctx context.Context, bucket, key string, data []byte) error {
	uploader := s3manager.NewUploader(c.awsSess)
	if uploader == nil {
		return errors.New("error creating uploader")
	}

	_, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

// SaveObjectFile uploads a file from the local filesystem to the specified bucket and key.
func (c *Client) SaveObjectFile(ctx context.Context, bucket, key, filePath string) error {
	return c.saveObjectFile(ctx, bucket, key, filePath, nil)
}

// SaveObjectFileWithMetadata uploads a file with additional metadata.
func (c *Client) SaveObjectFileWithMetadata(ctx context.Context, bucket, key, filePath string, metadata map[string]string) error {
	return c.saveObjectFile(ctx, bucket, key, filePath, metadata)
}

// saveObjectFile is a helper function to upload a file with optional metadata.
func (c *Client) saveObjectFile(ctx context.Context, bucket, key, filePath string, metadata map[string]string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()

	return c.saveObjectStream(ctx, bucket, key, f, metadata)
}

// SaveObjectStream uploads data from an io.Reader to the specified bucket and key.
func (c *Client) SaveObjectStream(ctx context.Context, bucket, key string, r io.Reader) error {
	return c.saveObjectStream(ctx, bucket, key, r, nil)
}

// saveObjectStream is a helper function to upload data from an io.Reader with optional metadata.
func (c *Client) saveObjectStream(ctx context.Context, bucket, key string, r io.Reader, metadata map[string]string) error {
	uploader := s3manager.NewUploader(c.awsSess)

	params := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   r,
	}
	if len(metadata) != 0 {
		params.Metadata = aws.StringMap(metadata)
	}

	_, err := uploader.UploadWithContext(ctx, params)
	return err
}

// HasObject checks if an object exists in the specified bucket and key.
func (c *Client) HasObject(bucket, key string) (bool, error) {
	_, err := c.awsClient.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	if IsNotFoundErr(err) {
		return false, nil
	}
	return false, err
}

// ReadObject downloads an object and writes its content to the provided io.WriterAt.
// It returns a boolean indicating if the object was found and an error if any.
func (c *Client) ReadObject(ctx context.Context, bucket, key string, wa io.WriterAt) (bool, error) {
	getParams := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	s3Downloader := s3manager.NewDownloader(c.awsSess)

	var err error
	for i := 0; i < defaultMaxRetries; i++ {
		_, err = s3Downloader.DownloadWithContext(ctx, wa, getParams)
		if err == nil {
			return true, nil
		}

		if IsNotFoundErr(err) {
			return false, nil
		}

		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			break
		}

		time.Sleep(time.Duration(i+3) * time.Second)
	}

	return false, err
}

// StreamObject streams an S3 object directly to an io.Writer.
// It returns a boolean indicating if the object was found and an error if any.
func (c *Client) StreamObject(bucket, key string, w io.Writer) (bool, error) {
	getParams := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	s3Downloader := s3manager.NewDownloader(c.awsSess)
	s3Downloader.Concurrency = 1

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(defaultTimeout))
	defer cancel()

	_, err := s3Downloader.DownloadWithContext(ctx, SyncWriterAt{w}, getParams)
	if err != nil {
		if IsNotFoundErr(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// RenameObject renames an object by copying it to a new key and deleting the old one.
// It returns a boolean indicating if the operation was successful and an error if any.
func (c *Client) RenameObject(bucket, oldKey, newKey string) (bool, error) {
	copyParams := &s3.CopyObjectInput{
		CopySource: aws.String(fmt.Sprintf("%s/%s", bucket, oldKey)),
		Bucket:     aws.String(bucket),
		Key:        aws.String(newKey),
	}

	if _, err := c.awsClient.CopyObject(copyParams); err != nil {
		if IsNotFoundErr(err) {
			return false, nil
		}
		return false, err
	}

	_, err := c.awsClient.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(oldKey),
	})
	if err != nil {
		return false, err
	}

	return true, nil
}

// DeleteObjectSet deletes multiple objects from the specified bucket.
// It ignores "NoSuchKey" errors.
func (c *Client) DeleteObjectSet(ctx context.Context, bucket string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	params := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{
			Quiet: aws.Bool(false),
		},
	}

	for _, key := range keys {
		params.Delete.Objects = append(params.Delete.Objects, &s3.ObjectIdentifier{Key: aws.String(key)})
	}

	result, err := c.awsClient.DeleteObjects(params)
	if err != nil && !IsBucketExistsErr(err) {
		return err
	}

	for _, e := range result.Errors {
		if e.Code != nil && *e.Code == s3.ErrCodeNoSuchKey {
			continue
		}
		// Handle other errors if necessary
	}

	return nil
}

// ListObjects retrieves objects from an S3 bucket with optional prefix and pagination.
// It returns a slice of ObjectInfo, the next continuation token, and an error if any.
func (c *Client) ListObjects(ctx context.Context, bucket, prefix, cursor string, limit int) ([]*ObjectInfo, string, error) {
	listParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}
	if prefix != "" {
		listParams.Prefix = aws.String(prefix)
	}
	if cursor != "" {
		listParams.ContinuationToken = aws.String(cursor)
	}
	if limit > 0 {
		listParams.MaxKeys = aws.Int64(int64(limit))
	}

	var results []*ObjectInfo
	var nextToken string

	onS3Page := func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, record := range page.Contents {
			results = append(results, &ObjectInfo{
				Key:        aws.StringValue(record.Key),
				Size:       aws.Int64Value(record.Size),
				UpdateTime: *record.LastModified,
			})
		}

		if !lastPage {
			nextToken = aws.StringValue(page.NextContinuationToken)
			if limit > 0 && len(results) >= limit {
				return false
			}
		}

		return !lastPage
	}

	err := c.awsClient.ListObjectsV2PagesWithContext(ctx, listParams, onS3Page)
	return results, nextToken, err
}

// ListPageObjects retrieves a single page of objects from an S3 bucket with optional prefix.
// It returns a slice of ObjectInfo, the next continuation token, and an error if any.
func (c *Client) ListPageObjects(ctx context.Context, bucket, prefix, cursor string, count int64) ([]*ObjectInfo, string, error) {
	listParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	if cursor != "" {
		listParams.ContinuationToken = aws.String(cursor)
	}

	if count > 0 {
		listParams.MaxKeys = aws.Int64(count)
	}

	var results []*ObjectInfo

	out, err := c.awsClient.ListObjectsV2WithContext(ctx, listParams)
	if err != nil {
		return nil, "", err
	}

	for _, record := range out.Contents {
		results = append(results, &ObjectInfo{
			Key:        aws.StringValue(record.Key),
			Size:       aws.Int64Value(record.Size),
			UpdateTime: *record.LastModified,
		})
	}

	if aws.BoolValue(out.IsTruncated) {
		cursor = aws.StringValue(out.NextContinuationToken)
	}

	return results, cursor, nil
}

// Signed URL Operations

// GetSignedObjectURL generates a pre-signed URL for accessing an object.
// It uses the default duration limit of 30 minutes.
func (c *Client) GetSignedObjectURL(ctx context.Context, bucket, key string) (string, error) {
	return c.GetSignedObjectURLWithDuration(ctx, bucket, key, DefaultDurationLimit)
}

// GetSignedObjectURLWithDuration generates a pre-signed URL with a specified duration.
func (c *Client) GetSignedObjectURLWithDuration(ctx context.Context, bucket, key string, duration time.Duration) (string, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	req, _ := c.awsClient.GetObjectRequest(params)
	return req.Presign(duration)
}

// GetSignedPutUploadURL generates a pre-signed URL for uploading an object with optional metadata.
// It uses the default duration limit of 30 minutes.
func (c *Client) GetSignedPutUploadURL(ctx context.Context, bucket, key string, metadata map[string]*string) (string, error) {
	return c.GetSignedPutUploadURLWithDuration(ctx, bucket, key, metadata, DefaultDurationLimit)
}

// GetSignedPutUploadURLWithDuration generates a pre-signed URL for uploading with a specified duration.
func (c *Client) GetSignedPutUploadURLWithDuration(ctx context.Context, bucket, key string, metadata map[string]*string, duration time.Duration) (string, error) {
	params := &s3.PutObjectInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		Metadata: metadata,
	}

	req, _ := c.awsClient.PutObjectRequest(params)
	return req.Presign(duration)
}

// External URL Operations

// GetObjectFromURL retrieves an object using a pre-signed URL.
// It returns the object data and an error if any.
func (c *Client) GetObjectFromURL(ctx context.Context, signedURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, signedURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to retrieve object, status code: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

// UploadObjectToURL uploads data to an S3 object using a pre-signed URL.
// It returns an error if the upload fails.
func (c *Client) UploadObjectToURL(ctx context.Context, signedURL string, objectData []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL, bytes.NewReader(objectData))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", contentTypeOctetStream)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to upload object, status code: %d", resp.StatusCode)
	}
	return nil
}

// Helper Functions
const (
	NotFoundErr      = "NotFound"
	AlreadyExistsErr = "AlreadyExists"
)

// IsNotFoundErr checks if the error returned by AWS SDK is a NotFound error.
func IsNotFoundErr(err error) bool {
	// Implement logic to determine if the error is a NotFound error.
	// This typically involves type assertion and checking the error code.
	// Example:
	if aerr, ok := err.(awserr.Error); ok {
		return aerr.Code() == s3.ErrCodeNoSuchKey
	}
	return false
}

// IsBucketExistsErr checks if the error returned by AWS SDK indicates that the bucket already exists.
func IsBucketExistsErr(err error) bool {
	// Implement logic to determine if the error is a BucketExists error.
	// This typically involves type assertion and checking the error code.
	// Example:
	if aerr, ok := err.(awserr.Error); ok {
		return aerr.Code() == s3.ErrCodeBucketAlreadyOwnedByYou || aerr.Code() == s3.ErrCodeBucketAlreadyExists
	}
	return false
}

// IsBucketNotFoundErr checks if the given error is a "Bucket Not Found" error, including AWS S3 specific "NoSuchBucket" error.
// It returns true if the error is a "Bucket Not Found" error, otherwise false.
func IsBucketNotFoundErr(err error) bool {
	var aerr awserr.Error
	if errors.As(err, &aerr) {
		switch aerr.Code() {
		case NotFoundErr, s3.ErrCodeNoSuchBucket:
			return true
		}
	}
	return false
}

// IsObjectNotFoundErr checks if the given error is an "Object Not Found" error, including AWS S3 specific "NoSuchKey" error.
// It returns true if the error is an "Object Not Found" error, otherwise false.
func IsObjectNotFoundErr(err error) bool {
	var aerr awserr.Error
	if errors.As(err, &aerr) {
		switch aerr.Code() {
		case NotFoundErr, s3.ErrCodeNoSuchKey:
			return true
		}
	}
	return false
}
