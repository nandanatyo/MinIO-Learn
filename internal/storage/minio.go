package storage

import (
	"bytes"
	"context"
	"fmt"
	io "io"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Config struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	BucketName      string
	Location        string
}

type MinIOService struct {
	Client     *minio.Client
	BucketName string
	Location   string
}

func NewMinIOService(config Config) (*MinIOService, error) {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize MinIO client: %w", err)
	}

	service := &MinIOService{
		Client:     client,
		BucketName: config.BucketName,
		Location:   config.Location,
	}

	err = service.EnsureBucket()
	if err != nil {
		return nil, fmt.Errorf("failed to ensure bucket exists: %w", err)
	}

	return service, nil
}

func (s *MinIOService) EnsureBucket() error {
	ctx := context.Background()
	exists, err := s.Client.BucketExists(ctx, s.BucketName)
	if err != nil {
		return fmt.Errorf("failed to check if bucket exists: %w", err)
	}

	if !exists {
		err = s.Client.MakeBucket(ctx, s.BucketName, minio.MakeBucketOptions{Region: s.Location})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	return nil
}

func (s *MinIOService) UploadFile(objectName, filePath, contentType string) (minio.UploadInfo, error) {
	ctx := context.Background()
	file, err := os.Open(filePath)
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("failed to get file stats: %w", err)
	}

	uploadInfo, err := s.Client.PutObject(ctx, s.BucketName, objectName, file, fileInfo.Size(),
		minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("failed to upload file: %w", err)
	}

	return uploadInfo, nil
}

func (s *MinIOService) UploadBuffer(objectName string, data []byte, contentType string) (minio.UploadInfo, error) {
	ctx := context.Background()
	reader := bytes.NewReader(data)
	uploadInfo, err := s.Client.PutObject(ctx, s.BucketName, objectName, reader, int64(len(data)),
		minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("failed to upload data: %w", err)
	}

	return uploadInfo, nil
}

func (s *MinIOService) DownloadFile(objectName, filePath string) error {
	ctx := context.Background()
	err := s.Client.FGetObject(ctx, s.BucketName, objectName, filePath, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}

	return nil
}

func (s *MinIOService) DownloadBuffer(objectName string) ([]byte, error) {
	ctx := context.Background()
	obj, err := s.Client.GetObject(ctx, s.BucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read object data: %w", err)
	}

	return data, nil
}

func (s *MinIOService) ListObjects(prefix string) ([]minio.ObjectInfo, error) {
	ctx := context.Background()
	objectCh := s.Client.ListObjects(ctx, s.BucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	var objects []minio.ObjectInfo
	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("error listing objects: %w", object.Err)
		}
		objects = append(objects, object)
	}

	return objects, nil
}

func (s *MinIOService) DeleteObject(objectName string) error {
	ctx := context.Background()
	err := s.Client.RemoveObject(ctx, s.BucketName, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	return nil
}

func (s *MinIOService) GetObjectURL(objectName string, expiry time.Duration) (string, error) {
	ctx := context.Background()
	presignedURL, err := s.Client.PresignedGetObject(ctx, s.BucketName, objectName, expiry, nil)
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL: %w", err)
	}

	return presignedURL.String(), nil
}

func (s *MinIOService) CheckObjectExists(objectName string) (bool, error) {
	ctx := context.Background()
	_, err := s.Client.StatObject(ctx, s.BucketName, objectName, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if object exists: %w", err)
	}

	return true, nil
}
