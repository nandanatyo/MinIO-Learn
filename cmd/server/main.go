package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinIOConfig struct {
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

type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type FileInfo struct {
	FileName    string    `json:"fileName"`
	Size        int64     `json:"size"`
	ContentType string    `json:"contentType"`
	URL         string    `json:"url,omitempty"`
	UploadedAt  time.Time `json:"uploadedAt"`
}

var minioService *MinIOService

func main() {
	config, err := loadMinIOConfig()
	if err != nil {
		log.Fatalf("Failed to load MinIO configuration: %v", err)
	}

	minioService, err = newMinIOService(config)
	if err != nil {
		log.Fatalf("Failed to initialize MinIO service: %v", err)
	}

	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/files", listFilesHandler)
	http.HandleFunc("/files/", getFileHandler)
	http.HandleFunc("/health", healthCheckHandler)

	port := getEnv("PORT", "8080")
	log.Printf("Server starting on port %s...", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func loadMinIOConfig() (MinIOConfig, error) {
	config := MinIOConfig{
		Endpoint:        getEnv("MINIO_ENDPOINT", "localhost:9000"),
		AccessKeyID:     getEnv("MINIO_ACCESS_KEY", "minio_admin"),
		SecretAccessKey: getEnv("MINIO_SECRET_KEY", "minio_password"),
		UseSSL:          getEnvBool("MINIO_USE_SSL", false),
		BucketName:      getEnv("MINIO_BUCKET", "mybucket"),
		Location:        getEnv("MINIO_LOCATION", "us-east-1"),
	}

	if config.Endpoint == "" {
		return config, fmt.Errorf("MINIO_ENDPOINT is required")
	}
	if config.AccessKeyID == "" {
		return config, fmt.Errorf("MINIO_ACCESS_KEY is required")
	}
	if config.SecretAccessKey == "" {
		return config, fmt.Errorf("MINIO_SECRET_KEY is required")
	}
	if config.BucketName == "" {
		return config, fmt.Errorf("MINIO_BUCKET is required")
	}

	return config, nil
}

func newMinIOService(config MinIOConfig) (*MinIOService, error) {
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

	err = service.ensureBucket()
	if err != nil {
		return nil, fmt.Errorf("failed to ensure bucket exists: %w", err)
	}

	log.Printf("MinIO service initialized successfully (endpoint: %s, bucket: %s)", config.Endpoint, config.BucketName)
	return service, nil
}

func (s *MinIOService) ensureBucket() error {
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
		log.Printf("Bucket '%s' created successfully", s.BucketName)
	} else {
		log.Printf("Bucket '%s' already exists", s.BucketName)
	}

	return nil
}

func (s *MinIOService) uploadFile(objectName, filePath, contentType string) (minio.UploadInfo, error) {
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

	log.Printf("File '%s' uploaded successfully as '%s' (size: %d bytes)", filePath, objectName, uploadInfo.Size)
	return uploadInfo, nil
}

func (s *MinIOService) downloadFile(objectName, filePath string) error {
	ctx := context.Background()
	err := s.Client.FGetObject(ctx, s.BucketName, objectName, filePath, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}

	log.Printf("File '%s' downloaded successfully to '%s'", objectName, filePath)
	return nil
}

func (s *MinIOService) downloadBuffer(objectName string) ([]byte, error) {
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

	log.Printf("File '%s' downloaded successfully as buffer (size: %d bytes)", objectName, len(data))
	return data, nil
}

func (s *MinIOService) listObjects(prefix string) ([]minio.ObjectInfo, error) {
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

	log.Printf("Listed %d objects with prefix '%s'", len(objects), prefix)
	return objects, nil
}

func (s *MinIOService) deleteObject(objectName string) error {
	ctx := context.Background()
	err := s.Client.RemoveObject(ctx, s.BucketName, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	log.Printf("Object '%s' deleted successfully", objectName)
	return nil
}

func (s *MinIOService) getObjectURL(objectName string, expiry time.Duration) (string, error) {
	ctx := context.Background()
	presignedURL, err := s.Client.PresignedGetObject(ctx, s.BucketName, objectName, expiry, nil)
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL: %w", err)
	}

	log.Printf("Generated presigned URL for '%s' (valid for %v)", objectName, expiry)
	return presignedURL.String(), nil
}

func (s *MinIOService) checkObjectExists(objectName string) (bool, error) {
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

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendResponse(w, false, "Method not allowed", nil, http.StatusMethodNotAllowed)
		return
	}

	r.ParseMultipartForm(10 << 20)

	file, handler, err := r.FormFile("file")
	if err != nil {
		sendResponse(w, false, "Error retrieving file: "+err.Error(), nil, http.StatusBadRequest)
		return
	}
	defer file.Close()

	tempFile, err := os.CreateTemp("", "upload-*"+filepath.Ext(handler.Filename))
	if err != nil {
		sendResponse(w, false, "Error creating temporary file: "+err.Error(), nil, http.StatusInternalServerError)
		return
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	_, err = io.Copy(tempFile, file)
	if err != nil {
		sendResponse(w, false, "Error saving temporary file: "+err.Error(), nil, http.StatusInternalServerError)
		return
	}
	tempFile.Close()

	objectName := fmt.Sprintf("uploads/%d-%s", time.Now().Unix(), handler.Filename)

	contentType := handler.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	uploadInfo, err := minioService.uploadFile(objectName, tempFile.Name(), contentType)
	if err != nil {
		sendResponse(w, false, "Error uploading to MinIO: "+err.Error(), nil, http.StatusInternalServerError)
		return
	}

	url, err := minioService.getObjectURL(objectName, time.Hour*24)
	if err != nil {
		log.Printf("Warning: Failed to generate presigned URL: %v", err)
	}

	fileInfo := FileInfo{
		FileName:    handler.Filename,
		Size:        uploadInfo.Size,
		ContentType: contentType,
		URL:         url,
		UploadedAt:  time.Now(),
	}

	sendResponse(w, true, "File uploaded successfully", fileInfo, http.StatusOK)
}

func listFilesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		sendResponse(w, false, "Method not allowed", nil, http.StatusMethodNotAllowed)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	if prefix == "" {
		prefix = "uploads/"
	}

	objects, err := minioService.listObjects(prefix)
	if err != nil {
		sendResponse(w, false, "Error listing files: "+err.Error(), nil, http.StatusInternalServerError)
		return
	}

	var fileList []FileInfo
	for _, obj := range objects {
		url, _ := minioService.getObjectURL(obj.Key, time.Hour*24)

		fileList = append(fileList, FileInfo{
			FileName:    filepath.Base(obj.Key),
			Size:        obj.Size,
			ContentType: obj.ContentType,
			URL:         url,
			UploadedAt:  obj.LastModified,
		})
	}

	sendResponse(w, true, fmt.Sprintf("Found %d files", len(fileList)), fileList, http.StatusOK)
}

func getFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		sendResponse(w, false, "Method not allowed", nil, http.StatusMethodNotAllowed)
		return
	}

	objectName := r.URL.Path[len("/files/"):]
	if objectName == "" {
		sendResponse(w, false, "Object name is required", nil, http.StatusBadRequest)
		return
	}

	exists, err := minioService.checkObjectExists(objectName)
	if err != nil {
		sendResponse(w, false, "Error checking object: "+err.Error(), nil, http.StatusInternalServerError)
		return
	}

	if !exists {
		sendResponse(w, false, "File not found", nil, http.StatusNotFound)
		return
	}

	download := r.URL.Query().Get("download") == "true"

	if download {
		data, err := minioService.downloadBuffer(objectName)
		if err != nil {
			sendResponse(w, false, "Error downloading file: "+err.Error(), nil, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filepath.Base(objectName)))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))

		w.WriteHeader(http.StatusOK)
		w.Write(data)
	} else {
		url, err := minioService.getObjectURL(objectName, time.Hour)
		if err != nil {
			sendResponse(w, false, "Error generating URL: "+err.Error(), nil, http.StatusInternalServerError)
			return
		}

		http.Redirect(w, r, url, http.StatusFound)
	}
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	_, err := minioService.listObjects("")
	if err != nil {
		sendResponse(w, false, "MinIO service is not healthy: "+err.Error(), nil, http.StatusServiceUnavailable)
		return
	}

	sendResponse(w, true, "Service is healthy", nil, http.StatusOK)
}

func sendResponse(w http.ResponseWriter, success bool, message string, data interface{}, statusCode int) {
	response := Response{
		Success: success,
		Message: message,
		Data:    data,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
	}
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvBool(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}

	return boolValue
}
