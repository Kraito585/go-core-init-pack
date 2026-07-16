package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client struct {
	svc    *s3.Client
	bucket string
}

func NewS3Client(ctx context.Context, endpoint, region, accessKey, secretKey, bucket string) (*S3Client, error) {
	// Инициализируем статичные креды (без попыток лезть в профили AWS)
	creds := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(creds),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("ошибка загрузки конфигурации s3: %w", err)
	}

	// Настраиваем сам S3-клиент под Selectel / RustFS
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // Критически важно для совместимости с не-AWS серверами!
	})

	return &S3Client{
		svc:    client,
		bucket: bucket,
	}, nil
}

// Upload загружает файл в бакет
func (s *S3Client) Upload(ctx context.Context, fileName string, fileBody io.Reader, contentType string) error {
	uploader := manager.NewUploader(s.svc)

	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(fileName),
		Body:        fileBody,
		ContentType: aws.String(contentType),
	})

	if err != nil {
		return fmt.Errorf("ошибка загрузки файла %s в S3: %w", fileName, err)
	}

	return nil
}
// Download возвращает тело файла из бакета (не забудь сделать defer body.Close() там, где вызовешь)
func (s *S3Client) Download(ctx context.Context, fileName string) (io.ReadCloser, error) {
	out, err := s.svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileName),
	})
	if err != nil {
		return nil, fmt.Errorf("ошибка скачивания файла %s из S3: %w", fileName, err)
	}
	return out.Body, nil
}

// Ping проверяет доступность бакета (отправляет HeadBucket запрос)
func (s *S3Client) Ping(ctx context.Context) error {
	_, err := s.svc.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return fmt.Errorf("бакет недоступен: %w", err)
	}
	return nil
}
