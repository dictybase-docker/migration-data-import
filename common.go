package main

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	minio "github.com/minio/minio-go"

	"gopkg.in/urfave/cli.v1"
)

func validateArgs(c *cli.Context) error {
	if !definedPostgres(c) && !definedChadoUser(c) {
		return cli.NewExitError("no database information", 2)
	}
	return nil
}

func validateS3Args(c *cli.Context) error {
	for _, p := range []string{"s3-server", "s3-bucket", "access-key", "secret-key"} {
		if !c.IsSet(p) {
			return cli.NewExitError(fmt.Sprintf("argument %s is missing", p), 2)
		}
	}
	return nil
}

func definedPostgres(c *cli.Context) bool {
	if len(c.GlobalString("pghost")) > 1 && len(c.GlobalString("pgport")) > 1 {
		return true
	}
	return false
}

func definedChadoUser(c *cli.Context) bool {
	if len(c.GlobalString("chado-user")) > 1 && len(c.GlobalString("chado-db")) > 1 && len(c.GlobalString("chado-pass")) > 1 {
		return true
	}
	return false
}

func untar(src, target string) error {
	reader, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("could not open file reading %s", err)
	}
	defer reader.Close()
	archive, err := gzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("could not read from gzip file %s", err)
	}
	defer archive.Close()
	tarReader := tar.NewReader(archive)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		path := filepath.Join(target, header.Name)
		info := header.FileInfo()
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, info.Mode()); err != nil {
				return err
			}
		case tar.TypeReg:
			file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = io.Copy(file, tarReader)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unable to figure out the file type of tar archive %s %s", header.Typeflag, err)
		}
	}
	return nil
}

func getS3Client(c *cli.Context) (*minio.Client, error) {
	s3Client, err := minio.New(
		c.GlobalString("s3-server"),
		c.GlobalString("access-key"),
		c.GlobalString("secret-key"),
		true,
	)
	if err != nil {
		return s3Client, fmt.Errorf("unable create the client %s", err.Error())
	}
	return s3Client, nil
}

func fetchRemoteFile(c *cli.Context, name string) (string, error) {
	s3Client, err := getS3Client(c)
	if err != nil {
		return "", err
	}
	tmpf, err := ioutil.TempFile("", name)
	if err != nil {
		return "", err
	}
	defer os.Remove(tmpf.Name())
	if err := s3Client.FGetObject(c.GlobalString("s3-bucket"), c.String("remote-path"), tmpf.Name()); err != nil {
		return "", fmt.Errorf("Unable to retrieve the object %s", err.Error(), 2)
	}
	return tmpf.Name(), nil
}
