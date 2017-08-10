package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	minio "github.com/minio/minio-go"
	"github.com/sirupsen/logrus"

	"gopkg.in/urfave/cli.v1"
)

func validateCommon(c *cli.Context) error {
	if err := validateArgs(c); err != nil {
		return err
	}
	if err := validateS3Args(c); err != nil {
		return err
	}
	return nil
}

func validateArgs(c *cli.Context) error {
	if !definedPostgres(c) && !definedChadoUser(c) {
		return cli.NewExitError("no database information", 2)
	}
	return nil
}

func validateS3Args(c *cli.Context) error {
	for _, p := range []string{"s3-server", "s3-bucket", "access-key", "secret-key"} {
		if len(c.GlobalString(p)) == 0 {
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
	//defer os.Remove(tmpf.Name())
	if err := s3Client.FGetObject(c.GlobalString("s3-bucket"), c.String("remote-path"), tmpf.Name()); err != nil {
		return "", fmt.Errorf("Unable to retrieve the object %s", err.Error(), 2)
	}
	return tmpf.Name(), nil
}

func uploadLocalFile(c *cli.Context, file string) error {
	s3Client, err := getS3Client(c)
	if err != nil {
		return err
	}
	_, err = s3Client.FPutObject(
		c.GlobalString("s3-bucket"),
		file,
		filepath.Join(c.GlobalString("remote-log-path"), filepath.Base(file)),
		"application/zip",
	)
	if err != nil {
		return fmt.Errorf("unable to upload %s file %s", file, err)
	}
	return nil
}

func listFiles(dir string) ([]string, error) {
	r, err := os.Open(dir)
	if err != nil {
		return []string{""}, err
	}
	defer r.Close()
	entries, err := r.Readdir(-1)
	if err != nil {
		return []string{""}, err
	}
	var files []string
	for _, f := range entries {
		if !f.IsDir() {
			files = append(files, filepath.Join(dir, f.Name()))
		}
	}
	return files, nil
}

func zipFiles(folder string, output string) error {
	w, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("unable to create zip file %s %s", output, err)
	}
	zw := zip.NewWriter(w)
	files, err := listFiles(folder)
	if err != nil {
		return fmt.Errorf("unable to reader folder %s %s", folder, err)
	}
	for _, entry := range files {
		zf, err := zw.Create(filepath.Base(entry))
		if err != nil {
			return fmt.Errorf("unable to create file %s %s", entry, err)
		}
		ct, err := ioutil.ReadFile(entry)
		if err != nil {
			return fmt.Errorf("unable to read file content %s %s", entry, err)
		}
		_, err = zf.Write(ct)
		if err != nil {
			return fmt.Errorf("unable to write content of file %s %s", entry, err)
		}
	}
	err = w.Close()
	if err != nil {
		return fmt.Errorf("unable to close the zip writer %s", err)
	}
	return nil
}

func fetchAndDecompress(c *cli.Context, log *logrus.Logger, name string) (string, error) {
	filename, err := fetchRemoteFile(c, name)
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "remote-get",
			"name": "input",
		}).Error(err)
		return "", cli.NewExitError(fmt.Sprintf("unable to fetch remote file %s ", err), 2)
	}
	log.Infof("retrieved the remote file %s", filename)

	tmpDir, err := ioutil.TempDir(os.TempDir(), name)
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "temp-dir",
			"name": "input",
		}).Error(err)
		return "", cli.NewExitError(fmt.Sprintf("unable to create temp directory %s", err), 2)
	}
	log.Debugf("create a temp folder %s", tmpDir)

	err = untar(filename, tmpDir)
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "untar",
			"name": "input",
		}).Error(err)
		return "", cli.NewExitError(fmt.Sprintf("error in untarring file %s", err), 2)
	}
	log.Debugf("untar file %s in %s temp folder", filename, tmpDir)
	return tmpDir, nil
}
