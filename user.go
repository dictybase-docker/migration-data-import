package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"gopkg.in/mgutz/dat.v1"
	"gopkg.in/mgutz/dat.v1/sqlx-runner"
	"gopkg.in/urfave/cli.v1"
)

const upSertUser = `
INSERT INTO auth_user (email,first_name,last_name,is_active)
VALUES ($1,$2,$3,$4)
ON CONFLICT (email)
DO UPDATE
SET first_name = $2,
	last_name = $3,
	is_active = $4
RETURNING auth_user_id
	`

func userAction(c *cli.Context) error {
	dat.EnableInterpolation = true
	// database connection
	dbh, err := getPgWrapper(c)
	if err != nil {
		return cli.NewExitError(
			fmt.Sprint("Unable to create database connection %s", err),
			2,
		)
	}
	log, err := getLogger(c, "user")
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	filename, err := fetchRemoteFile(c, "users")
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "remote-get",
			"name": "input",
		}).Error(err)
		return cli.NewExitError(fmt.Sprintf("unable to fetch remote file %s ", err), 2)
	}
	log.Infof("retrieved the remote file %s", filename)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "users")
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "temp-dir",
			"name": "input",
		}).Error(err)
		return cli.NewExitError(fmt.Sprintf("unable to create temp directory %s", err), 2)
	}
	log.Debugf("create a temp folder %s", tmpDir)
	err = untar(filename, tmpDir)
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "untar",
			"name": "input",
		}).Error(err)
		return cli.NewExitError(fmt.Sprintf("error in untarring file %s", err), 2)
	}
	log.Debugf("untar file %s in %s temp folder", filename, tmpDir)

	// open the csv file for reading
	usersFile := filepath.Join(tmpDir, "users.csv")
	handler, err := os.Open(usersFile)
	if err != nil {
		return cli.NewExitError(
			fmt.Sprintf("Unable to open file %s %s", usersFile, err),
			2,
		)
	}
	defer handler.Close()
	r := csv.NewReader(handler)
	_, err = r.Read()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return cli.NewExitError(
			fmt.Sprintf("Unable to read header from csv file %s", err),
			2,
		)
	}
	// variable for new records
	allRecords := [][]string{}
	var idsDelete []int64
	tx, err := dbh.Begin()
	if err != nil {
		log.Errorf("error in starting transaction %s", err)
		return cli.NewExitError(
			fmt.Sprintf("error in starting transaction %s", err),
			2,
		)
	}
	defer tx.AutoRollback()
	// read the file and insert record as needed
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return cli.NewExitError(
				fmt.Sprintf("Unable to read from csv file %s", err),
				2,
			)
		}
		id, err := upSertRecord(tx, record, log)
		if err != nil {
			return err
		}
		allRecords = append(allRecords, record)
		idsDelete = append(idsDelete, id)
	}
	log.Infof("upserted %d records", len(allRecords))
	if err := deleteAllUsersInfo(tx, idsDelete, log); err != nil {
		return err
	}
	builder := tx.InsertInto("auth_user_info").
		Columns("organization",
			"group_name",
			"first_address",
			"second_address",
			"city",
			"state",
			"zipcode",
			"country",
			"phone",
			"auth_user_id",
		)
	for i, r := range allRecords {
		builder.Record(newUserInfo(r, idsDelete[i]))
	}
	res, err := builder.Exec()
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "bulk insert",
		}).Error(err)
		return cli.NewExitError(
			fmt.Sprintf("error in bulk inserting records %s ", err),
			2,
		)
	}
	err = tx.Commit()
	if err != nil {
		return cli.NewExitError(
			fmt.Sprintf("error in commiting %s", err),
			2,
		)
	}
	log.Infof("inserted %d user information", res.RowsAffected)
	return nil
}

func deleteAllUsersInfo(tx *runner.Tx, ids []int64, log *logrus.Logger) error {
	res, err := tx.DeleteFrom("auth_user_info").Where("auth_user_id IN $1", ids).Exec()
	if err != nil {
		log.WithFields(logrus.Fields{
			"type":  "delete",
			"field": "auth_user_id",
		}).Error(err)
		return cli.NewExitError(
			fmt.Sprintf("error in deleting all user information %s", err),
			2,
		)
	}
	log.Infof("deleted %d records", res.RowsAffected)
	return nil
}

func upSertRecord(tx *runner.Tx, record []string, log *logrus.Logger) (int64, error) {
	var id int64
	err := tx.SQL(upSertUser, record[0], record[1], record[2], getActiveStatus(record)).QueryScalar(&id)
	if err != nil {
		log.WithFields(logrus.Fields{
			"type":  "query",
			"field": "email",
			"value": record[0],
		}).Error(err)
		return id, cli.NewExitError(
			fmt.Sprintf("error in upserting record %s", err),
			2,
		)
	}
	return id, nil
}

func newUserInfo(record []string, id int64) *UserInfo {
	uInfo := new(UserInfo)
	for i, v := range record {
		switch i {
		case 6:
			if len(v) > 0 {
				uInfo.Organization = dat.NullStringFrom(v)
			}
		case 7:
			if len(v) > 0 {
				uInfo.FirstAddress = dat.NullStringFrom(v)
			}
		case 8:
			if len(v) > 0 {
				uInfo.SecondAddress = dat.NullStringFrom(v)
			}
		case 9:
			if len(v) > 0 {
				uInfo.City = dat.NullStringFrom(v)
			}
		case 10:
			if len(v) > 0 {
				uInfo.State = dat.NullStringFrom(v)
			}
		case 12:
			if len(v) > 0 {
				uInfo.Country = dat.NullStringFrom(v)
			}
		case 13:
			if len(v) > 0 {
				uInfo.Zipcode = dat.NullStringFrom(v)
			}
		case 15:
			if len(v) > 0 {
				uInfo.Phone = dat.NullStringFrom(v)
			}
		}
	}
	uInfo.UserID = id
	return uInfo
}

func getActiveStatus(record []string) bool {
	if len(record[14]) > 0 {
		if record[14] == "Y" {
			return true
		}
	}
	return false
}
