package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/mgutz/dat.v1"
	sqlx "gopkg.in/mgutz/dat.v1/sqlx-runner"
	"gopkg.in/urfave/cli.v1"
)

const orderDateLayout = "2006-01-02 15:04:05"

var strainData []string = []string{
	"characteristics",
	"publications",
	"props",
	"parent",
	"genotype",
	"inventory",
}

var plasmidData []string = []string{
	"publications",
	"props",
	"images",
	"inventory",
}

type cmdFunc func(*cli.Context, string, string, *logrus.Logger) error

var inventorySQL = `
	SELECT DISTINCT stock.stock_id
    FROM stock
		JOIN stockprop sprop on stock.stock_id = sprop.stock_id
		JOIN cvterm inventory on inventory.cvterm_id = sprop.type_id
		JOIN  cv on cv.cv_id = inventory.cv_id
		JOIN cvterm on cvterm.cvterm_id = stock.type_id
		JOIN cv cv2 ON cv2.cv_id = cvterm.cv_id
    WHERE cvterm.name IN('strain','plasmid')
    AND cv2.name = 'dicty_stockcenter'
	AND cv.name IN('strain_inventory','plasmid_inventory')
		EXCEPT
	SELECT stock.stock_id
		FROM stock
		JOIN cvterm cvt ON cvt.cvterm_id = stock.type_id
		JOIN cv ON cv.cv_id = cvt.cv_id
		JOIN stockprop sprop ON stock.stock_id = sprop.stock_id
		JOIN cvterm inventory on inventory.cvterm_id = sprop.type_id
		JOIN cv cv2 On cv2.cv_id = inventory.cv_id
	WHERE cvt.name IN ('strain','plasmid')
	AND cv.name = 'dicty_stockcenter'
	AND inventory.name = 'is_available'
	AND cv2.name = 'dicty_stockcenter'
`

func TagInventoryAction(c *cli.Context) error {
	log := getLogger(c)
	dat.EnableInterpolation = true
	// database connection
	dbh, err := getPgWrapper(c)
	if err != nil {
		log.Errorf("unable to create database connection %s", err)
		return cli.NewExitError(
			fmt.Sprint("Unable to create database connection %s", err),
			2,
		)
	}
	tx, err := dbh.Begin()
	if err != nil {
		log.Errorf("error in starting transaction %s", err)
		return cli.NewExitError(
			fmt.Sprintf("error in starting transaction %s", err),
			2,
		)
	}
	defer tx.AutoRollback()
	cvtermId, err := findOrCreateCvterm(
		"dicty_stockcenter",
		"is_available",
		tx,
	)
	if err != nil {
		log.Errorf("error with cvterm finding %s", err)
		return cli.NewExitError(err.Error(), 2)
	}
	var stockIds []int64
	err = tx.SQL(inventorySQL).QuerySlice(&stockIds)
	if err != nil {
		if err == dat.ErrNotFound {
			log.Info("all the stock inventories are tagged")
			return nil
		}
		log.WithFields(logrus.Fields{
			"type":  "select",
			"value": "list of inventories",
		}).Error(err)
		return cli.NewExitError(
			fmt.Sprintf("error in querying for list of inventories %s", err),
			2,
		)
	}
	if len(stockIds) == 0 {
		log.Info("all the stock inventories are tagged")
		return nil
	}

	insertBuilder := tx.InsertInto("stockprop").
		Columns("type_id", "value", "stock_id")
	expected := 0
	for _, id := range stockIds {
		insertBuilder.Record(&StockInventoryTag{
			StockID: id,
			TypeID:  cvtermId,
			Value:   "1",
		})
		expected++
	}
	res, err := insertBuilder.Exec()
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "bulk insert",
		}).Error(err)
		return cli.NewExitError(
			fmt.Sprintf("error in bulk insert in tagging stock inventories  %s", err),
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
	log.WithFields(logrus.Fields{
		"type":     "bulk insert",
		"expected": expected,
		"count":    res.RowsAffected,
	}).Info("inserted stock inventories")
	return nil
}

func findOrCreateCvterm(cv string, cvterm string, tx *sqlx.Tx) (int64, error) {
	var cvtermId int64
	cvId, err := findCvId(cv, tx)
	if err != nil {
		return cvtermId, err
	}
	dbxrefId, err := findOrCreateDbxref("internal", cvterm, tx)
	if err != nil {
		return cvtermId, err
	}
	err = tx.Insect("cvterm").
		Columns("name", "definition", "cv_id", "dbxref_id").
		Values("is_available", "Model availability of stocks", cvId, dbxrefId).
		Where("cv_id = $1 AND name = $2 and dbxref_id = $3", cvId, "is_available", dbxrefId).
		Returning("cvterm_id").
		QueryScalar(&cvtermId)
	if err != nil {
		return cvtermId, fmt.Errorf("error in finding or creating cvterm %s %s", cvterm, err)
	}
	return cvtermId, nil
}

func findOrCreateDbxref(db string, dbxref string, tx *sqlx.Tx) (int64, error) {
	var dbxrefId int64
	dbId, err := findDbId(db, tx)
	if err != nil {
		return dbId, err
	}
	err = tx.Insect("dbxref").
		Columns("accession", "db_id").
		Values(dbxref, dbId).
		Where("db_id = $1 AND accession = $2", dbId, dbxref).
		Returning("dbxref_id").
		QueryScalar(&dbxrefId)
	if err != nil {
		return dbxrefId, fmt.Errorf("error in finding or creating dbxref %s %s", dbxref, err)
	}
	return dbxrefId, nil
}

func findCvId(cv string, tx *sqlx.Tx) (int64, error) {
	var cvId int64
	err := tx.Select("cv_id").From("cv").Where("name = $1", cv).QueryScalar(&cvId)
	if err != nil {
		if err == dat.ErrNotFound {
			return cvId, fmt.Errorf("%s cvterm not found", cv)
		}
		return cvId, fmt.Errorf("select error %s", err)

	}
	return cvId, nil
}

func findDbId(db string, tx *sqlx.Tx) (int64, error) {
	var dbId int64
	err := tx.Select("db_id").From("db").Where("name = $1", db).QueryScalar(&dbId)
	if err != nil {
		if err == dat.ErrNotFound {
			return dbId, fmt.Errorf("%s db name  not found", db)
		}
		return dbId, fmt.Errorf("select error %s", err)

	}
	return dbId, nil
}

func PrefixPlasmidAction(c *cli.Context) error {
	log := getLogger(c)
	dat.EnableInterpolation = true
	// database connection
	dbh, err := getPgWrapper(c)
	if err != nil {
		log.Errorf("unable to create database connection %s", err)
		return cli.NewExitError(
			fmt.Sprint("Unable to create database connection %s", err),
			2,
		)
	}
	tx, err := dbh.Begin()
	if err != nil {
		log.Errorf("error in starting transaction %s", err)
		return cli.NewExitError(
			fmt.Sprintf("error in starting transaction %s", err),
			2,
		)
	}
	defer tx.AutoRollback()
	plasmidSQL := `SELECT stock.name,stock.stock_id from stock
	JOIN cvterm ON cvterm.cvterm_id = stock.type_id
	JOIN cv ON cv.cv_id = cvterm.cv_id
	WHERE cvterm.name = 'plasmid'
	AND cv.name = 'dicty_stockcenter'
	AND stock.name !~ '^p'
	`
	var plasmids []*PlasmidName
	err = tx.SQL(plasmidSQL).QueryStructs(&plasmids)
	if err != nil {
		log.WithFields(logrus.Fields{
			"type":  "select",
			"value": "list of plasmid",
		}).Error(err)
		return cli.NewExitError(
			fmt.Sprintf("error in querying for list of plasmid %s", err),
			2,
		)
	}
	if len(plasmids) == 0 {
		log.Info("all plasmids are prefixed with p")
		return nil
	}
	count := 0
	for _, p := range plasmids {
		res, err := tx.Update("stock").
			Set("name", "p"+p.Name).
			Where("stock_id = $1", p.StockID).
			Exec()
		if err != nil {
			log.Errorf("unable to prefix plasmid %s %s", p.Name, err)
			cli.NewExitError(
				fmt.Sprintf("unable to prefix plasmid %s %s", p.Name, err),
				2,
			)
		}
		count = count + int(res.RowsAffected)
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("error in commiting %s", err)
		return cli.NewExitError(
			fmt.Sprintf("error in commiting %s", err),
			2,
		)
	}
	log.Infof("updated %d records of %d plasmids", len(plasmids), count)
	return nil
}

func ScAction(c *cli.Context) error {
	log := getLogger(c)
	mi, err := exec.LookPath("modware-import")
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "binary-lookup",
			"name": "modware-import",
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	log.Debugf("successfully looked up command %s", mi)

	filename, err := fetchRemoteFile(c, "dsc")
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "remote-get",
			"name": "input",
		}).Error(err)
		return cli.NewExitError(fmt.Sprintf("unable to fetch remote file %s ", err), 2)
	}
	log.Infof("retrieved the remote file %s", filename)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "dsc")
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

	allfuncs := []cmdFunc{runStrainImport, runPlasmidImport, runStrainPlasmidImport}
	for _, cf := range allfuncs {
		if err := cf(c, tmpDir, mi, log); err != nil {
			return cli.NewExitError(err.Error(), 2)
		}
	}
	return nil
}

func runStrainImport(c *cli.Context, tmpDir string, mainCmd string, log *logrus.Logger) error {
	cmd := makeStrainImportCmd(c, tmpDir)
	for i, data := range strainData {
		rcmd := make([]string, len(cmd))
		copy(rcmd, cmd)
		rcmd = append(rcmd, data)
		if i == 0 && c.Bool("prune") {
			rcmd = append(rcmd, "--prune")
		}
		if c.GlobalBool("use-logfile") {
			logf, err := getLogFileName(c, data)
			if err != nil {
				log.WithFields(logrus.Fields{
					"type": "logfile",
					"name": "name-generation",
				}).Error(err)
				return cli.NewExitError(err.Error(), 2)
			}
			log.Debugf("logfile %s for data %s", logf, data)
			rcmd = append(rcmd, "--logfile", logf, "--log_level", "info")
		}
		out, err := exec.Command(mainCmd, rcmd...).CombinedOutput()
		if err != nil {
			log.WithFields(logrus.Fields{
				"type":        "modware-import",
				"name":        "dictystrain2chado",
				"status":      string(out),
				"data":        data,
				"commandline": strings.Join(rcmd, " "),
			}).Error(err)
			return cli.NewExitError(err.Error(), 2)
		}
		log.Infof("successfully ran command %s", strings.Join(rcmd, " "))
	}
	pcmd := make([]string, len(cmd))
	copy(pcmd, cmd)
	pcmd = append(pcmd, "phenotype", "--dsc_phenotypes", filepath.Join(tmpDir, "DSC_phenotypes_import.tsv"))
	if c.GlobalBool("use-logfile") {
		logf, err := getLogFileName(c, "phenotype")
		if err != nil {
			log.WithFields(logrus.Fields{
				"type": "logfile",
				"name": "name-generation",
			}).Error(err)
			return cli.NewExitError(err.Error(), 2)
		}
		log.Debugf("logfile %s for data phenotype", logf)
		pcmd = append(pcmd, "--logfile", logf, "--log_level", "info")
	}
	out, err := exec.Command(mainCmd, pcmd...).CombinedOutput()
	if err != nil {
		log.WithFields(logrus.Fields{
			"type":        "modware-import",
			"name":        "dictystrain2chado",
			"data":        "phenotype",
			"status":      string(out),
			"commandline": strings.Join(pcmd, " "),
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	log.Infof("successfully ran command %s", strings.Join(pcmd, " "))
	return nil
}

func runStrainPlasmidImport(c *cli.Context, tmpDir string, mainCmd string, log *logrus.Logger) error {
	cmd := makeStrainImportCmd(c, tmpDir)
	spcmd := make([]string, len(cmd))
	copy(spcmd, cmd)
	spcmd = append(spcmd, "plasmid")
	out, err := exec.Command(mainCmd, spcmd...).CombinedOutput()
	if c.GlobalBool("use-logfile") {
		logf, err := getLogFileName(c, "strain-plasmid")
		if err != nil {
			log.WithFields(logrus.Fields{
				"type": "logfile",
				"name": "name-generation",
			}).Error(err)
			return cli.NewExitError(err.Error(), 2)
		}
		log.Debugf("logfile %s for data %s", logf, "plasmid")
		spcmd = append(spcmd, "--logfile", logf, "--log_level", "info")
	}
	if err != nil {
		log.WithFields(logrus.Fields{
			"type":        "modware-import",
			"name":        "dictystrain2chado",
			"data":        "plasmid",
			"status":      string(out),
			"commandline": strings.Join(spcmd, " "),
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	log.Infof("successfully ran command %s", strings.Join(spcmd, " "))
	return nil
}

func runPlasmidImport(c *cli.Context, tmpDir string, mainCmd string, log *logrus.Logger) error {
	cmd := makePlasmidImportCmd(c, tmpDir)
	for i, data := range plasmidData {
		rcmd := make([]string, len(cmd))
		copy(rcmd, cmd)
		rcmd = append(rcmd, data)
		if i == 0 && c.Bool("prune") {
			rcmd = append(rcmd, "--prune")
		}
		if c.GlobalBool("use-logfile") {
			logf, err := getLogFileName(c, data)
			if err != nil {
				log.WithFields(logrus.Fields{
					"type": "logfile",
					"name": "name-generation",
				}).Error(err)
				return cli.NewExitError(err.Error(), 2)
			}
			log.Debugf("logfile %s for data %s", logf, data)
			rcmd = append(rcmd, "--logfile", logf, "--log_level", "info")
		}
		out, err := exec.Command(mainCmd, rcmd...).CombinedOutput()
		if err != nil {
			log.WithFields(logrus.Fields{
				"type":        "modware-import",
				"name":        "dictyplasmid2chado",
				"status":      string(out),
				"data":        data,
				"commandline": strings.Join(rcmd, " "),
			}).Error(err)
			return cli.NewExitError(err.Error(), 2)
		}
		log.Infof("successfully ran command %s", strings.Join(rcmd, " "))
	}
	scmd := make([]string, len(cmd))
	copy(scmd, cmd)
	scmd = append(scmd, "sequence", "--seq_data_dir", filepath.Join(tmpDir, "formatted_sequence"))
	if c.GlobalBool("use-logfile") {
		logf, err := getLogFileName(c, "sequence")
		if err != nil {
			log.WithFields(logrus.Fields{
				"type": "logfile",
				"name": "name-generation",
			}).Error(err)
			return cli.NewExitError(err.Error(), 2)
		}
		log.Debugf("logfile %s for data %s", logf, "sequence")
		scmd = append(scmd, "--logfile", logf, "--log_level", "info")
	}
	out, err := exec.Command(mainCmd, scmd...).CombinedOutput()
	if err != nil {
		log.WithFields(logrus.Fields{
			"type":        "modware-import",
			"name":        "plasmid2chado",
			"data":        "sequence",
			"status":      string(out),
			"commandline": strings.Join(scmd, " "),
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	log.Infof("successfully ran command %s", strings.Join(scmd, " "))
	return nil
}

func makePlasmidImportCmd(c *cli.Context, folder string) []string {
	return []string{
		"dictyplasmid2chado",
		"--dsn",
		getPostgresDsn(c),
		"--user",
		c.GlobalString("chado-user"),
		"--password",
		c.GlobalString("chado-pass"),
		"--dir",
		folder,
		"--data",
	}
}

func makeStrainImportCmd(c *cli.Context, folder string) []string {
	return []string{
		"dictystrain2chado",
		"--dsn",
		getPostgresDsn(c),
		"--user",
		c.GlobalString("chado-user"),
		"--password",
		c.GlobalString("chado-pass"),
		"--dir",
		folder,
		"--data",
	}
}

func ScOrderAction(c *cli.Context) error {
	log := getLogger(c)
	tmpDir, err := fetchAndDecompress(c, log, "dscorder")
	if err != nil {
		return err
	}
	if err := loadOrders(c, tmpDir, log); err != nil {
		return err
	}
	return nil
}

func loadOrders(c *cli.Context, tmpDir string, log *logrus.Logger) error {
	strainOrderFile := filepath.Join(tmpDir, "stock_orders.csv")
	handler, err := os.Open(strainOrderFile)
	if err != nil {
		log.Errorf("unable to open file %s", err)
		return cli.NewExitError(
			fmt.Sprintf("Unable to open file %s %s", strainOrderFile, err),
			2,
		)
	}
	defer handler.Close()
	r := csv.NewReader(handler)
	r.FieldsPerRecord = -1

	dat.EnableInterpolation = true
	// database connection
	dbh, err := getPgWrapper(c)
	if err != nil {
		log.Errorf("unable to create database connection %s", err)
		return cli.NewExitError(
			fmt.Sprint("Unable to create database connection %s", err),
			2,
		)
	}
	tx, err := dbh.Begin()
	if err != nil {
		log.Errorf("error in starting transaction %s", err)
		return cli.NewExitError(
			fmt.Sprintf("error in starting transaction %s", err),
			2,
		)
	}
	defer tx.AutoRollback()
	// delete all orders
	resD, err := tx.DeleteFrom("stock_order").Exec()
	if err != nil {
		log.WithFields(logrus.Fields{
			"type":  "delete",
			"table": "stock_order",
		}).Error(err)
		return cli.NewExitError(
			fmt.Sprintf("error in deleting all user information %s", err),
			2,
		)
	}
	log.Infof("deleted %d records", resD.RowsAffected)
	sItemOrderIbuilder := tx.InsertInto("stock_item_order").Columns("item_id", "order_id")
	orderCounter := 0
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("Unable to read from csv file %s", err)
			return cli.NewExitError(
				fmt.Sprintf("Unable to read from csv file %s", err),
				2,
			)
		}
		var userId int64
		err = tx.Select("auth_user_id").From("auth_user").
			Where("email = $1", record[1]).
			QueryScalar(&userId)
		if err != nil {
			if err == dat.ErrNotFound {
				log.Warnf("email %s not found", record[1])
				continue
			}
			log.WithFields(logrus.Fields{
				"type":  "select",
				"value": "email",
			}).Error(err)
			return cli.NewExitError(
				fmt.Sprintf("error in querying with email %s %s", record[1], err),
				2,
			)
		}
		t, err := time.Parse(orderDateLayout, record[0])
		if err != nil {
			log.WithFields(logrus.Fields{
				"type": "date parsing",
			}).Error(err)
			return cli.NewExitError(
				fmt.Sprintf("error in parsing date %s %s", record[0], err),
				2,
			)
		}
		stockOrder := &StockOrder{
			UserID:    userId,
			CreatedAt: dat.NullTimeFrom(t),
		}
		err = tx.InsertInto("stock_order").
			Columns("user_id", "created_at").
			Record(stockOrder).
			Returning("stock_order_id").
			QueryStruct(stockOrder)
		if err != nil {
			log.WithFields(logrus.Fields{
				"type":   "insert",
				"record": strings.Join(record, ":"),
			}).Error(err)
			return cli.NewExitError(
				fmt.Sprintf("error in inserting stock order %s ", err),
				2,
			)
		}
		orderCounter += 1
		for _, item := range record[2:] {
			var stockId int64
			if strings.HasPrefix(item, "DBS") { // strain
				err = tx.Select("stock_id").From("stock").
					Where("uniquename = $1", item).
					QueryScalar(&stockId)
				if err != nil {
					if err == dat.ErrNotFound {
						log.Warnf("strain  %s not found", item)
						continue
					}
					log.WithFields(logrus.Fields{
						"type":  "select",
						"item":  "strain id",
						"value": item,
					}).Error(err)
					return cli.NewExitError(
						fmt.Sprintf("error in querying with strain id  %s %s", item, err),
						2,
					)
				}
				sItemOrderIbuilder.Record(&StockItemOrder{
					ItemID:  stockId,
					OrderID: stockOrder.ID,
				})
			} else { // plasmid
				err = tx.Select("stock_id").From("stock").
					Where("name = $1", item).
					QueryScalar(&stockId)
				if err != nil {
					if err == dat.ErrNotFound {
						log.Warnf("plasmid %s not found", item)
						continue
					}
					log.WithFields(logrus.Fields{
						"type":  "select",
						"value": item,
						"item":  "plasmid id",
					}).Error(err)
					return cli.NewExitError(
						fmt.Sprintf("error in querying with plasmid id  %s %s", item, err),
						2,
					)
				}
				sItemOrderIbuilder.Record(&StockItemOrder{
					ItemID:  stockId,
					OrderID: stockOrder.ID,
				})
			}
		}
	}
	sIRes, err := sItemOrderIbuilder.Exec()
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "bulk insert",
		}).Error(err)
		return cli.NewExitError(
			fmt.Sprintf("error in bulk insert in stock_item_order %s", err),
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
	log.WithFields(logrus.Fields{
		"type":  "stock order",
		"count": orderCounter,
	}).Info("inserted stock order")
	log.WithFields(logrus.Fields{
		"type":  "stock item",
		"count": sIRes.RowsAffected,
	}).Info("inserted stock items")
	return nil
}
