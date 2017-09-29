package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/johntdyer/slackrus"
	"github.com/sirupsen/logrus"
	"gopkg.in/urfave/cli.v1"
)

const layout = "2006-01-02_150405"

func validateUploadLog(c *cli.Context) error {
	if err := validateS3Args(c); err != nil {
		return err
	}
	return nil
}

func UploadLogAction(c *cli.Context) error {
	log, err := getLogger(c)
	if err != nil {
		return err
	}
	zfile := zipFileName("import")
	if err := zipFiles(c.GlobalString("local-log-path"), zfile); err != nil {
		log.WithFields(logrus.Fields{
			"type":   "zipfile",
			"action": "upload-log",
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	if err := uploadLocalFile(c, filepath.Join(c.GlobalString("local-log-path"), zfile)); err != nil {
		log.WithFields(logrus.Fields{
			"type":   "upload",
			"action": "upload-log",
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	return nil
}

func getLogFileName(c *cli.Context, prefix string) (string, error) {
	logfolder := c.GlobalString("local-log-path")
	if _, err := os.Stat(logfolder); os.IsNotExist(err) {
		err = os.MkdirAll(logfolder, os.ModeDir)
		if err != nil {
			return "", err
		}
	}
	logf := fmt.Sprintf("%s_%s.log", prefix, time.Now().Format(layout))
	return filepath.Join(logfolder, logf), nil
}

func zipFileName(prefix string) string {
	return fmt.Sprintf("%s_%s.zip", prefix, time.Now().Format(layout))
}

func getLogger(c *cli.Context, prefix string) (*logrus.Logger, error) {
	log := logrus.New()
	switch c.GlobalString("log-format") {
	case "text":
		log.Formatter = &logrus.TextFormatter{
			TimestampFormat: "02/Jan/2006:15:04:05",
		}
	case "json":
		log.Formatter = &logrus.JSONFormatter{
			TimestampFormat: "02/Jan/2006:15:04:05",
		}
	}
	if c.IsSet("use-logfile") {
		if len(prefix) == 0 {
			prefix = "auto"
		}
		w, err := getLogFileName(c, prefix)
		if err != nil {
			return log, fmt.Errorf("unable to create log file %s", err)
		}
		log.Out = w
	} else {
		log.Out = os.Stderr
	}
	l := c.GlobalString("log-level")
	switch l {
	case "debug":
		log.Level = logrus.DebugLevel
	case "warn":
		log.Level = logrus.WarnLevel
	case "error":
		log.Level = logrus.ErrorLevel
	case "fatal":
		log.Level = logrus.FatalLevel
	case "panic":
		log.Level = logrus.PanicLevel
	}
	// Set up hook
	lh := make(logrus.LevelHooks)
	for _, h := range c.GlobalStringSlice("hooks") {
		switch h {
		case "slack":
			lh.Add(&slackrus.SlackrusHook{
				HookURL:        c.GlobalString("slack-url"),
				AcceptedLevels: slackrus.LevelThreshold(log.Level),
				IconEmoji:      ":skull:",
			})
		default:
			continue
		}
	}
	log.Hooks = lh
	return log, nil
}
