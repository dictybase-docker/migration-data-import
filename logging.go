package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/johntdyer/slackrus"
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
	log := getLogger(c)
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

func getLogger(c *cli.Context) *logrus.Logger {
	log := logrus.New()
	log.Formatter = &logrus.JSONFormatter{
		TimestampFormat: "02/Jan/2006:15:04:05",
	}
	log.Out = os.Stderr
	l := c.String("log-level")
	switch l {
	case "debug":
		log.Level = logrus.DebugLevel
	case "warn":
		log.Level = logrus.WarnLevel
	case "fatal":
		log.Level = logrus.FatalLevel
	case "panic":
		log.Level = logrus.PanicLevel
	default:
		log.Level = logrus.InfoLevel
	}
	// Set up hook
	lh := make(logrus.LevelHooks)
	for _, h := range c.StringSlice("hooks") {
		switch h {
		case "slack":
			lh.Add(&slackrus.SlackrusHook{
				HookURL:        c.String("slack-url"),
				AcceptedLevels: slackrus.LevelThreshold(log.Level),
				IconEmoji:      ":skull:",
			})
		default:
			continue
		}
	}
	log.Hooks = lh
	return log
}
