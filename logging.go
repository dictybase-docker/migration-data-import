package main

import (
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/johntdyer/slackrus"
	"gopkg.in/urfave/cli.v1"
)

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
