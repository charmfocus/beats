package actions

import (
	"github.com/elastic/beats/libbeat/processors"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/common"
	"fmt"
	"github.com/pkg/errors"
	"strings"
	"xojoc.pw/useragent"
	"github.com/elastic/beats/libbeat/beat"
)

type ua struct {
	Fields []string
	Target string
}

func init() {
	processors.RegisterPlugin("ua",
		configChecked(newUA,
			requireFields("fields"),
			allowedFields("fields", "target", "when")))
}

func newUA(c *common.Config) (processors.Processor, error) {
	config := struct {
		Fields []string    `config:"fields"`
		Target string     `config:"target"`
	}{}

	err := c.Unpack(&config)
	if err != nil {
		logp.Warn("Error unpacking config for UA")
		return nil, fmt.Errorf("fail to unpack the UA configuration: %s", err)
	}

	f := &ua{
		Fields: config.Fields,
		Target: config.Target,
	}

	return f, nil
}

func (f *ua) Run(event *beat.Event) (*beat.Event, error) {
	var errs []string

	for _, field := range f.Fields {
		data, err := event.GetValue(field)
		if err != nil && errors.Cause(err) != common.ErrKeyNotFound {
			debug("Error trying to GetValue for field : %s in event : %v", field, event)
			errs = append(errs, err.Error())
			continue
		}

		text, ok := data.(string)

		if !ok {
			continue
		}

		_ua := useragent.Parse(text)

		var output map[string]interface{}

		if _ua != nil {
			output = map[string]interface{}{
				"version": _ua.Version,
				"mobile": _ua.Mobile,
				"name": _ua.Name,
				"os":_ua.OS,
				"os_version":_ua.OSVersion,
				"tablet":_ua.Tablet,
			}
		}

		target := field

		if f.Target != "" {
			target = f.Target
		}

		_, err = event.PutValue(target, output)

		if err != nil {
			debug("Error trying to Put value %v for field : %s", output, field)
			errs = append(errs, err.Error())
			continue
		}

	}

	if len(errs) > 0 {
		return event, fmt.Errorf(strings.Join(errs, ", "))
	}

	return event, nil
}

func (f *ua) String() string {
	return "ua=" + strings.Join(f.Fields, ", ")
}
