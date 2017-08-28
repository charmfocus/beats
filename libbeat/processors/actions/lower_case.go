package actions

import (
	"github.com/elastic/beats/libbeat/processors"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/common"
	"fmt"
	"github.com/pkg/errors"
	"strings"
)

type lowerCase struct {
	Fields []string
	Target string
}

func init() {
	processors.RegisterPlugin("lower_case",
		configChecked(newLowerCase,
			requireFields("fields"),
			allowedFields("fields", "target", "when")))
}

func newLowerCase(c common.Config) (processors.Processor, error) {
	config := struct {
		Fields      []string    `config:"fields"`
		Target      string     `config:"target"`
	}{}

	err := c.Unpack(&config)
	if err != nil {
		logp.Warn("Error unpacking config for lower_case")
		return nil, fmt.Errorf("fail to unpack the lower_case configuration: %s", err)
	}

	f := &lowerCase{
		Fields:      config.Fields,
		Target:      config.Target,
	}

	return f, nil
}

func (f *lowerCase) Run(event common.MapStr) (common.MapStr, error) {
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

		output := strings.ToLower(text)

		target := field
		if f.Target != "" {
			target = f.Target
		}

		_, err = event.Put(target, output)

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

func (f lowerCase) String() string {
	return "lower_case=" + strings.Join(f.Fields, ", ")
}
