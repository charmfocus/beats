package actions

import (
	"github.com/elastic/beats/libbeat/processors"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/common"
	"fmt"
	"github.com/pkg/errors"
)

type copyField struct {
	From   string
	Target string
}

func init() {
	processors.RegisterPlugin("copy_field",
		configChecked(newCopyField,
			requireFields("from", "target"),
			allowedFields("from", "target", "when")))
}

func newCopyField(c common.Config) (processors.Processor, error) {
	config := struct {
		From   string    `config:"from"`
		Target string     `config:"target"`
	}{}

	err := c.Unpack(&config)
	if err != nil {
		logp.Warn("Error unpacking config for copy_field")
		return nil, fmt.Errorf("fail to unpack the lower_case configuration: %s", err)
	}

	f := &copyField{
		From:   config.From,
		Target: config.Target,
	}

	return f, nil
}

func (f copyField) Run(event common.MapStr) (common.MapStr, error) {
	field := f.From
	data, err := event.GetValue(field)
	if err != nil && errors.Cause(err) != common.ErrKeyNotFound {
		debug("Error trying to GetValue for field : %s in event : %v", field, event)
		return event, fmt.Errorf(err.Error())
	}


	text, ok := data.(string)

	if !ok {
		return event, nil
	}

	_, err = event.Put(f.Target, text)

	if err != nil {
		debug("Error trying to Put value %v for field : %s", text, field)
		return event, fmt.Errorf(err.Error())
	}

	return event, nil
}

func (f copyField) String() string {
	return "copy_fields=" + f.From
}
