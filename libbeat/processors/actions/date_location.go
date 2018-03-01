package actions

import (
	"github.com/elastic/beats/libbeat/processors"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/common"
	"fmt"
	"github.com/pkg/errors"
	"strings"
	"time"
	"github.com/elastic/beats/libbeat/beat"
)

type dateLocation struct {
	Fields   []string
	Location string
}

func init() {
	processors.RegisterPlugin("date_location",
		configChecked(newDateLocation,
			requireFields("fields"),
			allowedFields("fields", "location", "when")))
}

func newDateLocation(c *common.Config) (processors.Processor, error) {
	config := struct {
		Fields   []string  `config:"fields"`
		Location string    `config:"location"`
	}{}

	err := c.Unpack(&config)
	if err != nil {
		logp.Warn("Error unpacking config for date location")
		return nil, fmt.Errorf("fail to unpack the date location configuration: %s", err)
	}

	f := &dateLocation{
		Fields:   config.Fields,
		Location: config.Location,
	}

	return f, nil
}

func (f *dateLocation) Run(event *beat.Event) (*beat.Event, error) {
	var errs []string

	for _, field := range f.Fields {
		data, err := event.GetValue(field)
		if err != nil && errors.Cause(err) != common.ErrKeyNotFound {
			debug("Error trying to GetValue for field : %s in event : %v", field, event)
			errs = append(errs, err.Error())
			continue
		}

		//text, ok := data.(string)
		//
		//if !ok {
		//	continue
		//}

		local, err := time.LoadLocation(f.Location)

		if err != nil {
			debug("Error trying to load location %v for field : %s", f.Location, field)
			errs = append(errs, err.Error())
			continue
		}

		var localTime time.Time
		switch data.(type) {
		case time.Time:
			localTime = data.(time.Time).In(local)
		case common.Time:
			localTime = time.Time(data.(common.Time)).In(local)
		}

		if field == "@timestamp" {
			event.Timestamp = localTime
		} else {
			_, err = event.PutValue(field, common.Time(localTime))
		}

		if err != nil {
			debug("Error trying to Put value %v for field : %s", localTime.Format(time.RFC3339), field)
			errs = append(errs, err.Error())
			continue
		}

	}

	if len(errs) > 0 {
		return event, fmt.Errorf(strings.Join(errs, ", "))
	}

	return event, nil
}

func (f *dateLocation) String() string {
	return "date_location=" + strings.Join(f.Fields, ", ")
}
