package actions

import (
	"github.com/elastic/beats/libbeat/processors"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/common"
	"fmt"
	"github.com/pkg/errors"
	"strings"
	"time"
	"strconv"
)

type date struct {
	Fields      []string
	InLocation  string
	OutLocation string
	Target      string
}

func init() {
	processors.RegisterPlugin("date",
		configChecked(newDate,
			requireFields("fields"),
			allowedFields("fields", "in_location", "out_location", "target", "when")))
}

func newDate(c common.Config) (processors.Processor, error) {
	config := struct {
		Fields      []string    `config:"fields"`
		InLocation  string    `config:"in_location"`
		OutLocation string    `config:"out_location"`
		Target      string     `config:"target"`
	}{}

	err := c.Unpack(&config)
	if err != nil {
		logp.Warn("Error unpacking config for date")
		return nil, fmt.Errorf("fail to unpack the date configuration: %s", err)
	}

	f := date{
		Fields:      config.Fields,
		InLocation:  config.InLocation,
		OutLocation: config.OutLocation,
		Target:      config.Target,
	}

	return f, nil
}

func (f date) Run(event common.MapStr) (common.MapStr, error) {
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

		output, err := format(text, f.InLocation, f.OutLocation)

		if err != nil {
			debug("Error trying to format date %v for field : %s", text, field)
			errs = append(errs, err.Error())
			continue
		}

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

func format(t interface{}, inLoc string, outLoc string) (tm string, err error) {

	if inLoc == "" {
		inLoc = "UTC"
	}

	inloc, err := time.LoadLocation(inLoc)

	var outloc *time.Location
	if outLoc == "" || outLoc == inLoc {
		outloc = inloc
	} else {
		outloc, err = time.LoadLocation(outLoc)
	}

	if err != nil {
		return "", err
	}

	switch t.(type) {
	case string:
		ts := t.(string)
		tm, err := time.ParseInLocation(time.RFC3339, ts, inloc)
		if err != nil {
			tm, err = time.ParseInLocation("2006-01-02 15:04:05", ts, inloc)
		}

		if err != nil {
			var ut, err = strconv.ParseInt(ts, 10, 64)
			if err != nil {
				return "", err
			}

			tm, err = time.ParseInLocation(time.RFC3339, time.Unix(ut, 0).
				Format(time.RFC3339), inloc)
		}

		return tm.In(outloc).Format(time.RFC3339), err
	case int:
		tm, err := time.Parse(time.RFC3339, time.Unix(int64(t.(int)), 0).
			Format(time.RFC3339))
		return tm.In(outloc).Format(time.RFC3339), err
	case int32:
		tm, err := time.Parse(time.RFC3339, time.Unix(int64(t.(int32)), 0).
			Format(time.RFC3339))
		return tm.In(outloc).Format(time.RFC3339), err
	case int64:
		tm, err := time.Parse(time.RFC3339, time.Unix(t.(int64), 0).
			Format(time.RFC3339))

		return tm.In(outloc).Format(time.RFC3339), err
	}

	return "", nil
}

func (f date) String() string {
	return "date=" + strings.Join(f.Fields, ", ")
}
