package actions

import (
	"github.com/elastic/beats/libbeat/processors"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/common"
	"fmt"
	"github.com/pkg/errors"
	"strings"
	"github.com/wangtuanjie/ip17mon"
)

type ipip struct {
	Fields []string
	DbPath string
	Target string
}

func init() {
	processors.RegisterPlugin("ipip",
		configChecked(newIpip,
			requireFields("fields", "db_path"),
			allowedFields("fields", "db_path", "target", "when")))
}

func newIpip(c common.Config) (processors.Processor, error) {
	config := struct {
		Fields []string    `config:"fields"`
		DbPath string      `config:"db_path"`
		Target string     `config:"target"`
	}{}

	err := c.Unpack(&config)
	if err != nil {
		logp.Warn("Error unpacking config for ipip")
		return nil, fmt.Errorf("fail to unpack the ipip configuration: %s", err)
	}

	f := &ipip{
		Fields: config.Fields,
		DbPath: config.DbPath,
		Target: config.Target,
	}

	if err := ip17mon.Init(config.DbPath); err != nil {
		logp.Warn("Error ip17mon init failed: %s", err)
		return nil, fmt.Errorf("ip17mon init failed: %s", err)
	}

	return f, nil
}

func (f ipip) Run(event common.MapStr) (common.MapStr, error) {
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

		loc, err := ip17mon.Find(text)
		if err != nil {
			debug("Error ip [%s] convert to loc failed: %s", text, err)
			errs = append(errs, err.Error())
			continue
		}

		output := map[string]string{"country": loc.Country, "province": loc.Region, "city": loc.City, "isp": loc.Isp}

		_, err = event.Put(f.Target, output)

		if err != nil {
			debug("Error trying to Put value %v for field : %s", loc, field)
			errs = append(errs, err.Error())
			continue
		}

	}

	if len(errs) > 0 {
		return event, fmt.Errorf(strings.Join(errs, ", "))
	}

	return event, nil
}

func (f ipip) String() string {
	return "ipip=" + strings.Join(f.Fields, ", ")
}
